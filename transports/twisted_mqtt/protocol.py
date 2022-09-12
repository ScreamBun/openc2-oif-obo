import time
import struct
import uuid

from collections import OrderedDict, deque
from dataclasses import fields
from typing import Any, Callable, Deque, List, Literal, NoReturn, Optional, Tuple, Union
from twisted.internet.endpoints import TCP4ClientEndpoint, TCP6ClientEndpoint
from twisted.internet.protocol import Protocol, connectionDone
from twisted.internet.task import LoopingCall
from twisted.logger import Logger
from twisted.python.failure import Failure
from .consts import MQTT_CLEAN_START_FIRST_ONLY, MQTT_BRIDGE
from .matcher import MQTTMatcher
from .message import MQTTMessage, MQTTMessageInfo
from .properties import Properties
from .reasoncodes import ReasonCodes
from .subscribeoptions import SubscribeOptions
from .utils import (
    ConnAckCodes, ConnectionStates, ErrorValues, MessageStates, Versions, MessageTypes,
    Callbacks, InPacket, OutPacket,
    filter_wildcard_len_check, topic_wildcard_len_check
)

try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time


# Vars
SUBSCRIPTIONS = List[Tuple[str, Literal[0, 1, 2]]]
OnMessageCallback = Callable[["MQTTProtocol", Any, "MQTTMessage"], NoReturn]
log = Logger(namespace="mqtt")


class MQTTProtocol(Protocol):
    factory: "MQTTFactory"
    addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]
    broker: str
    _protocol: Versions
    _clean_session: bool
    _userdata: Any
    _client_id: bytes
    _client_mode: Literal[0, 1]
    # MQTT
    _initialTimeout: int
    _window: int
    _subs: SUBSCRIPTIONS = []
    # Connection
    _state: ConnectionStates
    _connect_properties: Properties = None
    _keepalive: int = 60
    _pingLoop: LoopingCall
    # Auth
    _username: bytes = None
    _password: bytes = None
    # Packet processing
    _on_message_filtered = MQTTMatcher()
    _in_packet: InPacket = InPacket()
    _out_packet: Deque[OutPacket] = deque()
    _in_messages: OrderedDict = OrderedDict()
    _out_messages: OrderedDict = OrderedDict()
    _last_msg_in: float
    _last_msg_out: float
    _max_inflight_messages: int = 20
    _inflight_messages: int = 0
    _max_queued_messages: int = 0
    # _queued_messages: int = 0
    # Will
    _will_properties: Properties = None
    _will: bool = False
    _will_topic: bytes = b""
    _will_payload: bytes = b""
    _will_qos: Literal[0, 1, 2] = 0
    _will_retain: bool = False
    # Callbacks
    _callbacks: Callbacks = Callbacks()
    suppress_exceptions: bool = False  # For callbacks
    # Assorted
    _last_mid: int = 0
    # for clean_start == MQTT_CLEAN_START_FIRST_ONLY
    _mqttv5_first_connect: bool = True
    # Consts
    MAX_WINDOW = 16  # Max value of in-flight PUBLISH/SUBSCRIBE/UNSUBSCRIBE
    TIMEOUT_INITIAL = 4  # Initial timeout for retransmissions
    TIMEOUT_MAX_INITIAL = 1024  # Maximum value for initial timeout

    def __init__(self, factory: "MQTTFactory", addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]) -> None:
        self.factory = factory
        self.addr = addr
        self.broker = f"mqtt://{addr.host}:{addr.port}"
        self._state = ConnectionStates.NEW
        self._protocol = self.factory._version
        self._userdata = self.factory._userdata
        self._client_id = self.factory._client_id
        self._subs = self.factory._subs
        self._callbacks = self.factory._callbacks
        self._clean_session = self.factory._clean_session
        self._client_mode = self.factory._client_mode
        self._clean_start = MQTT_CLEAN_START_FIRST_ONLY
        self._pingLoop = LoopingCall(self._send_pingreq)

    # Twisted Interface
    def connectionMade(self) -> NoReturn:
        try:
            yield self.connect()
        except Exception as err:
            log.error("Connecting to {broker} raised {excp!s}", broker=self.broker, excp=err)
            raise err
        else:
            log.info("Connected to {broker}", broker=self.broker)

    def connectionLost(self, reason: Failure = connectionDone) -> NoReturn:
        log.debug(f"--- Connection to MQTT Broker lost: {reason.getErrorMessage()}")
        if self._pingLoop.running:
            self._pingLoop.stop()
        self.connect()

    def dataReceived(self, data: bytes) -> ErrorValues:
        # log.debug("Received: bits={data_len} - `{data}`", data_len=len(data), data=data)
        if self._in_packet.command == 0:
            if len(data) == 0:
                return ErrorValues.CONN_LOST
            self._in_packet.command = struct.unpack(f"!B{len(data)-1}s", data)[0]
        self._in_packet.packet = data
        # All data for this packet is read.
        self._in_packet.pos = 0
        rc = self._packet_handle()
        # Free data and reset values
        self._in_packet = InPacket(
            command=MessageTypes.NEW,
            remaining_length=0,
            packet=bytearray(b""),
            pos=0
        )
        self._last_msg_in = time_func()
        if rc != ErrorValues.SUCCESS:
            log.debug("Process error: {rc} -> {msg}", rc=rc, msg=rc.name)
            return rc
        return ErrorValues.SUCCESS

    def doConnect(self, keepalive: int = 60) -> ErrorValues:
        proto_ver = self._protocol
        # hard-coded UTF-8 encoded string
        protocol = b"MQTT" if proto_ver >= Versions.v311 else b"MQIsdp"
        remaining_length = 2 + len(protocol) + 1 + 1 + 2 + 2 + len(self._client_id)
        connect_flags = 0
        if self._protocol == Versions.v5:
            if self._clean_start is True:
                connect_flags |= 0x02
            elif self._clean_start == MQTT_CLEAN_START_FIRST_ONLY and self._mqttv5_first_connect:
                connect_flags |= 0x02
        elif self._clean_session:
            connect_flags |= 0x02

        if self._will:
            remaining_length += 2 + len(self._will_topic) + 2 + len(self._will_payload)
            connect_flags |= 0x04 | ((self._will_qos & 0x03) << 3) | ((self._will_retain & 0x01) << 5)

        if self._username is not None:
            remaining_length += 2 + len(self._username)
            connect_flags |= 0x80
            if self._password is not None:
                connect_flags |= 0x40
                remaining_length += 2 + len(self._password)

        packed_connect_properties = b""
        packed_will_properties = b""
        if self._protocol == Versions.v5:
            if self._connect_properties is None:
                packed_connect_properties = b"\x00"
            else:
                packed_connect_properties = self._connect_properties.pack()
            remaining_length += len(packed_connect_properties)
            if self._will:
                if self._will_properties is None:
                    packed_will_properties = b"\x00"
                else:
                    packed_will_properties = self._will_properties.pack()
                remaining_length += len(packed_will_properties)

        command = MessageTypes.CONNECT
        packet = bytearray()
        packet.append(command)
        # as per the mosquitto broker, if the MSB of this version is set
        # to 1, then it treats the connection as a bridge
        if self._client_mode == MQTT_BRIDGE:
            proto_ver |= 0x80

        self._pack_remaining_length(packet, remaining_length)
        packet.extend(struct.pack("!H" + str(len(protocol)) + "sBBH", len(protocol), protocol, proto_ver, connect_flags, keepalive))
        if self._protocol == Versions.v5:
            packet += packed_connect_properties

        self._pack_str16(packet, self._client_id)
        if self._will:
            if self._protocol == Versions.v5:
                packet += packed_will_properties
            self._pack_str16(packet, self._will_topic)
            self._pack_str16(packet, self._will_payload)

        if self._username is not None:
            self._pack_str16(packet, self._username)
            if self._password is not None:
                self._pack_str16(packet, self._password)

        self._keepalive = keepalive
        log_args = {
            "u": (connect_flags & 0x80) >> 7,
            "p": (connect_flags & 0x40) >> 6,
            "wr": (connect_flags & 0x20) >> 5,
            "wq": (connect_flags & 0x18) >> 3,
            "wf": (connect_flags & 0x4) >> 2,
            "c": (connect_flags & 0x2) >> 1,
            "k": keepalive,
            "id": self._client_id
        }
        if self._protocol == Versions.v5:
            log.debug("Sending CONNECT (u{u}, p{p}, wr{wr}, wq{wq}, wf{wf}, c{c}, k{k}) client_id={id} properties={props}", **log_args, props=self._connect_properties)
        else:
            log.debug("Sending CONNECT (u{u}, p{p}, wr{wr}, wq{wq}, wf{wf}, c{c}, k{k}) client_id={id}", **log_args)
        return self._packet_queue(command, packet, 0, 0)

    # MQTT Interface
    def connect(self, clean_start=MQTT_CLEAN_START_FIRST_ONLY, properties=None) -> NoReturn:
        """API Entry Point"""
        log.debug(f"--- Connect to MQTT Broker {self.addr} with {self._protocol.name}")
        if self._protocol == Versions.v5:
            self._mqttv5_first_connect = True
        else:
            if clean_start != MQTT_CLEAN_START_FIRST_ONLY:
                raise ValueError("Clean start only applies to MQTT V5")
            if properties is not None:
                raise ValueError("Properties only apply to MQTT V5")
        self.doConnect()
        for sub in self._subs:
            self.subscribe(*sub)
        self._pingLoop.start(self._keepalive, now=False)

    def setTimeout(self, timeout: int) -> NoReturn:
        """API Entry Point"""
        if not 1 <= timeout <= self.TIMEOUT_MAX_INITIAL:
            raise ValueError(timeout)
        self._initialTimeout = timeout

    def setWindowSize(self, size: int) -> NoReturn:
        """API Entry Point"""
        if not 0 < size <= self.MAX_WINDOW:
            raise ValueError(f"Invalid window size of {size}")
        self._window = min(size, self.MAX_WINDOW)

    def publish(self, topic: Union[bytes, str], payload: Union[bytes, bytearray, int, float, str, None], qos: int = 0, retain: bool = False, properties: Properties = None) -> MQTTMessageInfo:
        if self._protocol != Versions.v5:
            if topic is None or len(topic) == 0:
                raise ValueError("Invalid topic.")

        topic = topic.encode("utf-8") if isinstance(topic, str) else topic
        if topic_wildcard_len_check(topic) != ErrorValues.SUCCESS:
            raise ValueError("Publish topic cannot contain wildcards.")

        if qos < 0 or qos > 2:
            raise ValueError("Invalid QoS level.")

        if isinstance(payload, str):
            local_payload = payload.encode("utf-8")
        elif isinstance(payload, (bytes, bytearray)):
            local_payload = payload
        elif isinstance(payload, (int, float)):
            local_payload = str(payload).encode("ascii")
        elif payload is None:
            local_payload = b""
        else:
            raise TypeError("payload must be a string, bytearray, int, float or None.")

        if len(local_payload) > 268435455:
            raise ValueError("Payload too large.")

        local_mid = self._mid_generate()
        if qos == 0:
            info = MQTTMessageInfo(local_mid)
            rc = self._send_publish(local_mid, topic, local_payload, qos, retain, False, info, properties)
            info.rc = rc
            return info
        message = MQTTMessage(local_mid, topic)
        message.timestamp = time_func()
        message.payload = payload
        message.qos = qos
        message.retain = retain
        message.dup = False
        message.properties = properties

        if 0 < self._max_queued_messages <= len(self._out_messages):
            message.info.rc = ErrorValues.QUEUE_SIZE
            return message.info

        if local_mid in self._out_messages:
            message.info.rc = ErrorValues.QUEUE_SIZE
            return message.info

        self._out_messages[message.mid] = message
        if self._max_inflight_messages == 0 or self._inflight_messages < self._max_inflight_messages:
            self._inflight_messages += 1
            if qos == 1:
                message.state = MessageStates.WAIT_FOR_PUBACK
            elif qos == 2:
                message.state = MessageStates.WAIT_FOR_PUBREC

            rc = self._send_publish(message.mid, topic, message.payload, message.qos, message.retain, message.dup, message.info, message.properties)
            # remove from inflight messages so it will be send after a connection is made
            if rc is ErrorValues.NO_CONN:
                self._inflight_messages -= 1
                message.state = MessageStates.PUBLISH

            message.info.rc = rc
            return message.info
        message.state = MessageStates.QUEUED
        message.info.rc = ErrorValues.SUCCESS
        return message.info

    def username_pw_set(self, username: Union[bytes, str], password: Optional[Union[bytes, str]] = None) -> NoReturn:
        # [MQTT-3.1.3-11] User name must be UTF-8 encoded string
        self._username = None if username is None else username.encode("utf-8")
        self._password = password
        if isinstance(self._password, str):
            self._password = self._password.encode("utf-8")

    def disconnect(self, rc: ReasonCodes = None, properties: Properties = None) -> ErrorValues:
        self._state = ConnectionStates.DISCONNECTING
        return self._send_disconnect(rc, properties)

    def subscribe(self, topic: Union[str, Tuple[str, SubscribeOptions]], qos: int = 0, options: Optional[SubscribeOptions] = None, properties: Properties = None) -> Tuple[ErrorValues, int]:
        topic_qos_list = None
        if isinstance(topic, tuple):
            if self._protocol == Versions.v5:
                topic, options = topic
                if not isinstance(options, SubscribeOptions):
                    raise ValueError("Subscribe options must be instance of SubscribeOptions class.")
            else:
                topic, qos = topic

        if isinstance(topic, str):
            if qos < 0 or qos > 2:
                raise ValueError("Invalid QoS level.")
            if self._protocol == Versions.v5:
                if options is None:
                    # if no options are provided, use the QoS passed instead
                    options = SubscribeOptions(qos=qos)
                elif qos != 0:
                    raise ValueError("Subscribe options and qos parameters cannot be combined.")
                if not isinstance(options, SubscribeOptions):
                    raise ValueError("Subscribe options must be instance of SubscribeOptions class.")
                topic_qos_list = [(topic.encode("utf-8"), options)]
            else:
                if topic is None or len(topic) == 0:
                    raise ValueError("Invalid topic.")
                topic_qos_list = [(topic.encode("utf-8"), qos)]
        elif isinstance(topic, list):
            topic_qos_list = []
            if self._protocol == Versions.v5:
                for t, o in topic:
                    if not isinstance(o, SubscribeOptions):
                        # then the second value should be QoS
                        if o < 0 or o > 2:
                            raise ValueError("Invalid QoS level.")
                        o = SubscribeOptions(qos=o)
                    topic_qos_list.append((t.encode("utf-8"), o))
            else:
                for t, q in topic:
                    if q < 0 or q > 2:
                        raise ValueError("Invalid QoS level.")
                    if t is None or len(t) == 0 or not isinstance(t, str):
                        raise ValueError("Invalid topic.")
                    topic_qos_list.append((t.encode("utf-8"), q))

        if topic_qos_list is None:
            raise ValueError("No topic specified, or incorrect topic type.")

        if any(filter_wildcard_len_check(topic) != ErrorValues.SUCCESS for topic, _ in topic_qos_list):
            raise ValueError("Invalid subscription filter.")

        return self._send_subscribe(False, topic_qos_list, properties)

    def unsubscribe(self, topic: Union[bytes, str, List[str]], properties: Properties = None) -> Tuple[ErrorValues, int]:
        topic_list = None
        if topic is None:
            raise ValueError("Invalid topic.")
        if isinstance(topic, str):
            if len(topic) == 0:
                raise ValueError("Invalid topic.")
            topic_list = [topic.encode("utf-8")]
        elif isinstance(topic, list):
            topic_list = []
            for t in topic:
                if len(t) == 0 or not isinstance(t, str):
                    raise ValueError("Invalid topic.")
                topic_list.append(t.encode("utf-8"))

        if topic_list is None:
            raise ValueError("No topic specified, or incorrect topic type.")

        return self._send_unsubscribe(False, topic_list, properties)

    # MQTT packet sending
    def _send_pingreq(self) -> ErrorValues:
        log.debug("Sending PINGREQ")
        rc = self._send_simple_command(MessageTypes.PINGREQ)
        return rc

    def _send_pingresp(self) -> ErrorValues:
        log.debug("Sending PINGRESP")
        return self._send_simple_command(MessageTypes.PINGRESP)

    def _send_puback(self, mid: int) -> ErrorValues:
        log.debug("Sending PUBACK (Mid: {mid})", mid=mid)
        return self._send_command_with_mid(MessageTypes.PUBACK, mid, False)

    def _send_pubcomp(self, mid: int) -> ErrorValues:
        log.debug("Sending PUBCOMP (Mid: {mid})", mid=mid)
        return self._send_command_with_mid(MessageTypes.PUBCOMP, mid, False)

    def _send_publish(self, mid: int, topic: Union[bytes, str], payload: Union[bytes, bytearray, int, float, str, None] = b"", qos: int = 0, retain: bool = False, dup: bool = False, info: MQTTMessageInfo = None, properties: Properties = None) -> ErrorValues:
        # we assume that topic and payload are already properly encoded
        assert not isinstance(topic, str) and not isinstance(payload, str) and payload is not None
        command = MessageTypes.PUBLISH | ((dup & 0x1) << 3) | (qos << 1) | retain
        packet = bytearray([command, ])
        payloadlen = len(payload)
        remaining_length = 2 + len(topic) + payloadlen
        log_args = {"dup": dup, "qos": qos, "retain": retain, "mid": mid, "topic": topic}
        if payloadlen == 0:
            if self._protocol == Versions.v5:
                log.debug("Sending PUBLISH (d{dup}, q{qos}, r{retain}, m{mid}), '{topic}', properties={props} (NULL payload)", **log_args, props=properties)
            else:
                log.debug("Sending PUBLISH (d{dup}, q{qos}, r{retain}, m{mid}), '{topic}' (NULL payload)", **log_args)
        else:
            if self._protocol == Versions.v5:
                log.debug("Sending PUBLISH (d{dup}, q{qos}, r{retain}, m{mid}), '{topic}', properties={props}, ... ({size} bytes)", **log_args, props=properties, size=payloadlen)
            else:
                log.debug("Sending PUBLISH (d{dup}, q{qos}, r{retain}, m{mid}), '{topic}', ... ({size} bytes)", **log_args, size=payloadlen)

        if qos > 0:
            # For message id
            remaining_length += 2

        packed_properties = b""
        if self._protocol == Versions.v5:
            packed_properties = b"\x00" if properties is None else properties.pack()
            remaining_length += len(packed_properties)

        self._pack_remaining_length(packet, remaining_length)
        self._pack_str16(packet, topic)
        if qos > 0:
            # For message id
            packet.extend(struct.pack("!H", mid))

        if self._protocol == Versions.v5:
            packet.extend(packed_properties)

        packet.extend(payload)
        return self._packet_queue(MessageTypes.PUBLISH, packet, mid, qos, info)

    def _send_pubrec(self, mid: int) -> ErrorValues:
        log.debug("Sending PUBREC (Mid: {mid})", mid=mid)
        return self._send_command_with_mid(MessageTypes.PUBREC, mid, False)

    def _send_pubrel(self, mid: int) -> ErrorValues:
        log.debug("Sending PUBREL (Mid: {mid}})", mid=mid)
        return self._send_command_with_mid(MessageTypes.PUBREL | 2, mid, False)

    def _send_command_with_mid(self, command: int, mid: int, dup: bool) -> ErrorValues:
        # For PUBACK, PUBCOMP, PUBREC, and PUBREL
        if dup:
            command |= 0x8

        remaining_length = 2
        packet = struct.pack("!BBH", command, remaining_length, mid)
        return self._packet_queue(command, packet, mid, 1)

    def _send_simple_command(self, command: int) -> ErrorValues:
        # For DISCONNECT, PINGREQ and PINGRESP
        remaining_length = 0
        packet = struct.pack("!BB", command, remaining_length)
        return self._packet_queue(command, packet, 0, 0)

    def _send_connect(self, keepalive: int) -> ErrorValues:
        proto_ver = self._protocol
        # hard-coded UTF-8 encoded string
        protocol = b"MQTT" if proto_ver >= Versions.v311 else b"MQIsdp"
        remaining_length = 2 + len(protocol) + 1 + 1 + 2 + 2 + len(self._client_id)
        connect_flags = 0
        if self._protocol == Versions.v5:
            if self._clean_start is True:
                connect_flags |= 0x02
            elif self._clean_start == MQTT_CLEAN_START_FIRST_ONLY and self._mqttv5_first_connect:
                connect_flags |= 0x02
        elif self._clean_session:
            connect_flags |= 0x02

        if self._will:
            remaining_length += 2 + len(self._will_topic) + 2 + len(self._will_payload)
            connect_flags |= 0x04 | ((self._will_qos & 0x03) << 3) | ((self._will_retain & 0x01) << 5)

        if self._username is not None:
            remaining_length += 2 + len(self._username)
            connect_flags |= 0x80
            if self._password is not None:
                connect_flags |= 0x40
                remaining_length += 2 + len(self._password)

        packed_connect_properties = b""
        packed_will_properties = b""
        if self._protocol == Versions.v5:
            if self._connect_properties is None:
                packed_connect_properties = b"\x00"
            else:
                packed_connect_properties = self._connect_properties.pack()
            remaining_length += len(packed_connect_properties)
            if self._will:
                if self._will_properties is None:
                    packed_will_properties = b"\x00"
                else:
                    packed_will_properties = self._will_properties.pack()
                remaining_length += len(packed_will_properties)

        command = MessageTypes.CONNECT
        packet = bytearray()
        packet.append(command)
        # as per the mosquitto broker, if the MSB of this version is set
        # to 1, then it treats the connection as a bridge
        if self._client_mode == MQTT_BRIDGE:
            proto_ver |= 0x80

        self._pack_remaining_length(packet, remaining_length)
        packet.extend(struct.pack("!H" + str(len(protocol)) + "sBBH", len(protocol), protocol, proto_ver, connect_flags, keepalive))
        if self._protocol == Versions.v5:
            packet += packed_connect_properties

        self._pack_str16(packet, self._client_id)
        if self._will:
            if self._protocol == Versions.v5:
                packet += packed_will_properties
            self._pack_str16(packet, self._will_topic)
            self._pack_str16(packet, self._will_payload)

        if self._username is not None:
            self._pack_str16(packet, self._username)
            if self._password is not None:
                self._pack_str16(packet, self._password)

        self._keepalive = keepalive
        log_args = {
            "u": (connect_flags & 0x80) >> 7,
            "p": (connect_flags & 0x40) >> 6,
            "wr": (connect_flags & 0x20) >> 5,
            "wq": (connect_flags & 0x18) >> 3,
            "wf": (connect_flags & 0x4) >> 2,
            "c": (connect_flags & 0x2) >> 1,
            "k": keepalive,
            "id": self._client_id
        }
        if self._protocol == Versions.v5:
            log.debug("Sending CONNECT (u{u}, p{p}, wr{wr}, wq{wq}, wf{wf}, c{c}, k{k}) client_id={id} properties={props}", **log_args, props=self._connect_properties)
        else:
            log.debug("Sending CONNECT (u{u}, p{p}, wr{wr}, wq{wq}, wf{wf}, c{c}, k{k}) client_id={id}", **log_args)
        return self._packet_queue(command, packet, 0, 0)

    def _send_disconnect(self, rc: ReasonCodes = None, properties: Properties = None) -> ErrorValues:
        if self._protocol == Versions.v5:
            log.debug("Sending DISCONNECT reasonCode={code} properties={props}", code=rc, props=properties)
        else:
            log.debug("Sending DISCONNECT")

        remaining_length = 0
        command = MessageTypes.DISCONNECT
        packet = bytearray([command, ])
        packed_props = b""
        if self._protocol == Versions.v5:
            if properties is not None or rc is not None:
                if rc is None:
                    rc = ReasonCodes(MessageTypes.DISCONNECT >> 4, identifier=0)
                remaining_length += 1
                if properties is not None:
                    packed_props = properties.pack()
                    remaining_length += len(packed_props)

        self._pack_remaining_length(packet, remaining_length)
        if self._protocol == Versions.v5:
            if rc is not None:
                packet += rc.pack()
                if properties is not None:
                    packet += packed_props

        return self._packet_queue(command, packet, 0, 0)

    def _send_subscribe(self, dup: int, topics: List[Tuple[bytes, Union[int, SubscribeOptions]]], properties: Properties = None) -> Tuple[ErrorValues, int]:
        remaining_length = 2
        packed_subscribe_properties = b""
        if self._protocol == Versions.v5:
            if properties is None:
                packed_subscribe_properties = b"\x00"
            else:
                packed_subscribe_properties = properties.pack()
            remaining_length += len(packed_subscribe_properties)
        for t, _ in topics:
            remaining_length += 2 + len(t) + 1

        command = MessageTypes.SUBSCRIBE | (dup << 3) | 0x2
        packet = bytearray()
        packet.append(command)
        self._pack_remaining_length(packet, remaining_length)
        local_mid = self._mid_generate()
        packet.extend(struct.pack("!H", local_mid))

        if self._protocol == Versions.v5:
            packet += packed_subscribe_properties

        for t, q in topics:
            self._pack_str16(packet, t)
            if self._protocol == Versions.v5:
                packet += q.pack()
            else:
                packet.append(q)

        log.debug("Sending SUBSCRIBE (d{dup}, m{mid}) {topics}", dup=dup, mid=local_mid, topics=topics)
        return self._packet_queue(command, packet, local_mid, 1), local_mid

    def _send_unsubscribe(self, dup: int, topics: List[Union[bytes, str]], properties: Properties = None) -> Tuple[ErrorValues, int]:
        remaining_length = 2
        packed_unsubscribe_properties = b""
        if self._protocol == Versions.v5:
            if properties is None:
                packed_unsubscribe_properties = b"\x00"
            else:
                packed_unsubscribe_properties = properties.pack()
            remaining_length += len(packed_unsubscribe_properties)
        for t in topics:
            remaining_length += 2 + len(t)

        command = MessageTypes.UNSUBSCRIBE | (dup << 3) | 0x2
        packet = bytearray()
        packet.append(command)
        self._pack_remaining_length(packet, remaining_length)
        local_mid = self._mid_generate()
        packet.extend(struct.pack("!H", local_mid))

        if self._protocol == Versions.v5:
            packet += packed_unsubscribe_properties

        for t in topics:
            self._pack_str16(packet, t)

        # topics_repr = ", ".join("'"+topic.decode("utf8")+"'" for topic in topics)
        if self._protocol == Versions.v5:
            log.debug("Sending UNSUBSCRIBE (d{dup}, m{mid}) {props} {topics}", dup=dup, mid=local_mid, props=properties, topics=topics)
        else:
            log.debug("Sending UNSUBSCRIBE (d{dup}, m{mid}) {topics}", dup=dup, mid=local_mid, topics=topics)
        return self._packet_queue(command, packet, local_mid, 1), local_mid

    # MQTT packet handling
    def _packet_handle(self) -> ErrorValues:
        cmd = self._in_packet.command & 0xF0
        if cmd == MessageTypes.PINGREQ:
            return self._handle_pingreq()
        if cmd == MessageTypes.PINGRESP:
            return self._handle_pingresp()
        if cmd == MessageTypes.PUBACK:
            return self._handle_pubackcomp("PUBACK")
        if cmd == MessageTypes.PUBCOMP:
            return self._handle_pubackcomp("PUBCOMP")
        if cmd == MessageTypes.PUBLISH:
            return self._handle_publish()
        if cmd == MessageTypes.PUBREC:
            return self._handle_pubrec()
        if cmd == MessageTypes.PUBREL:
            return self._handle_pubrel()
        if cmd == MessageTypes.CONNACK:
            return self._handle_connack()
        if cmd == MessageTypes.SUBACK:
            return self._handle_suback()
        if cmd == MessageTypes.UNSUBACK:
            return self._handle_unsuback()
        if cmd == MessageTypes.DISCONNECT and self._protocol == Versions.v5:  # only allowed in MQTT 5.0
            return self._handle_disconnect()
        # If we don't recognise the command, return an error straight away.
        log.error("Error: Unrecognised command {cmd}", cmd=cmd)
        return ErrorValues.PROTOCOL

    def _handle_connack(self) -> ErrorValues:
        if self._protocol == Versions.v5:
            flags, result = struct.unpack("!BB", self._in_packet.packet[1:3])
            if result == 1:
                # This is probably a failure from a broker that doesn't support
                # MQTT v5.
                reason = 132  # Unsupported protocol version
                properties = None
            else:
                reason = ReasonCodes(MessageTypes.CONNACK >> 4, identifier=result)
                properties = Properties(MessageTypes.CONNACK >> 4)
                properties.unpack(self._in_packet.packet[2:])
        else:
            flags, result = struct.unpack("!BB", self._in_packet.packet)

        if self._protocol == Versions.v311:
            if result == ConnAckCodes.REFUSED_PROTOCOL_VERSION:
                log.debug("Received CONNACK ({flags}, {rslt}), attempting downgrade to MQTT v3.1.", flags=flags, rslt=result)
                # Downgrade to MQTT v3.1
                self._protocol = Versions.v31
                return self.disconnect(ReasonCodes(132))
            if result == ConnAckCodes.REFUSED_IDENTIFIER_REJECTED and self._client_id == b"":
                log.debug("Received CONNACK ({flags}, {rslt}), attempting to use non-empty CID", flags=flags, rslt=result)
                self._client_id = self.make_id()
                return self.disconnect(ReasonCodes(133))

        if result == 0:
            self._state = ConnectionStates.CONNECTED

        if self._protocol == Versions.v5:
            log.debug("Received CONNACK ({flags}, {reason}) properties={props}", flags=flags, reason=reason, props=properties)
        else:
            log.debug("Received CONNACK ({flags}, {reason})", flags=flags, reason=result)

        # it won't be the first successful connect any more
        self._mqttv5_first_connect = False
        on_connect = self._callbacks.on_connect
        if on_connect:
            flags_dict = {"session present": flags & 0x01}
            try:
                if self._protocol == Versions.v5:
                    on_connect(self, self._userdata, flags_dict, reason, properties)
                else:
                    on_connect(self, self._userdata, flags_dict, result)
            except Exception as err:
                log.error("Caught exception in on_connect: {err}", err=err)
                if not self.suppress_exceptions:
                    raise

        if result == 0:
            rc = 0
            for m in self._out_messages.values():
                m.timestamp = time_func()
                if m.state == MessageStates.QUEUED:
                    self._packet_write()  # Process outgoing messages that have just been queued up
                    return ErrorValues.SUCCESS

                if m.qos == 0:
                    rc = self._send_publish(m.mid, m.topic.encode("utf-8"), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                    if rc != 0:
                        return rc
                elif m.qos == 1:
                    if m.state == MessageStates.PUBLISH:
                        self._inflight_messages += 1
                        m.state = MessageStates.WAIT_FOR_PUBACK
                        rc = self._send_publish(m.mid, m.topic.encode("utf-8"), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                        if rc != 0:
                            return rc
                elif m.qos == 2:
                    if m.state == MessageStates.PUBLISH:
                        self._inflight_messages += 1
                        m.state = MessageStates.WAIT_FOR_PUBREC
                        rc = self._send_publish(m.mid, m.topic.encode("utf-8"), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                        if rc != 0:
                            return rc
                    elif m.state == MessageStates.RESEND_PUBREL:
                        self._inflight_messages += 1
                        m.state = MessageStates.WAIT_FOR_PUBCOMP
                        rc = self._send_pubrel(m.mid)
                        if rc != 0:
                            return rc
                self._packet_write()  # Process outgoing messages that have just been queued up
            return rc
        if 0 < result < 6:
            return ErrorValues.CONN_REFUSED
        return ErrorValues.PROTOCOL

    def _handle_on_connect_fail(self) -> NoReturn:
        on_connect_fail = self._callbacks.on_connect_fail
        if on_connect_fail:
            try:
                on_connect_fail(self, self._userdata)
            except Exception as err:
                log.error("Caught exception in on_connect_fail: {err}", err=err)

    def _handle_disconnect(self) -> ErrorValues:
        packet_type = MessageTypes.DISCONNECT >> 4
        reasonCode = properties = None
        if self._in_packet.remaining_length > 2:
            reasonCode = ReasonCodes(packet_type)
            reasonCode.unpack(self._in_packet.packet)
            if self._in_packet.remaining_length > 3:
                properties = Properties(packet_type)
                properties.unpack(self._in_packet.packet[1:])
        log.debug("Received DISCONNECT {code} {props}", code=reasonCode, props=properties)
        self._loop_rc_handle(reasonCode, properties)
        return ErrorValues.SUCCESS

    def _handle_pingreq(self) -> ErrorValues:
        if self._in_packet.remaining_length != 0:
            return ErrorValues.PROTOCOL

        log.debug("Received PINGREQ")
        return self._send_pingresp()

    def _handle_pingresp(self) -> ErrorValues:
        if self._in_packet.remaining_length != 0:
            return ErrorValues.PROTOCOL
        log.debug("Received PINGRESP")
        return ErrorValues.SUCCESS

    def _handle_publish(self) -> ErrorValues:
        header = self._in_packet.command
        message = MQTTMessage()
        message.dup = (header & 0x08) >> 3
        message.qos = (header & 0x06) >> 1
        message.retain = (header & 0x01)

        packet = self._packet_data()
        pack_format = f"!H{len(packet) - 2}s"
        slen, packet = struct.unpack(pack_format, packet)
        pack_format = f"!{slen}s{len(packet) - slen}s"
        topic, packet = struct.unpack(pack_format, packet)

        if self._protocol != Versions.v5 and len(topic) == 0:
            return ErrorValues.PROTOCOL

        # Handle topics with invalid UTF-8
        # This replaces an invalid topic with a message and the hex
        # representation of the topic for logging. When the user attempts to
        # access message.topic in the callback, an exception will be raised.
        try:
            print_topic = topic.decode("utf-8")
        except UnicodeDecodeError:
            print_topic = "TOPIC WITH INVALID UTF-8: " + str(topic)

        message.topic = topic
        if message.qos > 0:
            pack_format = f"!H{len(packet) - 2}s"
            (message.mid, packet) = struct.unpack(pack_format, packet)

        if self._protocol == Versions.v5:
            message.properties = Properties(MessageTypes.PUBLISH >> 4)
            _, props_len = message.properties.unpack(packet)
            packet = packet[props_len:]

        message.payload = packet
        log_args = {"dup": message.dup, "qos": message.qos, "retain": message.retain, "mid": message.mid, "topics": print_topic}
        if self._protocol == Versions.v5:
            log.debug("Received PUBLISH (d{dup}, q{qos}, r{retain}, m{mid}), '{topics}', properties={props}, ...  ({size} bytes)", **log_args, props=message.properties, size=len(message.payload))
        else:
            log.debug("Received PUBLISH (d{dup}, q{qos}, r{retain}, m{mid}), '{topics}', ...  ({size} bytes)",  **log_args, size=len(message.payload))

        message.timestamp = time_func()
        if message.qos == 0:
            self._handle_on_message(message)
            return ErrorValues.SUCCESS
        if message.qos == 1:
            self._handle_on_message(message)
            return self._send_puback(message.mid)
        if message.qos == 2:
            rc = self._send_pubrec(message.mid)
            message.state = MessageStates.WAIT_FOR_PUBREL
            self._in_messages[message.mid] = message
            return rc
        return ErrorValues.PROTOCOL

    def _handle_pubrec(self) -> ErrorValues:
        if self._protocol == Versions.v5:
            if self._in_packet.remaining_length < 2:
                return ErrorValues.PROTOCOL
        elif self._in_packet.remaining_length != 2:
            return ErrorValues.PROTOCOL

        mid, = struct.unpack("!H", self._in_packet.packet[:2])
        if self._protocol == Versions.v5:
            if self._in_packet.remaining_length > 2:
                reasonCode = ReasonCodes(MessageTypes.PUBREC >> 4)
                reasonCode.unpack(self._in_packet.packet[2:])
                if self._in_packet.remaining_length > 3:
                    properties = Properties(MessageTypes.PUBREC >> 4)
                    properties.unpack(self._in_packet.packet[3:])
        log.debug("Received PUBREC (Mid: {mid})", mid=mid)

        if mid in self._out_messages:
            msg = self._out_messages[mid]
            msg.state = MessageStates.WAIT_FOR_PUBCOMP
            msg.timestamp = time_func()
            return self._send_pubrel(mid)
        return ErrorValues.SUCCESS

    def _handle_pubrel(self) -> ErrorValues:
        if self._protocol == Versions.v5:
            if self._in_packet.remaining_length < 2:
                return ErrorValues.PROTOCOL
        elif self._in_packet.remaining_length != 2:
            return ErrorValues.PROTOCOL

        mid, = struct.unpack("!H", self._in_packet.packet)
        log.debug("Received PUBREL (Mid: {mid})", mid=mid)

        if mid in self._in_messages:
            # Only pass the message on if we have removed it from the queue - this
            # prevents multiple callbacks for the same message.
            message = self._in_messages.pop(mid)
            self._handle_on_message(message)
            self._inflight_messages -= 1
            if self._max_inflight_messages > 0:
                rc = self._update_inflight()
                if rc != ErrorValues.SUCCESS:
                    return rc

        # FIXME: this should only be done if the message is known
        # If unknown it's a protocol error and we should close the connection.
        # But since we don't have (on disk) persistence for the session, it
        # is possible that we must known about this message.
        # Choose to acknwoledge this messsage (and thus losing a message) but
        # avoid hanging. See #284.
        return self._send_pubcomp(mid)

    def _handle_suback(self) -> ErrorValues:
        log.debug("Received SUBACK")
        pack_format = f"!H{len(self._in_packet.packet) - 2}s"
        mid, packet = struct.unpack(pack_format, self._in_packet.packet)
        if self._protocol == Versions.v5:
            properties = Properties(MessageTypes.SUBACK >> 4)
            _, props_len = properties.unpack(packet)
            reasoncodes = []
            for c in packet[props_len:]:
                reasoncodes.append(ReasonCodes(MessageTypes.SUBACK >> 4, identifier=c))
        else:
            pack_format = "!" + "B" * len(packet)
            granted_qos = struct.unpack(pack_format, packet)

        on_subscribe = self._callbacks.on_subscribe
        if on_subscribe:
            try:
                if self._protocol == Versions.v5:
                    on_subscribe(self, self._userdata, mid, reasoncodes, properties)
                else:
                    on_subscribe(self, self._userdata, mid, granted_qos)
            except Exception as err:
                log.error("Caught exception in on_subscribe: {err}", err=err)
                if not self.suppress_exceptions:
                    raise

        return ErrorValues.SUCCESS

    def _handle_unsuback(self) -> ErrorValues:
        if self._protocol == Versions.v5:
            if self._in_packet.remaining_length < 4:
                return ErrorValues.PROTOCOL
        elif self._in_packet.remaining_length != 2:
            return ErrorValues.PROTOCOL

        mid, = struct.unpack("!H", self._in_packet.packet[:2])
        if self._protocol == Versions.v5:
            packet = self._in_packet.packet[2:]
            properties = Properties(MessageTypes.UNSUBACK >> 4)
            _, props_len = properties.unpack(packet)
            reasoncodes = []
            for c in packet[props_len:]:
                reasoncodes.append(ReasonCodes(MessageTypes.UNSUBACK >> 4, identifier=c))
            if len(reasoncodes) == 1:
                reasoncodes = reasoncodes[0]

        log.debug("Received UNSUBACK (Mid: {mid})", mid=mid)
        on_unsubscribe = self._callbacks.on_unsubscribe
        if on_unsubscribe:
            try:
                if self._protocol == Versions.v5:
                    on_unsubscribe(self, self._userdata, mid, properties, reasoncodes)
                else:
                    on_unsubscribe(self, self._userdata, mid)
            except Exception as err:
                log.error("Caught exception in on_unsubscribe: {err}", err=err)
                if not self.suppress_exceptions:
                    raise

        return ErrorValues.SUCCESS

    def _handle_pubackcomp(self, cmd: str) -> ErrorValues:
        if self._protocol == Versions.v5:
            if self._in_packet.remaining_length < 2:
                return ErrorValues.PROTOCOL
        elif self._in_packet.remaining_length != 2:
            return ErrorValues.PROTOCOL

        packet_type = MessageTypes.PUBACK if cmd == "PUBACK" else MessageTypes.PUBCOMP
        packet_type = packet_type >> 4
        mid, = struct.unpack("!H", self._in_packet.packet[:2])
        if self._protocol == Versions.v5:
            if self._in_packet.remaining_length > 2:
                reasonCode = ReasonCodes(packet_type)
                reasonCode.unpack(self._in_packet.packet[2:])
                if self._in_packet.remaining_length > 3:
                    properties = Properties(packet_type)
                    properties.unpack(self._in_packet.packet[3:])

        log.debug("Received {cmd} (Mid: {mid}", cmd=cmd, mid=mid)
        if mid in self._out_messages:
            # Only inform the client the message has been sent once.
            return self._do_on_publish(mid)

        return ErrorValues.SUCCESS

    def _handle_on_message(self, message: MQTTMessage) -> NoReturn:
        try:
            topic = message.topic
        except UnicodeDecodeError:
            topic = None

        on_message_callbacks = []
        if topic is not None:
            for callback in self._on_message_filtered.iter_match(message.topic):
                on_message_callbacks.append(callback)
        on_message = self._callbacks.on_message if len(on_message_callbacks) == 0 else None

        for callback in on_message_callbacks:
            try:
                callback(self, self._userdata, message)
            except Exception as err:
                log.error("Caught exception in user defined callback function {callback}: {err}", callback=callback.__name__, err=err)
                if not self.suppress_exceptions:
                    raise

        if on_message:
            try:
                on_message(self, self._userdata, message)
            except Exception as err:
                log.error("Caught exception in on_message: {err}", err=err)
                if not self.suppress_exceptions:
                    raise

    # MQTT Helpers

    def _do_on_disconnect(self, rc: int, properties: Properties = None):
        on_disconnect = self._callbacks.on_disconnect

        if on_disconnect:
            try:
                if self._protocol == Versions.v5:
                    on_disconnect(self, self._userdata, rc, properties)
                else:
                    on_disconnect(self, self._userdata, rc)
            except Exception as err:
                log.error("Caught exception in on_disconnect: {err}", err=err)
                if not self.suppress_exceptions:
                    raise

    def _do_on_publish(self, mid):
        on_publish = self._callbacks.on_publish

        if on_publish:
            try:
                on_publish(self, self._userdata, mid)
            except Exception as err:
                log.error("Caught exception in on_publish: {err}", err=err)
                if not self.suppress_exceptions:
                    raise

        msg = self._out_messages.pop(mid)
        msg.info._set_as_published()
        if msg.qos > 0:
            self._inflight_messages -= 1
            if self._max_inflight_messages > 0:
                rc = self._update_inflight()
                if rc != ErrorValues.SUCCESS:
                    return rc
        return ErrorValues.SUCCESS

    def _loop_rc_handle(self, rc: Optional[ReasonCodes], properties: Properties = None) -> NoReturn:
        if rc:
            self._send_disconnect(rc, properties)
            if self._state == ConnectionStates.DISCONNECTING:
                rc = ErrorValues.SUCCESS
            self._do_on_disconnect(rc, properties)

    def _packet_data(self) -> bytearray:
        """
        removes the control and variable length header
        :return:
        """
        lenLen = 1
        while self._in_packet.packet[lenLen] & 0x80:
            lenLen += 1
        return self._in_packet.packet[lenLen + 1:]

    def _mid_generate(self) -> int:
        self._last_mid += 1
        if self._last_mid == 65536:
            self._last_mid = 1
        return self._last_mid

    def _packet_write(self) -> ErrorValues:
        try:
            packet = self._out_packet.popleft()
        except IndexError:
            return ErrorValues.SUCCESS

        try:
            data = bytes(packet.packet[packet.pos:])
            self.transport.write(data)
        except (AttributeError, ValueError) as err:
            self._out_packet.appendleft(packet)
            log.error("failed to write data: {err}", err=err)
            return ErrorValues.SUCCESS
        except BlockingIOError as err:
            self._out_packet.appendleft(packet)
            log.error("failed to write data: {err}", err=err)
            return ErrorValues.AGAIN
        except ConnectionError as err:
            self._out_packet.appendleft(packet)
            log.error("failed to receive on socket: {err}", err=err)
            return ErrorValues.CONN_LOST

        self._last_msg_out = time_func()
        return ErrorValues.SUCCESS

    def _packet_queue(self, command: int, packet: Union[bytes, bytearray], mid: int, qos: int, info: MQTTMessageInfo = None) -> ErrorValues:
        mpkt = OutPacket(
            command=command,
            mid=mid,
            qos=qos,
            pos=0,
            packet=packet,
            info=info
        )
        self._out_packet.append(mpkt)
        return self._packet_write()

    def _pack_remaining_length(self, packet: bytearray, remaining_length: int) -> bytearray:
        remaining_bytes = []
        while True:
            byte = remaining_length % 128
            remaining_length = remaining_length // 128
            # If there are more digits to encode, set the top bit of this digit
            if remaining_length > 0:
                byte |= 0x80

            remaining_bytes.append(byte)
            packet.append(byte)
            if remaining_length == 0:
                # FIXME - this doesn't deal with incorrectly large payloads
                return packet

    def _pack_str16(self, packet: bytearray, data: Union[bytes, str]) -> NoReturn:
        if isinstance(data, str):
            data = data.encode("utf-8")
        packet.extend(struct.pack("!H", len(data)))
        packet.extend(data)

    def _update_inflight(self) -> ErrorValues:
        for m in self._out_messages.values():
            if self._inflight_messages < self._max_inflight_messages:
                if m.qos > 0 and m.state == MessageStates.QUEUED:
                    self._inflight_messages += 1
                    if m.qos == 1:
                        m.state = MessageStates.WAIT_FOR_PUBACK
                    elif m.qos == 2:
                        m.state = MessageStates.WAIT_FOR_PUBREC
                    rc = self._send_publish(m.mid, m.topic.encode('utf-8'), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                    if rc != 0:
                        return rc
            else:
                return ErrorValues.SUCCESS
        return ErrorValues.SUCCESS

    # MQTT Callbacks
    def addCallback(self, fun: Callable, key: str = None) -> Callable:
        key = key or fun.__name__
        if key in [f.name for f in fields(self._callbacks)]:
            setattr(self._callbacks, key, fun)
            return fun
        raise KeyError(f"Unknown callback name of {key}")

    def max_inflight_messages_set(self, inflight: int) -> NoReturn:
        if inflight < 0:
            raise ValueError("Invalid inflight.")
        self._max_inflight_messages = inflight

    def max_queued_messages_set(self, queue_size: int) -> NoReturn:
        if queue_size < 0:
            raise ValueError("Invalid queue size.")
        if not isinstance(queue_size, int):
            raise ValueError("Invalid type of queue size.")
        self._max_queued_messages = queue_size

    def message_callback_add(self, sub: Union[bytes, str], callback: OnMessageCallback) -> NoReturn:
        sub = sub.decode("utf-8") if isinstance(sub, bytes) else sub
        if callback is None or sub is None:
            raise ValueError("sub and callback must both be defined.")
        self._on_message_filtered[sub] = callback

    def topic_callback(self, sub: Union[bytes, str]) -> Callable[[OnMessageCallback], OnMessageCallback]:
        sub = sub.decode("utf-8") if isinstance(sub, bytes) else sub

        def decorator(func: OnMessageCallback) -> OnMessageCallback:
            self.message_callback_add(sub, func)
            return func
        return decorator

    def message_callback_remove(self, sub: Union[bytes, str]) -> NoReturn:
        sub = sub.decode("utf-8") if isinstance(sub, bytes) else sub
        if sub is None:
            raise ValueError("sub must defined.")

        try:
            del self._on_message_filtered[sub]
        except KeyError:  # no such subscription
            pass

    # General Helpers
    def make_id(self) -> bytes:
        return f"{uuid.uuid4().int}"[:22].encode("utf-8")
