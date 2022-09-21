import time
import struct
import uuid

from collections import OrderedDict, deque
from dataclasses import asdict
from typing import Any, Callable, Deque, Generator, List, Literal, NoReturn, Optional, Tuple, Union
from twisted.internet import reactor as Reactor
from twisted.internet.endpoints import TCP4ClientEndpoint, TCP6ClientEndpoint
from twisted.internet.protocol import Protocol, connectionDone
from twisted.logger import Logger, LogLevel
from twisted.python.failure import Failure
from .consts import MQTT_CLEAN_START_FIRST_ONLY
from .matcher import MQTTMatcher
from .message import MQTTMessage, MQTTMessageInfo
from .properties import Properties
from .subscribeoptions import SubscribeOptions
from .utils import (
    ConnAckCodes, ConnectionStates, ErrorValues, LogLevels, MessageStates, MessageTypes, Transports, Versions,  # Enums
    DummyLock,  # Contexts
    InPacket, OutPacket,  # Dataclasses
    CallbackMixin,  # Mixins
    ReasonCodes,  # Assorted
    base62, filter_wildcard_len_check, pack_remaining_length, pack_str16, topic_wildcard_len_check  # Utils
)

# Types
OnMessageCallback = Callable[["MQTTProtocol", Any, MQTTMessage], NoReturn]
Payload = Union[bytes, bytearray, int, float, str]
QOS = Literal[0, 1, 2]
Subscriptions = List[Tuple[str, Literal[0, 1, 2]]]
TopicSubscription = Union[
    str,
    Tuple[Union[str, List[str]], int],
    Tuple[Union[str, List[str]], SubscribeOptions]  # MQTTv5
]

# Vars
log = Logger(namespace="mqtt")
try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time


class MQTTProtocol(CallbackMixin, Protocol):
    factory: "MQTTFactory"
    addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]
    broker: str
    _reactor: Reactor
    _transport: Transports
    _protocol: Versions
    _userdata: Any
    _client_id: bytes
    # Connection
    _keepalive: int = 60
    _connect_timeout: float = 5.0
    _clean_session: bool
    _clean_start: int
    _connect_properties: Optional[Properties] = None
    # Authentication & Security
    _username: bytes = None
    _password: bytes = None
    # Packet Processing
    _in_packet: InPacket
    _out_packet: Deque[OutPacket]
    _last_msg_in: float
    _last_msg_out: float
    _pinger: Reactor.callLater = None
    _last_mid: int = 0
    _state: ConnectionStates
    _out_messages: OrderedDict
    _in_messages: OrderedDict
    _max_inflight_messages: int = 20
    _inflight_messages: int = 0
    _max_queued_messages: int = 0
    # _queued_messages: int = 0
    _on_message_filtered: MQTTMatcher
    # Will
    _will_properties: Optional[Properties] = None
    _will: bool = False
    _will_topic: bytes = b""
    _will_payload: bytes = b""
    _will_qos: QOS = 0
    _will_retain: bool = False
    # Mutex/Locks
    _in_callback_mutex: DummyLock = DummyLock()
    _callback_mutex: DummyLock = DummyLock()
    _msgtime_mutex: DummyLock = DummyLock()
    _out_message_mutex: DummyLock = DummyLock()
    _in_message_mutex: DummyLock = DummyLock()
    _reconnect_delay_mutex: DummyLock = DummyLock()
    _mid_generate_mutex: DummyLock = DummyLock()
    # Assorted
    _subs: Subscriptions = []
    _mqttv5_first_connect: bool = True  # for clean_start == MQTT_CLEAN_START_FIRST_ONLY
    suppress_exceptions: bool = False  # For callbacks
    __factory_props__ = (
        "_reactor", "_protocol", "_userdata", "_client_id", "_subs", "_clean_session", "_username", "_password"
    )

    def __init__(self, factory: "MQTTFactory", addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]) -> None:
        self.factory = factory
        self.addr = addr
        self.broker = f"mqtt://{addr.host}:{addr.port}"

        self._in_packet = InPacket(
            command=0,
            have_remaining=0,
            remaining_count=[],
            remaining_mult=1,
            remaining_length=0,
            packet=bytearray(b""),
            pos=0
        )
        self._out_packet = deque()
        self._last_msg_in = time_func()
        self._last_msg_out = time_func()
        self._state = ConnectionStates.NEW
        self._out_messages = OrderedDict()
        self._in_messages = OrderedDict()
        self._on_message_filtered = MQTTMatcher()

        # Copy properties from factory
        for prop in self.__factory_props__:
            val = getattr(self.factory, prop, None)
            setattr(self, prop, val)
        self._clean_start = MQTT_CLEAN_START_FIRST_ONLY
        for field, val in asdict(self.factory._callbacks).items():
            if val:
                setattr(self, field, val)

    # Twisted Interface
    def connectionMade(self) -> NoReturn:
        try:
            yield self.connect()
        except Exception as err:
            self._easy_log(LogLevels.ERR, "Connecting to {} raised {!s}", self.broker, err)
            raise err
        else:
            self._easy_log(LogLevels.INFO, "Connected to {}", self.broker)

    def connectionLost(self, reason: Failure = connectionDone) -> NoReturn:
        self._easy_log(LogLevels.DEBUG, "Connection to MQTT Broker lost: {}", reason.getErrorMessage())
        self.connect()

    def dataReceived(self, data: bytes) -> ErrorValues:
        # self._easy_log(LogLevels.DEBUG, "Received: bits={} - `{}`", len(data), data)
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
            command=0,
            remaining_length=0,
            packet=bytearray(b""),
            pos=0
        )
        self._last_msg_in = time_func()
        if rc != ErrorValues.SUCCESS:
            self._easy_log(LogLevels.DEBUG, "Process error: {} -> {}", rc, rc.name)
            return rc
        return ErrorValues.SUCCESS

    # MQTT Interface
    def connect(self, clean_start: int = MQTT_CLEAN_START_FIRST_ONLY, properties: Optional[Properties] = None) -> NoReturn:
        self._easy_log(LogLevels.DEBUG, "Connect to MQTT Broker {} with {}", self.addr, self._protocol.name)
        if self._protocol == Versions.v5:
            self._mqttv5_first_connect = True
        else:
            if clean_start != MQTT_CLEAN_START_FIRST_ONLY:
                raise ValueError("Clean start only applies to MQTT V5")
            if properties is not None:
                raise ValueError("Properties only apply to MQTT V5")
        self._send_connect(self._keepalive)
        for sub in self._subs:
            self.subscribe(*sub)
        self._update_pinger()

    def publish(self, topic: str, payload: Payload = None, qos: QOS = 0, retain: bool = False, properties: Optional[Properties] = None) -> MQTTMessageInfo:
        if self._protocol != Versions.v5:
            if topic is None or len(topic) == 0:
                raise ValueError("Invalid topic.")

        topic = topic.encode("utf-8")
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
        message.payload = local_payload
        message.qos = qos
        message.retain = retain
        message.dup = False
        message.properties = properties
        with self._out_message_mutex:
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

    def username_pw_set(self, username: str, password: Optional[str] = None) -> NoReturn:
        # [MQTT-3.1.3-11] User name must be UTF-8 encoded string
        self._username = None if username is None else username.encode("utf-8")
        self._password = password.encode("utf-8") if isinstance(password, str) else password

    def is_connected(self) -> bool:
        return self._state == ConnectionStates.CONNECTED

    def disconnect(self, rc: ReasonCodes = None, properties: Optional[Properties] = None) -> ErrorValues:
        self._state = ConnectionStates.DISCONNECTING
        if self._pinger is not None:
            self._pinger.cancel()
        return self._send_disconnect(rc, properties)

    def subscribe(self, topic: TopicSubscription, qos: QOS = 0, options: Optional[SubscribeOptions] = None, properties: Optional[Properties] = None) -> Tuple[ErrorValues, int]:
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

    def unsubscribe(self, topic: Union[str, List[str]], properties: Optional[Properties] = None) -> Tuple[ErrorValues, int]:
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

    # Packet Processing
    def loop_write(self, max_packets: int = None) -> Generator[ErrorValues, None, ErrorValues]:
        count = max_packets if isinstance(max_packets, int) and max_packets > 0 else len(self._out_packet)
        for idx in range(0, count):
            try:
                rc = self._packet_write()
                if rc == ErrorValues.AGAIN:
                    yield ErrorValues.SUCCESS
                if rc > 0:
                    yield self._loop_rc_handle(rc)
                yield ErrorValues.SUCCESS
            except Exception as err:
                self._easy_log(LogLevels.ERR, "Write packet error: {}", err)
        return ErrorValues.SUCCESS

    def _packet_handle(self) -> ErrorValues:
        cmd = self._in_packet.command & 0xF0
        rslt = ErrorValues.PROTOCOL
        if cmd == MessageTypes.PINGREQ:
            rslt = self._handle_pingreq()
        elif cmd == MessageTypes.PINGRESP:
            rslt = self._handle_pingresp()
        elif cmd == MessageTypes.PUBACK:
            rslt = self._handle_pubackcomp("PUBACK")
        elif cmd == MessageTypes.PUBCOMP:
            rslt = self._handle_pubackcomp("PUBCOMP")
        elif cmd == MessageTypes.PUBLISH:
            rslt = self._handle_publish()
        elif cmd == MessageTypes.PUBREC:
            rslt = self._handle_pubrec()
        elif cmd == MessageTypes.PUBREL:
            rslt = self._handle_pubrel()
        elif cmd == MessageTypes.CONNACK:
            rslt = self._handle_connack()
        elif cmd == MessageTypes.SUBACK:
            rslt = self._handle_suback()
        elif cmd == MessageTypes.UNSUBACK:
            rslt = self._handle_unsuback()
        elif cmd == MessageTypes.DISCONNECT and self._protocol == Versions.v5:  # only allowed in MQTT 5.0
            rslt = self._handle_disconnect()
        else:
            # If we don't recognise the command, return an error straight away.
            self._easy_log(LogLevels.ERR, "Error: Unrecognised command {}", cmd)
        return rslt

    # Packet Sending
    def _send_pingreq(self) -> ErrorValues:
        self._easy_log(LogLevels.DEBUG, "Sending PINGREQ")
        rc = self._send_simple_command(MessageTypes.PINGREQ)
        return rc

    def _send_pingresp(self) -> ErrorValues:
        self._easy_log(LogLevels.DEBUG, "Sending PINGRESP")
        return self._send_simple_command(MessageTypes.PINGRESP)

    def _send_puback(self, mid: int) -> ErrorValues:
        self._easy_log(LogLevels.DEBUG, "Sending PUBACK (Mid: {})", mid)
        return self._send_command_with_mid(MessageTypes.PUBACK, mid, False)

    def _send_pubcomp(self, mid: int) -> ErrorValues:
        self._easy_log(LogLevels.DEBUG, "Sending PUBCOMP (Mid: {})", mid)
        return self._send_command_with_mid(MessageTypes.PUBCOMP, mid, False)

    def _send_publish(self, mid: int, topic: str, payload: Payload = b"", qos: QOS = 0, retain: bool = False, dup: bool = False, info: Optional[MQTTMessageInfo] = None, properties: Optional[Properties] = None) -> ErrorValues:
        # we assume that topic and payload are already properly encoded
        assert not isinstance(topic, str) and not isinstance(payload, str) and payload is not None
        command = MessageTypes.PUBLISH | ((dup & 0x1) << 3) | (qos << 1) | retain
        packet = bytearray([command, ])
        payloadlen = len(payload)
        remaining_length = 2 + len(topic) + payloadlen
        log_args = [dup, qos, retain, mid, topic]
        if payloadlen == 0:
            if self._protocol == Versions.v5:
                self._easy_log(LogLevels.DEBUG, "Sending PUBLISH (d{}, q{}, r{}, m{}), '{}', properties={} (NULL payload)", *log_args, properties)
            else:
                self._easy_log(LogLevels.DEBUG, "Sending PUBLISH (d{}, q{}, r{}, m{}), '{}' (NULL payload)", *log_args)
        else:
            if self._protocol == Versions.v5:
                self._easy_log(LogLevels.DEBUG, "Sending PUBLISH (d{}, q{}, r{}, m{}), '{}', properties={}, ... ({} bytes)", *log_args, properties, payloadlen)
            else:
                self._easy_log(LogLevels.DEBUG, "Sending PUBLISH (d{}, q{}, r{}, m{}), '{}', ... ({} bytes)", *log_args, payloadlen)

        if qos > 0:
            # For message id
            remaining_length += 2

        if self._protocol == Versions.v5:
            packed_properties = b"\x00" if properties is None else properties.pack()
            remaining_length += len(packed_properties)

        pack_remaining_length(packet, remaining_length)
        pack_str16(packet, topic)
        if qos > 0:
            # For message id
            packet.extend(struct.pack("!H", mid))

        if self._protocol == Versions.v5:
            packet.extend(packed_properties)

        packet.extend(payload)
        return self._packet_queue(MessageTypes.PUBLISH, packet, mid, qos, info)

    def _send_pubrec(self, mid: int) -> ErrorValues:
        self._easy_log(LogLevels.DEBUG, "Sending PUBREC (Mid: {})", mid)
        return self._send_command_with_mid(MessageTypes.PUBREC, mid, False)

    def _send_pubrel(self, mid: int) -> ErrorValues:
        self._easy_log(LogLevels.DEBUG, "Sending PUBREL (Mid: {})", mid)
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
        packet = bytearray([command, ])
        pack_remaining_length(packet, remaining_length)
        packet.extend(struct.pack("!H" + str(len(protocol)) + "sBBH", len(protocol), protocol, proto_ver, connect_flags, keepalive))
        if self._protocol == Versions.v5:
            packet += packed_connect_properties

        pack_str16(packet, self._client_id)
        if self._will:
            if self._protocol == Versions.v5:
                packet += packed_will_properties
            pack_str16(packet, self._will_topic)
            pack_str16(packet, self._will_payload)

        if self._username is not None:
            pack_str16(packet, self._username)
            if self._password is not None:
                pack_str16(packet, self._password)

        self._keepalive = keepalive
        log_args = [
            (connect_flags & 0x80) >> 7, (connect_flags & 0x40) >> 6, (connect_flags & 0x20) >> 5,
            (connect_flags & 0x18) >> 3, (connect_flags & 0x4) >> 2, (connect_flags & 0x2) >> 1, keepalive,
            self._client_id
        ]
        if self._protocol == Versions.v5:
            self._easy_log(LogLevels.DEBUG, "Sending CONNECT (u{}, p{}, wr{}, wq{}, wf{}, c{}, k{}) client_id={} properties={}", *log_args, self._connect_properties)
        else:
            self._easy_log(LogLevels.DEBUG, "Sending CONNECT (u{}, p{}, wr{}, wq{}, wf{}, c{}, k{}) client_id={}", *log_args)
        return self._packet_queue(command, packet, 0, 0)

    def _send_disconnect(self, reasoncode: Optional[int] = None, properties: Optional[Properties] = None) -> ErrorValues:
        if self._protocol == Versions.v5:
            self._easy_log(LogLevels.DEBUG, "Sending DISCONNECT reasonCode={} properties={}", reasoncode, properties)
        else:
            self._easy_log(LogLevels.DEBUG, "Sending DISCONNECT")

        remaining_length = 0
        command = MessageTypes.DISCONNECT
        packet = bytearray([command, ])
        if self._protocol == Versions.v5:
            if properties is not None or reasoncode is not None:
                if reasoncode is None:
                    reasoncode = ReasonCodes(MessageTypes.DISCONNECT >> 4, identifier=0)
                remaining_length += 1
                if properties is not None:
                    packed_props = properties.pack()
                    remaining_length += len(packed_props)

        pack_remaining_length(packet, remaining_length)
        if self._protocol == Versions.v5:
            if reasoncode is not None:
                packet += reasoncode.pack()
                if properties is not None:
                    packet += packed_props

        return self._packet_queue(command, packet, 0, 0)

    def _send_subscribe(self, dup: bool, topics: List[Tuple[bytes, Union[int, SubscribeOptions]]], properties: Optional[Properties] = None) -> Tuple[ErrorValues, int]:
        remaining_length = 2
        if self._protocol == Versions.v5:
            if properties is None:
                packed_subscribe_properties = b"\x00"
            else:
                packed_subscribe_properties = properties.pack()
            remaining_length += len(packed_subscribe_properties)
        for t, _ in topics:
            remaining_length += 2 + len(t) + 1

        command = MessageTypes.SUBSCRIBE | (dup << 3) | 0x2
        packet = bytearray([command, ])
        pack_remaining_length(packet, remaining_length)
        local_mid = self._mid_generate()
        packet.extend(struct.pack("!H", local_mid))

        if self._protocol == Versions.v5:
            packet += packed_subscribe_properties

        log_topics = []
        for t, q in topics:
            pack_str16(packet, t)
            log_topic = f"'{t.decode('utf-8')}': "
            if self._protocol == Versions.v5:
                packet += q.pack()
                log_topic += f"{{{q.json()}}}"
            else:
                packet.append(q)
                log_topic += f"{q}"
            log_topics.append(log_topic)

        self._easy_log(LogLevels.DEBUG, "Sending SUBSCRIBE (d{}, m{}) [{}]", dup, local_mid, ", ".join(log_topics))
        return self._packet_queue(command, packet, local_mid, 1), local_mid

    def _send_unsubscribe(self, dup: bool, topics: List[bytes], properties: Optional[Properties] = None) -> Tuple[ErrorValues, int]:
        remaining_length = 2
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
        pack_remaining_length(packet, remaining_length)
        local_mid = self._mid_generate()
        packet.extend(struct.pack("!H", local_mid))

        if self._protocol == Versions.v5:
            packet += packed_unsubscribe_properties

        for t in topics:
            pack_str16(packet, t)

        # topics_repr = ", ".join("'"+topic.decode("utf8")+"'" for topic in topics)
        if self._protocol == Versions.v5:
            self._easy_log(LogLevels.DEBUG, "Sending UNSUBSCRIBE (d{}, m{}) {} {}", dup, local_mid, properties, topics)
        else:
            self._easy_log(LogLevels.DEBUG, "Sending UNSUBSCRIBE (d{}, m{}) {}", dup, local_mid, topics)
        return self._packet_queue(command, packet, local_mid, 1), local_mid

    # Packet Handling/Receiving
    def _handle_pingreq(self) -> ErrorValues:
        if self._in_packet.remaining_length != 0:
            return ErrorValues.PROTOCOL

        self._easy_log(LogLevels.DEBUG, "Received PINGREQ")
        return self._send_pingresp()

    def _handle_pingresp(self) -> ErrorValues:
        if self._in_packet.remaining_length != 0:
            return ErrorValues.PROTOCOL

        # No longer waiting for a PINGRESP.
        self._update_pinger()
        self._easy_log(LogLevels.DEBUG, "Received PINGRESP")
        return ErrorValues.SUCCESS

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
                self._easy_log(LogLevels.DEBUG, "Received CONNACK ({}, {}), attempting downgrade to MQTT v3.1.", flags, result)
                # Downgrade to MQTT v3.1
                self._protocol = Versions.v31
                return self.connect()

            if result == ConnAckCodes.REFUSED_IDENTIFIER_REJECTED and self._client_id == b"":
                self._easy_log(LogLevels.DEBUG, "Received CONNACK ({}, {}), attempting to use non-empty CID", flags, result)
                self._client_id = base62(uuid.uuid4().int, padding=22).encode("utf-8")
                return self.connect()

        if result == 0:
            self._state = ConnectionStates.CONNECTED

        if self._protocol == Versions.v5:
            self._easy_log(LogLevels.DEBUG, "Received CONNACK ({}, {}) properties={}", flags, reason, properties)
        else:
            self._easy_log(LogLevels.DEBUG, "Received CONNACK ({}, {})", flags, result)

        # it won't be the first successful connect any more
        self._mqttv5_first_connect = False
        with self._callback_mutex:
            if on_connect := self.on_connect:
                flags_dict = {"session present": flags & 0x01}
                with self._in_callback_mutex:
                    try:
                        if self._protocol == Versions.v5:
                            on_connect(self, self._userdata, flags_dict, reason, properties)
                        else:
                            on_connect(self, self._userdata, flags_dict, result)
                    except Exception as err:
                        self._easy_log(LogLevels.ERR, "Caught exception in on_connect: {}", err)
                        if not self.suppress_exceptions:
                            raise

        if result == 0:
            rc = 0
            with self._out_message_mutex:
                for m in self._out_messages.values():
                    m.timestamp = time_func()
                    if m.state == MessageStates.QUEUED:
                        list(self.loop_write())  # Process outgoing messages that have just been queued up
                        return ErrorValues.SUCCESS

                    if m.qos == 0:
                        with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                            rc = self._send_publish(m.mid, m.topic.encode("utf-8"), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                        if rc != 0:
                            return rc
                    elif m.qos == 1:
                        if m.state == MessageStates.PUBLISH:
                            self._inflight_messages += 1
                            m.state = MessageStates.WAIT_FOR_PUBACK
                            with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                                rc = self._send_publish(m.mid, m.topic.encode("utf-8"), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                            if rc != 0:
                                return rc
                    elif m.qos == 2:
                        if m.state == MessageStates.PUBLISH:
                            self._inflight_messages += 1
                            m.state = MessageStates.WAIT_FOR_PUBREC
                            with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                                rc = self._send_publish(m.mid, m.topic.encode("utf-8"), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                            if rc != 0:
                                return rc
                        elif m.state == MessageStates.RESEND_PUBREL:
                            self._inflight_messages += 1
                            m.state = MessageStates.WAIT_FOR_PUBCOMP
                            with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                                rc = self._send_pubrel(m.mid)
                            if rc != 0:
                                return rc
            list(self.loop_write())  # Process outgoing messages that have just been queued up
            return rc
        if 0 < result < 6:
            return ErrorValues.CONN_REFUSED
        return ErrorValues.PROTOCOL

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

        self._easy_log(LogLevels.DEBUG, "Received {} (Mid: {})", cmd, mid)
        with self._out_message_mutex:
            if mid in self._out_messages:
                # Only inform the client the message has been sent once.
                return self._do_on_publish(mid)

        return ErrorValues.SUCCESS

    def _handle_disconnect(self) -> ErrorValues:
        packet_type = MessageTypes.DISCONNECT >> 4
        reasonCode = properties = None
        if self._in_packet.remaining_length > 2:
            reasonCode = ReasonCodes(packet_type)
            reasonCode.unpack(self._in_packet.packet)
            if self._in_packet.remaining_length > 3:
                properties = Properties(packet_type)
                properties.unpack(self._in_packet.packet[1:])
        self._easy_log(LogLevels.DEBUG, "Received DISCONNECT {} {}", reasonCode, properties)
        self._loop_rc_handle(reasonCode, properties)
        return ErrorValues.SUCCESS

    def _handle_suback(self) -> ErrorValues:
        self._easy_log(LogLevels.DEBUG, "Received SUBACK")
        mid, packet = struct.unpack(f"!H{len(self._in_packet.packet) - 2}s", self._in_packet.packet)
        if self._protocol == Versions.v5:
            properties = Properties(MessageTypes.SUBACK >> 4)
            _, props_len = properties.unpack(packet)
            reasoncodes = []
            for c in packet[props_len:]:
                reasoncodes.append(ReasonCodes(MessageTypes.SUBACK >> 4, identifier=c))
        else:
            granted_qos = struct.unpack(f"!B{len(packet)}", packet)

        with self._callback_mutex:
            if on_subscribe := self.on_subscribe:
                with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                    try:
                        if self._protocol == Versions.v5:
                            on_subscribe(self, self._userdata, mid, reasoncodes, properties)
                        else:
                            on_subscribe(self, self._userdata, mid, granted_qos)
                    except Exception as err:
                        self._easy_log(LogLevels.ERR, "Caught exception in on_subscribe: {}", err)
                        if not self.suppress_exceptions:
                            raise

        return ErrorValues.SUCCESS

    def _handle_publish(self) -> ErrorValues:
        header = self._in_packet.command
        message = MQTTMessage()
        message.dup = (header & 0x08) >> 3
        message.qos = (header & 0x06) >> 1
        message.retain = (header & 0x01)
        packet = self._packet_data()
        slen, packet = struct.unpack(f"!H{len(packet) - 2}s", packet)
        topic, packet = struct.unpack(f"!{slen}s{len(packet) - slen}s", packet)
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
            message.mid, packet = struct.unpack(f"!H{len(packet) - 2}s", packet)

        if self._protocol == Versions.v5:
            message.properties = Properties(MessageTypes.PUBLISH >> 4)
            _, props_len = message.properties.unpack(packet)
            packet = packet[props_len:]

        message.payload = packet
        log_args = [message.dup, message.qos, message.retain, message.mid, print_topic]
        if self._protocol == Versions.v5:
            self._easy_log(LogLevels.DEBUG, "Received PUBLISH (d{}, q{}, r{}, m{}), '{}', properties={}, ...  ({} bytes)", *log_args, message.properties, len(message.payload))
        else:
            self._easy_log(LogLevels.DEBUG, "Received PUBLISH (d{}, q{}, r{}, m{}), '{}', ...  ({} bytes)", *log_args, len(message.payload))

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
            with self._in_message_mutex:
                self._in_messages[message.mid] = message
            return rc
        return ErrorValues.PROTOCOL

    def _handle_pubrel(self) -> ErrorValues:
        if self._protocol == Versions.v5:
            if self._in_packet.remaining_length < 2:
                return ErrorValues.PROTOCOL
        elif self._in_packet.remaining_length != 2:
            return ErrorValues.PROTOCOL

        mid, = struct.unpack("!H", self._in_packet.packet)
        self._easy_log(LogLevels.DEBUG, "Received PUBREL (Mid: {})", mid)
        with self._in_message_mutex:
            if mid in self._in_messages:
                # Only pass the message on if we have removed it from the queue - this
                # prevents multiple callbacks for the same message.
                message = self._in_messages.pop(mid)
                self._handle_on_message(message)
                self._inflight_messages -= 1
                if self._max_inflight_messages > 0:
                    with self._out_message_mutex:
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
        self._easy_log(LogLevels.DEBUG, "Received PUBREC (Mid: {})", mid)

        with self._out_message_mutex:
            if mid in self._out_messages:
                msg = self._out_messages[mid]
                msg.state = MessageStates.WAIT_FOR_PUBCOMP
                msg.timestamp = time_func()
                return self._send_pubrel(mid)

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

        self._easy_log(LogLevels.DEBUG, "Received UNSUBACK (Mid: {})", mid)
        with self._callback_mutex:
            if on_unsubscribe := self.on_unsubscribe:
                with self._in_callback_mutex:
                    try:
                        if self._protocol == Versions.v5:
                            on_unsubscribe(self, self._userdata, mid, properties, reasoncodes)
                        else:
                            on_unsubscribe(self, self._userdata, mid)
                    except Exception as err:
                        self._easy_log(LogLevels.ERR, "Caught exception in on_unsubscribe: {}", err)
                        if not self.suppress_exceptions:
                            raise

        return ErrorValues.SUCCESS

    def _handle_on_message(self, message: MQTTMessage) -> NoReturn:
        try:
            topic = message.topic
        except UnicodeDecodeError:
            topic = None

        on_message_callbacks = []
        with self._callback_mutex:
            if topic is not None:
                for callback in self._on_message_filtered.iter_match(message.topic):
                    on_message_callbacks.append(callback)
            on_message = self.on_message if len(on_message_callbacks) == 0 else None

        for callback in on_message_callbacks:
            with self._in_callback_mutex:
                try:
                    callback(self, self._userdata, message)
                except Exception as err:
                    self._easy_log(LogLevels.ERR, "Caught exception in user defined callback function {}: {}", callback.__name__, err)
                    if not self.suppress_exceptions:
                        raise

        if on_message:
            with self._in_callback_mutex:
                try:
                    on_message(self, self._userdata, message)
                except Exception as err:
                    self._easy_log(LogLevels.ERR, "Caught exception in on_message: {}", err)
                    if not self.suppress_exceptions:
                        raise

    def _handle_on_connect_fail(self) -> NoReturn:
        with self._callback_mutex:
            if on_connect_fail := self.on_connect_fail:
                with self._in_callback_mutex:
                    try:
                        on_connect_fail(self, self._userdata)
                    except Exception as err:
                        self._easy_log(LogLevels.ERR, "Caught exception in on_connect_fail: {}", err)

    # Helpers
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

    def _do_on_disconnect(self, rc: int, properties: Optional[Properties] = None) -> NoReturn:
        with self._callback_mutex:
            if on_disconnect := self.on_disconnect:
                with self._in_callback_mutex:
                    try:
                        if self._protocol == Versions.v5:
                            on_disconnect(self, self._userdata, rc, properties)
                        else:
                            on_disconnect(self, self._userdata, rc)
                    except Exception as err:
                        self._easy_log(LogLevels.ERR, "Caught exception in on_disconnect: {}", err)
                        if not self.suppress_exceptions:
                            raise

    def _do_on_publish(self, mid: int) -> ErrorValues:
        with self._callback_mutex:
            if on_publish := self.on_publish:
                with self._in_callback_mutex:
                    try:
                        on_publish(self, self._userdata, mid)
                    except Exception as err:
                        self._easy_log(LogLevels.ERR, "Caught exception in on_publish: {}", err)
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

    def _easy_log(self, level: LogLevels, fmt: str, *args: Any, **kwargs: Any) -> NoReturn:
        buf = fmt.format(*args, **kwargs)
        if on_log := self.on_log:
            try:
                on_log(self, self._userdata, level, buf)
            except Exception:
                pass
        twisted_level = {
            LogLevels.DEBUG: LogLevel.debug,
            LogLevels.INFO: LogLevel.info,
            LogLevels.NOTICE: LogLevel.info,  # This has no direct equivalent level
            LogLevels.WARNING: LogLevel.warn,
            LogLevels.ERR: LogLevel.error
        }.get(level, LogLevel.info)
        log.emit(twisted_level, buf)

    def _loop_rc_handle(self, rc: ErrorValues, properties: Optional[Properties] = None) -> ErrorValues:
        if rc:
            self._send_disconnect(rc, properties)
            if self._state == ConnectionStates.DISCONNECTING:
                rc = ErrorValues.SUCCESS
            self._do_on_disconnect(rc, properties)
        return rc

    def _packet_data(self) -> bytearray:
        # Removes the control and variable length header
        lenLen = 1
        while self._in_packet.packet[lenLen] & 0x80:
            lenLen += 1
        return self._in_packet.packet[lenLen + 1:]

    def _mid_generate(self) -> int:
        with self._mid_generate_mutex:
            self._last_mid += 1
            if self._last_mid == 65536:
                self._last_mid = 1
            return self._last_mid

    def _packet_queue(self, command: int, packet: Union[bytes, bytearray], mid: int, qos: QOS, info: Optional[MQTTMessageInfo] = None) -> ErrorValues:
        self._out_packet.append(OutPacket(
            command=command,
            mid=mid,
            qos=qos,
            pos=0,
            packet=packet,
            info=info
        ))
        return list(self.loop_write(1))[0]

    def _packet_write(self) -> ErrorValues:
        while True:
            try:
                packet = self._out_packet.popleft()
            except IndexError:
                return ErrorValues.SUCCESS

            if self._state == ConnectionStates.NEW and packet.command == MessageTypes.CONNECT:
                pass
            elif self._state != ConnectionStates.CONNECTED:
                self._easy_log(LogLevels.DEBUG, "Queued packet: connection({}) -> {}", self._state.name, packet)
                self._out_packet.appendleft(packet)
                return ErrorValues.AGAIN

            try:
                data = bytes(packet.packet[packet.pos:])
                self.transport.write(data)
            except (AttributeError, ValueError):
                self._out_packet.appendleft(packet)
                return ErrorValues.SUCCESS
            except BlockingIOError:
                self._out_packet.appendleft(packet)
                return ErrorValues.AGAIN
            except ConnectionError:
                self._out_packet.appendleft(packet)
                return ErrorValues.CONN_LOST
            break

        with self._msgtime_mutex:
            self._last_msg_out = time_func()
        return ErrorValues.SUCCESS

    def _update_pinger(self) -> NoReturn:
        if self._pinger:
            if self._pinger.active():
                self._pinger.reset(self._keepalive)
            else:
                self._pinger = Reactor.callLater(self._keepalive, self._send_pingreq)
        else:
            self._pinger = Reactor.callLater(self._keepalive, self._send_pingreq)

    def _update_inflight(self) -> ErrorValues:
        # Don't lock message_mutex here
        for m in self._out_messages.values():
            if self._inflight_messages < self._max_inflight_messages:
                if m.qos > 0 and m.state == MessageStates.QUEUED:
                    self._inflight_messages += 1
                    if m.qos == 1:
                        m.state = MessageStates.WAIT_FOR_PUBACK
                    elif m.qos == 2:
                        m.state = MessageStates.WAIT_FOR_PUBREC
                    rc = self._send_publish(m.mid, m.topic.encode("utf-8"), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                    if rc != 0:
                        return rc
            else:
                return ErrorValues.SUCCESS
        return ErrorValues.SUCCESS

    # Callbacks
    def message_callback_add(self, sub: str, callback: OnMessageCallback) -> NoReturn:
        if callback is None or sub is None:
            raise ValueError("sub and callback must both be defined.")

        with self._callback_mutex:
            self._on_message_filtered[sub] = callback

    def topic_callback(self, sub: str) -> Callable[[OnMessageCallback], OnMessageCallback]:
        def decorator(func: OnMessageCallback) -> OnMessageCallback:
            self.message_callback_add(sub, func)
            return func
        return decorator

    def message_callback_remove(self, sub: str) -> NoReturn:
        if sub is None:
            raise ValueError("sub must defined.")

        with self._callback_mutex:
            try:
                del self._on_message_filtered[sub]
            except KeyError:  # no such subscription
                pass
