import time
import struct
import uuid

from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, List, Literal, Optional, Tuple, Union
from twisted.internet.endpoints import TCP4ClientEndpoint, TCP6ClientEndpoint
from twisted.internet.protocol import Protocol, connectionDone
from twisted.internet.task import LoopingCall
from twisted.logger import Logger
from twisted.python.failure import Failure
from .consts import MQTT_CLEAN_START_FIRST_ONLY, MQTT_BRIDGE
from .enums import ConnAckCodes, ConnectionStates, ErrorValues, MessageStates, Versions, LogLevel, MessageTypes
from .message import MQTTMessage, MQTTMessageInfo
from .properties import Properties
from .reasoncodes import ReasonCodes
from .subscribeoptions import SubscribeOptions
from .utils import filter_wildcard_len_check

try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time


# Vars
SUBSCRIPTIONS = List[Tuple[str, Literal[0, 1, 2]]]
log = Logger(namespace="mqtt")


@dataclass
class Packet:
    command: MessageTypes = MessageTypes.NEW
    packet: bytearray = field(default_factory=bytearray)
    to_process: int = 0
    pos: int = 0


@dataclass
class InPacket(Packet):
    have_remaining: int = 0
    remaining_count: list = field(default_factory=list)
    remaining_mult: int = 1
    remaining_length: int = 0


@dataclass
class OutPacket(Packet):
    mid: int = 0
    qos: int = 0
    info: Any = None


class MQTTProtocol(Protocol):
    factory: "MQTTFactory"
    addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]
    broker: str
    protocol: Versions
    client_id: bytes
    # MQTT
    _initialTimeout: int
    _window: int
    _keepalive: int = 60
    _subs: SUBSCRIPTIONS = []
    # Connection
    _connect_properties: Properties = None
    # Auth
    _username: bytes = None
    _password: bytes = None
    # Packet processing
    _buffer: bytearray = bytearray()
    _in_packet: InPacket = InPacket()
    _out_packet: Deque[OutPacket] = deque()
    # Will
    _will_properties: Properties = None
    _will: bool = False
    _will_topic: bytes = b""
    _will_payload: bytes = b""
    _will_qos: Literal[0, 1, 2] = 0
    _will_retain: bool = False
    # Callback functions
    _on_log: Callable = None
    _on_connect: Callable[[Any, Any, Any, ReasonCodes, Optional[Properties]], None] = None
    _on_connect_fail: Callable = None
    _on_subscribe: Callable = None
    _on_message: Callable = None
    _on_publish: Callable = None
    _on_unsubscribe: Callable = None
    _on_disconnect: Callable = None
    # Assorted
    _client_mode: Literal[0, 1]
    _state: ConnectionStates
    _out_messages: OrderedDict = OrderedDict()
    _in_messages: OrderedDict = OrderedDict()
    _pingLoop: LoopingCall
    _last_mid: int = 0
    # for clean_start == MQTT_CLEAN_START_FIRST_ONLY
    _mqttv5_first_connect: bool = True
    # Consts
    MAX_WINDOW = 16  # Max value of in-flight PUBLISH/SUBSCRIBE/UNSUBSCRIBE
    TIMEOUT_INITIAL = 4  # Initial timeout for retransmissions
    TIMEOUT_MAX_INITIAL = 1024  # Maximum value for initial timeout

    def __init__(self, factory: "MQTTFactory", addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]):
        self.factory = factory
        self.addr = addr
        self.broker = f"mqtt://{addr.host}:{addr.port}"
        self._state = ConnectionStates.NEW
        self.protocol = self.factory._protocol
        self.client_id = self.factory._client_id
        self._subs = self.factory._subs
        self.clean_session = self.factory._clean_session
        self._client_mode = self.factory._client_mode
        self._clean_start = MQTT_CLEAN_START_FIRST_ONLY
        self._pingLoop = LoopingCall(self._send_pingreq)

    # Twisted Interface
    def connectionMade(self):
        try:
            yield self.connect()
        except Exception as err:
            log.error("Connecting to {broker} raised {excp!s}", broker=self.broker, excp=err)
            raise err
        else:
            log.info("Connected to {broker}", broker=self.broker)

    def connectionLost(self, reason: Failure = connectionDone):
        log.debug(f"--- Connection to MQTT Broker lost: {reason.getErrorMessage()}")
        if self._pingLoop.running:
            self._pingLoop.stop()
        self.connect()

    def dataReceived(self, data: bytes):
        # log.debug("Packet: bits={data_len} - `{data}`", data_len=len(data), data=data)
        rc = self._packet_read(data)
        if rc != ErrorValues.SUCCESS:
            log.debug("Process error: {rc} -> {msg}", rc=rc, msg=rc.name)

    def doConnect(self, keepalive: int = 60):
        proto_ver = self.protocol
        # hard-coded UTF-8 encoded string
        protocol = b"MQTT" if proto_ver >= Versions.v311 else b"MQIsdp"
        remaining_length = 2 + len(protocol) + 1 + 1 + 2 + 2 + len(self.client_id)
        connect_flags = 0
        if self.protocol == Versions.v5:
            if self._clean_start is True:
                connect_flags |= 0x02
            elif self._clean_start == MQTT_CLEAN_START_FIRST_ONLY and self._mqttv5_first_connect:
                connect_flags |= 0x02
        elif self.clean_session:
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

        if self.protocol == Versions.v5:
            if self._connect_properties is None:
                packed_connect_properties = b'\x00'
            else:
                packed_connect_properties = self._connect_properties.pack()
            remaining_length += len(packed_connect_properties)
            if self._will:
                if self._will_properties is None:
                    packed_will_properties = b'\x00'
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
        if self.protocol == Versions.v5:
            packet += packed_connect_properties

        self._pack_str16(packet, self.client_id)
        if self._will:
            if self.protocol == Versions.v5:
                packet += packed_will_properties
            self._pack_str16(packet, self._will_topic)
            self._pack_str16(packet, self._will_payload)

        if self._username is not None:
            self._pack_str16(packet, self._username)
            if self._password is not None:
                self._pack_str16(packet, self._password)

        self._keepalive = keepalive
        if self.protocol == Versions.v5:
            self._easy_log(
                LogLevel.DEBUG,
                "Sending CONNECT (u%d, p%d, wr%d, wq%d, wf%d, c%d, k%d) client_id=%s properties=%s",
                (connect_flags & 0x80) >> 7,
                (connect_flags & 0x40) >> 6,
                (connect_flags & 0x20) >> 5,
                (connect_flags & 0x18) >> 3,
                (connect_flags & 0x4) >> 2,
                (connect_flags & 0x2) >> 1,
                keepalive,
                self.client_id,
                self._connect_properties
            )
        else:
            self._easy_log(
                LogLevel.DEBUG,
                "Sending CONNECT (u%d, p%d, wr%d, wq%d, wf%d, c%d, k%d) client_id=%s",
                (connect_flags & 0x80) >> 7,
                (connect_flags & 0x40) >> 6,
                (connect_flags & 0x20) >> 5,
                (connect_flags & 0x18) >> 3,
                (connect_flags & 0x4) >> 2,
                (connect_flags & 0x2) >> 1,
                keepalive,
                self.client_id
            )
        return self._packet_queue(command, packet, 0, 0)

    # MQTT Interface
    def connect(self, clean_start=MQTT_CLEAN_START_FIRST_ONLY, properties=None):
        """API Entry Point"""
        log.debug(f"--- Connect to MQTT Broker {self.addr} with {self.protocol.name}")
        if self.protocol == Versions.v5:
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

    def setTimeout(self, timeout):
        """API Entry Point"""
        if not (1 <= timeout <= self.TIMEOUT_MAX_INITIAL):
            raise ValueError(timeout)
        self._initialTimeout = timeout

    def setWindowSize(self, size):
        """API Entry Point"""
        if not (0 < size <= self.MAX_WINDOW):
            raise ValueError(f"Invalid window size of {size}")
        self._window = min(size, self.MAX_WINDOW)

    def publish(self, topic: str, payload: Union[bytes, bytearray, int, float, str, None] = None, qos: int = 0, retain: bool = False, properties: Properties = None):
        if self.protocol != Versions.v5:
            if topic is None or len(topic) == 0:
                raise ValueError('Invalid topic.')

        topic = topic.encode('utf-8')
        if self._topic_wildcard_len_check(topic) != ErrorValues.SUCCESS:
            raise ValueError('Publish topic cannot contain wildcards.')

        if qos < 0 or qos > 2:
            raise ValueError('Invalid QoS level.')

        if isinstance(payload, str):
            local_payload = payload.encode('utf-8')
        elif isinstance(payload, (bytes, bytearray)):
            local_payload = payload
        elif isinstance(payload, (int, float)):
            local_payload = str(payload).encode('ascii')
        elif payload is None:
            local_payload = b''
        else:
            raise TypeError('payload must be a string, bytearray, int, float or None.')

        if len(local_payload) > 268435455:
            raise ValueError('Payload too large.')

        local_mid = self._mid_generate()
        if qos == 0:
            info = MQTTMessageInfo(local_mid)
            rc = self._send_publish(local_mid, topic, local_payload, qos, retain, False, info, properties)
            info.rc = rc
            return info
        else:
            message = MQTTMessage(local_mid, topic)
            message.timestamp = time_func()
            message.payload = local_payload
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
            else:
                message.state = MessageStates.QUEUED
                message.info.rc = ErrorValues.SUCCESS
                return message.info

    def username_pw_set(self, username: Union[bytes, str], password: Optional[Union[bytes, str]] = None):
        # [MQTT-3.1.3-11] User name must be UTF-8 encoded string
        self._username = None if username is None else username.encode('utf-8')
        self._password = password
        if isinstance(self._password, str):
            self._password = self._password.encode('utf-8')

    def disconnect(self, reasoncode=None, properties=None):
        self._state = ConnectionStates.DISCONNECTING
        return self._send_disconnect(reasoncode, properties)

    def subscribe(self, topic: str, qos: int = 0, options: Optional[SubscribeOptions] = None, properties: Properties = None):
        topic_qos_list = None
        if isinstance(topic, tuple):
            if self.protocol == Versions.v5:
                topic, options = topic
                if not isinstance(options, SubscribeOptions):
                    raise ValueError('Subscribe options must be instance of SubscribeOptions class.')
            else:
                topic, qos = topic

        if isinstance(topic, str):
            if qos < 0 or qos > 2:
                raise ValueError('Invalid QoS level.')
            if self.protocol == Versions.v5:
                if options is None:
                    # if no options are provided, use the QoS passed instead
                    options = SubscribeOptions(qos=qos)
                elif qos != 0:
                    raise ValueError('Subscribe options and qos parameters cannot be combined.')
                if not isinstance(options, SubscribeOptions):
                    raise ValueError('Subscribe options must be instance of SubscribeOptions class.')
                topic_qos_list = [(topic.encode('utf-8'), options)]
            else:
                if topic is None or len(topic) == 0:
                    raise ValueError('Invalid topic.')
                topic_qos_list = [(topic.encode('utf-8'), qos)]
        elif isinstance(topic, list):
            topic_qos_list = []
            if self.protocol == Versions.v5:
                for t, o in topic:
                    if not isinstance(o, SubscribeOptions):
                        # then the second value should be QoS
                        if o < 0 or o > 2:
                            raise ValueError('Invalid QoS level.')
                        o = SubscribeOptions(qos=o)
                    topic_qos_list.append((t.encode('utf-8'), o))
            else:
                for t, q in topic:
                    if q < 0 or q > 2:
                        raise ValueError('Invalid QoS level.')
                    if t is None or len(t) == 0 or not isinstance(t, str):
                        raise ValueError('Invalid topic.')
                    topic_qos_list.append((t.encode('utf-8'), q))

        if topic_qos_list is None:
            raise ValueError("No topic specified, or incorrect topic type.")

        if any(filter_wildcard_len_check(topic) != ErrorValues.SUCCESS for topic, _ in topic_qos_list):
            raise ValueError('Invalid subscription filter.')

        return self._send_subscribe(False, topic_qos_list, properties)

    def unsubscribe(self, topic: str, properties: Properties = None):
        topic_list = None
        if topic is None:
            raise ValueError('Invalid topic.')
        if isinstance(topic, str):
            if len(topic) == 0:
                raise ValueError('Invalid topic.')
            topic_list = [topic.encode('utf-8')]
        elif isinstance(topic, list):
            topic_list = []
            for t in topic:
                if len(t) == 0 or not isinstance(t, str):
                    raise ValueError('Invalid topic.')
                topic_list.append(t.encode('utf-8'))

        if topic_list is None:
            raise ValueError("No topic specified, or incorrect topic type.")

        return self._send_unsubscribe(False, topic_list, properties)

    # MQTT packet sending
    def _send_pingreq(self):
        self._easy_log(LogLevel.DEBUG, "Sending PINGREQ")
        rc = self._send_simple_command(MessageTypes.PINGREQ)
        if rc == ErrorValues.SUCCESS:
            self._ping_t = time_func()
        return rc

    def _send_pingresp(self):
        self._easy_log(LogLevel.DEBUG, "Sending PINGRESP")
        return self._send_simple_command(MessageTypes.PINGRESP)

    def _send_puback(self, mid):
        self._easy_log(LogLevel.DEBUG, "Sending PUBACK (Mid: %d)", mid)
        return self._send_command_with_mid(MessageTypes.PUBACK, mid, False)

    def _send_pubcomp(self, mid):
        self._easy_log(LogLevel.DEBUG, "Sending PUBCOMP (Mid: %d)", mid)
        return self._send_command_with_mid(MessageTypes.PUBCOMP, mid, False)

    def _send_publish(self, mid, topic, payload=b'', qos=0, retain=False, dup=False, info=None, properties=None):
        # we assume that topic and payload are already properly encoded
        assert not isinstance(topic, str) and not isinstance(payload, str) and payload is not None
        command = MessageTypes.PUBLISH | ((dup & 0x1) << 3) | (qos << 1) | retain
        packet = bytearray()
        packet.append(command)
        payloadlen = len(payload)
        remaining_length = 2 + len(topic) + payloadlen
        if payloadlen == 0:
            if self.protocol == Versions.v5:
                self._easy_log(LogLevel.DEBUG, "Sending PUBLISH (d%d, q%d, r%d, m%d), '%s', properties=%s (NULL payload)", dup, qos, retain, mid, topic, properties)
            else:
                self._easy_log(LogLevel.DEBUG, "Sending PUBLISH (d%d, q%d, r%d, m%d), '%s' (NULL payload)", dup, qos, retain, mid, topic)
        else:
            if self.protocol == Versions.v5:
                self._easy_log(LogLevel.DEBUG, "Sending PUBLISH (d%d, q%d, r%d, m%d), '%s', properties=%s, ... (%d bytes)", dup, qos, retain, mid, topic, properties, payloadlen)
            else:
                self._easy_log(LogLevel.DEBUG, "Sending PUBLISH (d%d, q%d, r%d, m%d), '%s', ... (%d bytes)", dup, qos, retain, mid, topic, payloadlen)

        if qos > 0:
            # For message id
            remaining_length += 2

        if self.protocol == Versions.v5:
            packed_properties = b'\x00' if properties is None else properties.pack()
            remaining_length += len(packed_properties)

        self._pack_remaining_length(packet, remaining_length)
        self._pack_str16(packet, topic)
        if qos > 0:
            # For message id
            packet.extend(struct.pack("!H", mid))

        if self.protocol == Versions.v5:
            packet.extend(packed_properties)

        packet.extend(payload)
        return self._packet_queue(MessageTypes.PUBLISH, packet, mid, qos, info)

    def _send_pubrec(self, mid):
        self._easy_log(LogLevel.DEBUG, "Sending PUBREC (Mid: %d)", mid)
        return self._send_command_with_mid(MessageTypes.PUBREC, mid, False)

    def _send_pubrel(self, mid):
        self._easy_log(LogLevel.DEBUG, "Sending PUBREL (Mid: %d)", mid)
        return self._send_command_with_mid(MessageTypes.PUBREL | 2, mid, False)

    def _send_command_with_mid(self, command, mid, dup):
        # For PUBACK, PUBCOMP, PUBREC, and PUBREL
        if dup:
            command |= 0x8

        remaining_length = 2
        packet = struct.pack('!BBH', command, remaining_length, mid)
        return self._packet_queue(command, packet, mid, 1)

    def _send_simple_command(self, command):
        # For DISCONNECT, PINGREQ and PINGRESP
        remaining_length = 0
        packet = struct.pack('!BB', command, remaining_length)
        return self._packet_queue(command, packet, 0, 0)

    def _send_connect(self, keepalive):
        proto_ver = self.protocol
        # hard-coded UTF-8 encoded string
        protocol = b"MQTT" if proto_ver >= Versions.v311 else b"MQIsdp"

        remaining_length = 2 + len(protocol) + 1 + 1 + 2 + 2 + len(self._client_id)

        connect_flags = 0
        if self.protocol == Versions.v5:
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

        if self.protocol == Versions.v5:
            if self._connect_properties is None:
                packed_connect_properties = b'\x00'
            else:
                packed_connect_properties = self._connect_properties.pack()
            remaining_length += len(packed_connect_properties)
            if self._will:
                if self._will_properties is None:
                    packed_will_properties = b'\x00'
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
        if self.protocol == Versions.v5:
            packet += packed_connect_properties

        self._pack_str16(packet, self._client_id)
        if self._will:
            if self.protocol == Versions.v5:
                packet += packed_will_properties
            self._pack_str16(packet, self._will_topic)
            self._pack_str16(packet, self._will_payload)

        if self._username is not None:
            self._pack_str16(packet, self._username)
            if self._password is not None:
                self._pack_str16(packet, self._password)

        self._keepalive = keepalive
        if self.protocol == Versions.v5:
            self._easy_log(
                LogLevel.DEBUG,
                "Sending CONNECT (u%d, p%d, wr%d, wq%d, wf%d, c%d, k%d) client_id=%s properties=%s",
                (connect_flags & 0x80) >> 7,
                (connect_flags & 0x40) >> 6,
                (connect_flags & 0x20) >> 5,
                (connect_flags & 0x18) >> 3,
                (connect_flags & 0x4) >> 2,
                (connect_flags & 0x2) >> 1,
                keepalive,
                self._client_id,
                self._connect_properties
            )
        else:
            self._easy_log(
                LogLevel.DEBUG,
                "Sending CONNECT (u%d, p%d, wr%d, wq%d, wf%d, c%d, k%d) client_id=%s",
                (connect_flags & 0x80) >> 7,
                (connect_flags & 0x40) >> 6,
                (connect_flags & 0x20) >> 5,
                (connect_flags & 0x18) >> 3,
                (connect_flags & 0x4) >> 2,
                (connect_flags & 0x2) >> 1,
                keepalive,
                self._client_id
            )
        return self._packet_queue(command, packet, 0, 0)

    def _send_disconnect(self, reasoncode=None, properties=None):
        if self.protocol == Versions.v5:
            self._easy_log(LogLevel.DEBUG, "Sending DISCONNECT reasonCode=%s properties=%s", reasoncode, properties)
        else:
            self._easy_log(LogLevel.DEBUG, "Sending DISCONNECT")

        remaining_length = 0
        command = MessageTypes.DISCONNECT
        packet = bytearray()
        packet.append(command)

        if self.protocol == Versions.v5:
            if properties is not None or reasoncode is not None:
                if reasoncode is None:
                    reasoncode = ReasonCodes(MessageTypes.DISCONNECT >> 4, identifier=0)
                remaining_length += 1
                if properties is not None:
                    packed_props = properties.pack()
                    remaining_length += len(packed_props)

        self._pack_remaining_length(packet, remaining_length)
        if self.protocol == Versions.v5:
            if reasoncode is not None:
                packet += reasoncode.pack()
                if properties is not None:
                    packet += packed_props

        return self._packet_queue(command, packet, 0, 0)

    def _send_subscribe(self, dup, topics, properties=None):
        remaining_length = 2
        if self.protocol == Versions.v5:
            if properties is None:
                packed_subscribe_properties = b'\x00'
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

        if self.protocol == Versions.v5:
            packet += packed_subscribe_properties

        for t, q in topics:
            self._pack_str16(packet, t)
            if self.protocol == Versions.v5:
                packet += q.pack()
            else:
                packet.append(q)

        self._easy_log(LogLevel.DEBUG, "Sending SUBSCRIBE (d%d, m%d) %s", dup, local_mid, topics)
        return self._packet_queue(command, packet, local_mid, 1), local_mid

    def _send_unsubscribe(self, dup, topics, properties=None):
        remaining_length = 2
        if self.protocol == Versions.v5:
            if properties is None:
                packed_unsubscribe_properties = b'\x00'
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

        if self.protocol == Versions.v5:
            packet += packed_unsubscribe_properties

        for t in topics:
            self._pack_str16(packet, t)

        # topics_repr = ", ".join("'"+topic.decode('utf8')+"'" for topic in topics)
        if self.protocol == Versions.v5:
            self._easy_log(LogLevel.DEBUG, "Sending UNSUBSCRIBE (d%d, m%d) %s %s", dup, local_mid, properties, topics)
        else:
            self._easy_log(LogLevel.DEBUG, "Sending UNSUBSCRIBE (d%d, m%d) %s", dup, local_mid, topics)
        return self._packet_queue(command, packet, local_mid, 1), local_mid

    # MQTT packet handling
    def _handle_connack(self):
        if self.protocol == Versions.v5:
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
            flags, result = struct.unpack("!BB", self._in_packet.packet[1:3])

        if self.protocol == Versions.v311:
            if result == ConnAckCodes.REFUSED_PROTOCOL_VERSION:
                if not self._reconnect_on_failure:
                    return ErrorValues.PROTOCOL
                self._easy_log(LogLevel.DEBUG, "Received CONNACK (%s, %s), attempting downgrade to MQTT v3.1.", flags, result)
                # Downgrade to MQTT v3.1
                self.protocol = Versions.v31
                return self.reconnect()
            elif result == ConnAckCodes.REFUSED_IDENTIFIER_REJECTED and self._client_id == b'':
                if not self._reconnect_on_failure:
                    return ErrorValues.PROTOCOL
                self._easy_log(LogLevel.DEBUG, "Received CONNACK (%s, %s), attempting to use non-empty CID", flags, result)
                self._client_id = self.make_id()
                return self.reconnect()

        if result == 0:
            self._state = ConnectionStates.CONNECTED
            self._reconnect_delay = None

        if self.protocol == Versions.v5:
            self._easy_log(LogLevel.DEBUG, "Received CONNACK (%s, %s) properties=%s", flags, reason, properties)
        else:
            self._easy_log(LogLevel.DEBUG, "Received CONNACK (%s, %s)", flags, result)

        # it won't be the first successful connect any more
        self._mqttv5_first_connect = False
        if self.on_connect:
            flags_dict = {}
            flags_dict['session present'] = flags & 0x01
            try:
                if self.protocol == Versions.v5:
                    self.on_connect(self, self._userdata, flags_dict, reason, properties)
                else:
                    self.on_connect(self, self._userdata, flags_dict, result)
            except Exception as err:
                self._easy_log(LogLevel.ERR, 'Caught exception in on_connect: %s', err)
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
                    rc = self._send_publish(m.mid, m.topic.encode('utf-8'), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                    if rc != 0:
                        return rc
                elif m.qos == 1:
                    if m.state == MessageStates.PUBLISH:
                        self._inflight_messages += 1
                        m.state = MessageStates.WAIT_FOR_PUBACK
                        rc = self._send_publish(m.mid, m.topic.encode('utf-8'), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
                        if rc != 0:
                            return rc
                elif m.qos == 2:
                    if m.state == MessageStates.PUBLISH:
                        self._inflight_messages += 1
                        m.state = MessageStates.WAIT_FOR_PUBREC
                        rc = self._send_publish(m.mid, m.topic.encode('utf-8'), m.payload, m.qos, m.retain, m.dup, properties=m.properties)
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
        elif 0 < result < 6:
            return ErrorValues.CONN_REFUSED
        else:
            return ErrorValues.PROTOCOL

    def _handle_on_connect_fail(self):
        if self.on_connect_fail:
            try:
                self.on_connect_fail(self, self._userdata)
            except Exception as err:
                self._easy_log(LogLevel.ERR, 'Caught exception in on_connect_fail: %s', err)

    def _handle_disconnect(self):
        packet_type = MessageTypes.DISCONNECT >> 4
        reasonCode = properties = None
        if self._in_packet.remaining_length > 2:
            reasonCode = ReasonCodes(packet_type)
            reasonCode.unpack(self._in_packet.packet)
            if self._in_packet.remaining_length > 3:
                properties = Properties(packet_type)
                props, props_len = properties.unpack(self._in_packet.packet[1:])
        self._easy_log(LogLevel.DEBUG, "Received DISCONNECT %s %s", reasonCode, properties)
        self._loop_rc_handle(reasonCode, properties)
        return ErrorValues.SUCCESS

    def _handle_pingreq(self):
        if self._in_packet.remaining_length != 0:
            return ErrorValues.PROTOCOL

        self._easy_log(LogLevel.DEBUG, "Received PINGREQ")
        return self._send_pingresp()

    def _handle_pingresp(self):
        if self._in_packet.remaining_length != 0:
            return ErrorValues.PROTOCOL

        # No longer waiting for a PINGRESP.
        self._ping_t = 0
        self._easy_log(LogLevel.DEBUG, "Received PINGRESP")
        return ErrorValues.SUCCESS

    def _handle_publish(self):
        rc = 0
        header = self._in_packet.command
        message = MQTTMessage()
        message.dup = (header & 0x08) >> 3
        message.qos = (header & 0x06) >> 1
        message.retain = (header & 0x01)
        pack_format = "!H" + str(len(self._in_packet.packet) - 2) + 's'
        slen, packet = struct.unpack(pack_format, self._in_packet.packet)
        pack_format = '!' + str(slen) + 's' + str(len(packet) - slen) + 's'
        topic, packet = struct.unpack(pack_format, packet)

        if self.protocol != Versions.v5 and len(topic) == 0:
            return ErrorValues.PROTOCOL

        # Handle topics with invalid UTF-8
        # This replaces an invalid topic with a message and the hex
        # representation of the topic for logging. When the user attempts to
        # access message.topic in the callback, an exception will be raised.
        try:
            print_topic = topic.decode('utf-8')
        except UnicodeDecodeError:
            print_topic = "TOPIC WITH INVALID UTF-8: " + str(topic)

        message.topic = topic
        if message.qos > 0:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (message.mid, packet) = struct.unpack(pack_format, packet)

        if self.protocol == Versions.v5:
            message.properties = Properties(MessageTypes.PUBLISH >> 4)
            props, props_len = message.properties.unpack(packet)
            packet = packet[props_len:]

        message.payload = packet
        if self.protocol == Versions.v5:
            self._easy_log(LogLevel.DEBUG, "Received PUBLISH (d%d, q%d, r%d, m%d), '%s', properties=%s, ...  (%d bytes)", message.dup, message.qos, message.retain, message.mid, print_topic, message.properties, len(message.payload))
        else:
            self._easy_log(LogLevel.DEBUG, "Received PUBLISH (d%d, q%d, r%d, m%d), '%s', ...  (%d bytes)", message.dup, message.qos, message.retain, message.mid, print_topic, len(message.payload))

        message.timestamp = time_func()
        if message.qos == 0:
            self._handle_on_message(message)
            return ErrorValues.SUCCESS
        elif message.qos == 1:
            self._handle_on_message(message)
            return self._send_puback(message.mid)
        elif message.qos == 2:
            rc = self._send_pubrec(message.mid)
            message.state = MessageStates.WAIT_FOR_PUBREL
            self._in_messages[message.mid] = message
            return rc
        else:
            return ErrorValues.PROTOCOL

    def _handle_pubrec(self):
        if self.protocol == Versions.v5:
            if self._in_packet.remaining_length < 2:
                return ErrorValues.PROTOCOL
        elif self._in_packetremaining_length != 2:
            return ErrorValues.PROTOCOL

        mid, = struct.unpack("!H", self._in_packet.packet[:2])
        if self.protocol == Versions.v5:
            if self._in_packet.remaining_length > 2:
                reasonCode = ReasonCodes(MessageTypes.PUBREC >> 4)
                reasonCode.unpack(self._in_packet.packet[2:])
                if self._in_packet.remaining_length > 3:
                    properties = Properties(MessageTypes.PUBREC >> 4)
                    props, props_len = properties.unpack(self._in_packet.packet[3:])
        self._easy_log(LogLevel.DEBUG, "Received PUBREC (Mid: %d)", mid)

        if mid in self._out_messages:
            msg = self._out_messages[mid]
            msg.state = MessageStates.WAIT_FOR_PUBCOMP
            msg.timestamp = time_func()
            return self._send_pubrel(mid)
        return ErrorValues.SUCCESS

    def _handle_pubrel(self):
        if self.protocol == Versions.v5:
            if self._in_packet.remaining_length < 2:
                return ErrorValues.PROTOCOL
        elif self._in_packet.remaining_length != 2:
            return ErrorValues.PROTOCOL

        mid, = struct.unpack("!H", self._in_packet.packet)
        self._easy_log(LogLevel.DEBUG, "Received PUBREL (Mid: %d)", mid)

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

    def _handle_suback(self):
        self._easy_log(LogLevel.DEBUG, "Received SUBACK")
        pack_format = "!H" + str(len(self._in_packet.packet) - 2) + 's'
        mid, packet = struct.unpack(pack_format, self._in_packet.packet)
        if self.protocol == Versions.v5:
            properties = Properties(MessageTypes.SUBACK >> 4)
            props, props_len = properties.unpack(packet)
            reasoncodes = []
            for c in packet[props_len:]:
                reasoncodes.append(ReasonCodes(MessageTypes.SUBACK >> 4, identifier=c))
        else:
            pack_format = "!" + "B" * len(packet)
            granted_qos = struct.unpack(pack_format, packet)

        if self.on_subscribe:
            try:
                if self.protocol == Versions.v5:
                    self.on_subscribe(self, self._userdata, mid, reasoncodes, properties)
                else:
                    self.on_subscribe(self, self._userdata, mid, granted_qos)
            except Exception as err:
                self._easy_log(LogLevel.ERR, 'Caught exception in on_subscribe: %s', err)
                if not self.suppress_exceptions:
                    raise

        return ErrorValues.SUCCESS

    def _handle_unsuback(self):
        if self.protocol == Versions.v5:
            if self._in_packet.remaining_length < 4:
                return ErrorValues.PROTOCOL
        elif self._in_packet.remaining_length != 2:
            return ErrorValues.PROTOCOL

        mid, = struct.unpack("!H", self._in_packet.packet[:2])
        if self.protocol == Versions.v5:
            packet = self._in_packet.packet[2:]
            properties = Properties(MessageTypes.UNSUBACK >> 4)
            props, props_len = properties.unpack(packet)
            reasoncodes = []
            for c in packet[props_len:]:
                reasoncodes.append(ReasonCodes(MessageTypes.UNSUBACK >> 4, identifier=c))
            if len(reasoncodes) == 1:
                reasoncodes = reasoncodes[0]

        self._easy_log(LogLevel.DEBUG, "Received UNSUBACK (Mid: %d)", mid)
        if self.on_unsubscribe:
            try:
                if self.protocol == Versions.v5:
                    self.on_unsubscribe(self, self._userdata, mid, properties, reasoncodes)
                else:
                    self.on_unsubscribe(self, self._userdata, mid)
            except Exception as err:
                self._easy_log(LogLevel.ERR, 'Caught exception in on_unsubscribe: %s', err)
                if not self.suppress_exceptions:
                    raise

        return ErrorValues.SUCCESS

    # MQTT Helpers
    def _mid_generate(self):
        self._last_mid += 1
        if self._last_mid == 65536:
            self._last_mid = 1
        return self._last_mid

    def _packet_read(self, data: bytes):
        if self._in_packet.command == MessageTypes.NEW:
            if len(data) == 0:
                return ErrorValues.CONN_LOST
            command, = struct.unpack("!B", data[0:1])
            self._in_packet.command = MessageTypes.from_value(command)

        self._in_packet.packet = data
        # All data for this packet is read.
        self._in_packet.pos = 0
        rc = self._packet_handle()
        # Free data and reset values
        self._in_packet = InPacket(
            command=MessageTypes.NEW,
            have_remaining=0,
            remaining_count=[],
            remaining_mult=1,
            remaining_length=0,
            packet=bytearray(b""),
            to_process=0,
            pos=0
        )
        self._last_msg_in = time_func()
        return rc

    def _packet_write(self):
        while True:
            try:
                packet = self._out_packet.popleft()
            except IndexError:
                return ErrorValues.SUCCESS

            try:
                data = bytes(packet.packet[packet.pos:])
                self.transport.write(data)
                write_length = 0  # TODO: FIX ME!!
            except (AttributeError, ValueError) as err:
                self._out_packet.appendleft(packet)
                self._easy_log(LogLevel.ERR, 'failed to write data: %s', err)
                return ErrorValues.SUCCESS
            except BlockingIOError as err:
                self._out_packet.appendleft(packet)
                self._easy_log(LogLevel.ERR, 'failed to write data: %s', err)
                return ErrorValues.AGAIN
            except ConnectionError as err:
                self._out_packet.appendleft(packet)
                self._easy_log(LogLevel.ERR, 'failed to receive on socket: %s', err)
                return ErrorValues.CONN_LOST

            if write_length > 0:
                packet.to_process -= write_length
                packet.pos += write_length
                if packet.to_process == 0:
                    if (packet.command & 0xF0) == MessageTypes.PUBLISH and packet.qos == 0:
                        if self.on_publish:
                            try:
                                self.on_publish(self, self._userdata, packet.mid)
                            except Exception as err:
                                self._easy_log(LogLevel.ERR, 'Caught exception in on_publish: %s', err)
                                if not self.suppress_exceptions:
                                    raise

                        packet.info._set_as_published()

                    if (packet.command & 0xF0) == MessageTypes.DISCONNECT:
                        self._last_msg_out = time_func()
                        self._do_on_disconnect(ErrorValues.SUCCESS)
                        self.disconnect()
                        return ErrorValues.SUCCESS
                else:
                    # We haven't finished with this packet
                    self._out_packet.appendleft(packet)
            else:
                break

        self._last_msg_out = time_func()
        return ErrorValues.SUCCESS

    def _packet_queue(self, command: MessageTypes, packet: bytearray, mid: int, qos: int, info=None):
        mpkt = OutPacket(
            command=command,
            mid=mid,
            qos=qos,
            pos=0,
            to_process=len(packet),
            packet=packet,
            info=info
        )
        self._out_packet.append(mpkt)
        return self._packet_write()

    def _packet_handle(self):
        cmd = self._in_packet.command & 0xF0
        if cmd == MessageTypes.PINGREQ:
            return self._handle_pingreq()
        elif cmd == MessageTypes.PINGRESP:
            return self._handle_pingresp()
        elif cmd == MessageTypes.PUBACK:
            return self._handle_pubackcomp("PUBACK")
        elif cmd == MessageTypes.PUBCOMP:
            return self._handle_pubackcomp("PUBCOMP")
        elif cmd == MessageTypes.PUBLISH:
            return self._handle_publish()
        elif cmd == MessageTypes.PUBREC:
            return self._handle_pubrec()
        elif cmd == MessageTypes.PUBREL:
            return self._handle_pubrel()
        elif cmd == MessageTypes.CONNACK:
            return self._handle_connack()
        elif cmd == MessageTypes.SUBACK:
            return self._handle_suback()
        elif cmd == MessageTypes.UNSUBACK:
            return self._handle_unsuback()
        elif cmd == MessageTypes.DISCONNECT and self.protocol == Versions.v5:  # only allowed in MQTT 5.0
            return self._handle_disconnect()
        else:
            # If we don't recognise the command, return an error straight away.
            self._easy_log(LogLevel.ERR, "Error: Unrecognised command %s", cmd)
            return ErrorValues.PROTOCOL

    def _pack_remaining_length(self, packet, remaining_length):
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

    def _pack_str16(self, packet, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        packet.extend(struct.pack("!H", len(data)))
        packet.extend(data)

    # MQTT Callbacks
    @property
    def on_log(self):
        return self._on_log

    @on_log.setter
    def on_log(self, func: Callable):
        self._on_log = func

    def log_callback(self):
        def decorator(func):
            self.on_log = func
            return func
        return decorator

    @property
    def on_connect(self):
        return self._on_connect

    @on_connect.setter
    def on_connect(self, func: Callable):
        self._on_connect = func

    def connect_callback(self):
        def decorator(func):
            self.on_connect = func
            return func
        return decorator

    @property
    def on_connect_fail(self):
        return self._on_connect_fail

    @on_connect_fail.setter
    def on_connect_fail(self, func: Callable):
        self._on_connect_fail = func

    def connect_fail_callback(self):
        def decorator(func):
            self.on_connect_fail = func
            return func
        return decorator

    @property
    def on_subscribe(self):
        return self._on_subscribe

    @on_subscribe.setter
    def on_subscribe(self, func: Callable):
        self._on_subscribe = func

    def subscribe_callback(self):
        def decorator(func):
            self.on_subscribe = func
            return func
        return decorator

    @property
    def on_message(self):
        return self._on_message

    @on_message.setter
    def on_message(self, func: Callable):
        self._on_message = func

    def message_callback(self):
        def decorator(func):
            self.on_message = func
            return func
        return decorator

    @property
    def on_publish(self):
        return self._on_publish

    @on_publish.setter
    def on_publish(self, func: Callable):
        self._on_publish = func

    def publish_callback(self):
        def decorator(func):
            self.on_publish = func
            return func
        return decorator

    @property
    def on_unsubscribe(self):
        return self._on_unsubscribe

    @on_unsubscribe.setter
    def on_unsubscribe(self, func: Callable):
        self._on_unsubscribe = func

    def unsubscribe_callback(self):
        def decorator(func):
            self.on_unsubscribe = func
            return func
        return decorator

    @property
    def on_disconnect(self):
        return self._on_disconnect

    @on_disconnect.setter
    def on_disconnect(self, func: Callable):
        self._on_disconnect = func

    def disconnect_callback(self):
        def decorator(func):
            self.on_disconnect = func
            return func
        return decorator

    # General Helpers
    def _easy_log(self, level: LogLevel, fmt: str, *args: Any, **kwargs):
        logFun = level.logFunction()
        buf = fmt % args
        logFun(buf, **kwargs)

    def make_id(self) -> bytes:
        return f"{uuid.uuid4().int}"[:22].encode("utf-8")