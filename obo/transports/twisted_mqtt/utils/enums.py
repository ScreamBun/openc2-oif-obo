from enum import Enum
from typing import Union


class EnumBase(Enum):
    @classmethod
    def from_name(cls, fmt: str) -> "EnumBase":
        name = fmt.upper()
        for k, v in dict(cls.__members__).items():
            if name == k.upper():
                return v
        raise ValueError(f"{name} is not a valid name")

    @classmethod
    def from_value(cls, fmt: Union[int, str]) -> "EnumBase":
        members = dict(cls.__members__)
        for k, v in members.items():
            if fmt == v:
                return cls.__getattr__(k)
        raise ValueError(f"{fmt} is not a valid value")


# Enum Classes
class ConnectionStates(int, EnumBase):
    NEW = 0
    CONNECTED = 1
    DISCONNECTING = 2
    CONNECT_ASYNC = 3


class ConnAckCodes(int, EnumBase):
    ACCEPTED = 0
    REFUSED_PROTOCOL_VERSION = 1
    REFUSED_IDENTIFIER_REJECTED = 2
    REFUSED_SERVER_UNAVAILABLE = 3
    REFUSED_BAD_USERNAME_PASSWORD = 4
    REFUSED_NOT_AUTHORIZED = 5


class ErrorValues(int, EnumBase):
    AGAIN = -1
    SUCCESS = 0
    NOMEM = 1
    PROTOCOL = 2
    INVAL = 3
    NO_CONN = 4
    CONN_REFUSED = 5
    NOT_FOUND = 6
    CONN_LOST = 7
    TLS = 8
    PAYLOAD_SIZE = 9
    NOT_SUPPORTED = 10
    AUTH = 11
    ACL_DENIED = 12
    UNKNOWN = 13
    ERRNO = 14
    QUEUE_SIZE = 15
    KEEPALIVE = 16


class LogLevels(int, EnumBase):
    INFO = 0x01
    NOTICE = 0x02
    WARNING = 0x04
    ERR = 0x08
    DEBUG = 0x10


class MessageStates(int, EnumBase):
    INVALID = 0
    PUBLISH = 1
    WAIT_FOR_PUBACK = 2
    WAIT_FOR_PUBREC = 3
    RESEND_PUBREL = 4
    WAIT_FOR_PUBREL = 5
    RESEND_PUBCOMP = 6
    WAIT_FOR_PUBCOMP = 7
    SEND_PUBREC = 8
    QUEUED = 9


class MessageTypes(int, EnumBase):
    CONNECT = 0x10
    CONNACK = 0x20
    PUBLISH = 0x30
    PUBACK = 0x40
    PUBREC = 0x50
    PUBREL = 0x60
    PUBCOMP = 0x70
    SUBSCRIBE = 0x80
    SUBACK = 0x90
    UNSUBSCRIBE = 0xA0
    UNSUBACK = 0xB0
    PINGREQ = 0xC0
    PINGRESP = 0xD0
    DISCONNECT = 0xE0
    AUTH = 0xF0


class Transports(str, EnumBase):
    TCP = "tcp"
    WebSockets = "websockets"


class Versions(int, EnumBase):
    v31 = 3
    v311 = 4
    v5 = 5
