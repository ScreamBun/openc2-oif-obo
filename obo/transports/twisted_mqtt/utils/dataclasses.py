from dataclasses import dataclass, field
from typing import Any, Callable, List, NoReturn, Optional


@dataclass
class Packet:
    command: int = 0
    packet: bytearray = field(default_factory=bytearray)
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
    info: "MQTTMessageInfo" = None


@dataclass
class Callbacks:
    on_log: Callable[["MQTTProtocol", Any, "LogLevels", str], NoReturn] = None
    on_connect: Callable[["MQTTProtocol", Any, dict, int, Optional["Properties"]], NoReturn] = None
    on_connect_fail: Callable[["MQTTProtocol", Any], NoReturn] = None
    on_subscribe: Callable[["MQTTProtocol", Any, int, int], NoReturn] = None
    on_message: Callable[["MQTTProtocol", Any, "MQTTMessage"], NoReturn] = None
    on_publish: Callable[["MQTTProtocol", Any, int], NoReturn] = None
    on_unsubscribe: Callable[["MQTTProtocol", Any, int, Optional["Properties"], Optional[List["ReasonCodes"]]], NoReturn] = None
    on_disconnect: Callable[["MQTTProtocol", Any, int, Optional["Properties"]], NoReturn] = None
