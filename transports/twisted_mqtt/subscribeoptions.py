from typing import Any, Literal
from .utils import MQTTException


class SubscribeOptions:
    """
    The MQTT v5.0 subscribe options class.
    The options are:
        qos:                As in MQTT v3.1.1.
        noLocal:            True or False. If set to True, the subscriber will not receive its own publications.
        retainAsPublished:  True or False. If set to True, the retain flag on received publications will be as set
                            by the publisher.
        retainHandling:     RETAIN_SEND_ON_SUBSCRIBE, RETAIN_SEND_IF_NEW_SUB or RETAIN_DO_NOT_SEND
                            Controls when the broker should send retained messages:
                                - RETAIN_SEND_ON_SUBSCRIBE: on any successful subscribe request
                                - RETAIN_SEND_IF_NEW_SUB: only if the subscribe request is new
                                - RETAIN_DO_NOT_SEND: never send retained messages
    """
    # retain handling options
    RETAIN_SEND_ON_SUBSCRIBE = 0
    RETAIN_SEND_IF_NEW_SUB = 1
    RETAIN_DO_NOT_SEND = 2
    names = ["QoS", "noLocal", "retainAsPublished", "retainHandling"]
    QoS: int  # bits 0,1
    noLocal: bool  # bit 2
    retainAsPublished: bool  # bit 3
    retainHandling: Literal[0, 1, 2]  # bits 4 and 5: 0, 1 or 2

    def __init__(self, qos: int = 0, noLocal: bool = False, retainAsPublished: bool = False, retainHandling: Literal[0, 1, 2] = RETAIN_SEND_ON_SUBSCRIBE):
        """
        qos:                0, 1 or 2.  0 is the default.
        noLocal:            True or False. False is the default and corresponds to MQTT v3.1.1 behavior.
        retainAsPublished:  True or False. False is the default and corresponds to MQTT v3.1.1 behavior.
        retainHandling:     RETAIN_SEND_ON_SUBSCRIBE, RETAIN_SEND_IF_NEW_SUB or RETAIN_DO_NOT_SEND
                            RETAIN_SEND_ON_SUBSCRIBE is the default and corresponds to MQTT v3.1.1 behavior.
        """
        self.QoS = qos  # bits 0,1
        self.noLocal = noLocal  # bit 2
        self.retainAsPublished = retainAsPublished  # bit 3
        self.retainHandling = retainHandling  # bits 4 and 5: 0, 1 or 2
        assert self.QoS in [0, 1, 2]
        assert self.retainHandling in [0, 1, 2], "Retain handling should be 0, 1 or 2"

    def __setattr__(self, name: str, value: Any):
        if name not in self.names:
            raise MQTTException(f"{name} Attribute name must be one of {self.names}")
        object.__setattr__(self, name, value)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"{{QoS={self.QoS}, noLocal={self.noLocal}, retainAsPublished={self.retainAsPublished}, retainHandling={self.retainHandling}}}"

    def pack(self) -> bytes:
        assert self.QoS in [0, 1, 2]
        assert self.retainHandling in [0, 1, 2], "Retain handling should be 0, 1 or 2"
        noLocal = 1 if self.noLocal else 0
        retainAsPublished = 1 if self.retainAsPublished else 0
        data = [(self.retainHandling << 4) | (retainAsPublished << 3) | (noLocal << 2) | self.QoS]
        return bytes(data)

    def unpack(self, buffer) -> int:
        b0 = buffer[0]
        self.retainHandling = ((b0 >> 4) & 0x03)
        self.retainAsPublished = ((b0 >> 3) & 0x01) == 1
        self.noLocal = ((b0 >> 2) & 0x01) == 1
        self.QoS = (b0 & 0x03)
        assert self.retainHandling in [0, 1, 2], f"Retain handling should be 0, 1 or 2, not {self.retainHandling}"
        assert self.QoS in [0, 1, 2], f"QoS should be 0, 1 or 2, not {self.QoS}"
        return 1

    def json(self) -> dict:
        return {
            "QoS": self.QoS,
            "noLocal": self.noLocal,
            "retainAsPublished": self.retainAsPublished,
            "retainHandling": self.retainHandling,
        }
