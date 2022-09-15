from typing import NoReturn, Union
from .packettypes import PacketTypes


class ReasonCodes:
    """
    MQTT version 5.0 reason codes class.
    See ReasonCodes.names for a list of possible numeric values along with their
    names and the packets to which they apply.
    """
    packetType: Union[int, PacketTypes]
    names = {
        0: {
            "Success": [PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.PUBREL, PacketTypes.PUBCOMP, PacketTypes.UNSUBACK, PacketTypes.AUTH],
            "Normal disconnection": [PacketTypes.DISCONNECT],
            "Granted QoS 0": [PacketTypes.SUBACK]
        },
        1: {"Granted QoS 1": [PacketTypes.SUBACK]},
        2: {"Granted QoS 2": [PacketTypes.SUBACK]},
        4: {"Disconnect with will message": [PacketTypes.DISCONNECT]},
        16: {"No matching subscribers": [PacketTypes.PUBACK, PacketTypes.PUBREC]},
        17: {"No subscription found": [PacketTypes.UNSUBACK]},
        24: {"Continue authentication": [PacketTypes.AUTH]},
        25: {"Re-authenticate": [PacketTypes.AUTH]},
        128: {"Unspecified error": [PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.SUBACK, PacketTypes.UNSUBACK, PacketTypes.DISCONNECT]},
        129: {"Malformed packet": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        130: {"Protocol error": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        131: {"Implementation specific error": [PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.SUBACK, PacketTypes.UNSUBACK, PacketTypes.DISCONNECT]},
        132: {"Unsupported protocol version": [PacketTypes.CONNACK]},
        133: {"Client identifier not valid": [PacketTypes.CONNACK]},
        134: {"Bad user name or password": [PacketTypes.CONNACK]},
        135: {"Not authorized": [PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.SUBACK, PacketTypes.UNSUBACK, PacketTypes.DISCONNECT]},
        136: {"Server unavailable": [PacketTypes.CONNACK]},
        137: {"Server busy": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        138: {"Banned": [PacketTypes.CONNACK]},
        139: {"Server shutting down": [PacketTypes.DISCONNECT]},
        140: {"Bad authentication method": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        141: {"Keep alive timeout": [PacketTypes.DISCONNECT]},
        142: {"Session taken over": [PacketTypes.DISCONNECT]},
        143: {"Topic filter invalid": [PacketTypes.SUBACK, PacketTypes.UNSUBACK, PacketTypes.DISCONNECT]},
        144: {"Topic name invalid": [PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.DISCONNECT]},
        145: {"Packet identifier in use": [PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.SUBACK, PacketTypes.UNSUBACK]},
        146: {"Packet identifier not found": [PacketTypes.PUBREL, PacketTypes.PUBCOMP]},
        147: {"Receive maximum exceeded": [PacketTypes.DISCONNECT]},
        148: {"Topic alias invalid": [PacketTypes.DISCONNECT]},
        149: {"Packet too large": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        150: {"Message rate too high": [PacketTypes.DISCONNECT]},
        151: {"Quota exceeded": [PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.SUBACK, PacketTypes.DISCONNECT]},
        152: {"Administrative action": [PacketTypes.DISCONNECT]},
        153: {"Payload format invalid": [PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.DISCONNECT]},
        154: {"Retain not supported": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        155: {"QoS not supported": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        156: {"Use another server": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        157: {"Server moved": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        158: {"Shared subscription not supported": [PacketTypes.SUBACK, PacketTypes.DISCONNECT]},
        159: {"Connection rate exceeded": [PacketTypes.CONNACK, PacketTypes.DISCONNECT]},
        160: {"Maximum connect time": [PacketTypes.DISCONNECT]},
        161: {"Subscription identifiers not supported": [PacketTypes.SUBACK, PacketTypes.DISCONNECT]},
        162: {"Wildcard subscription not supported": [PacketTypes.SUBACK, PacketTypes.DISCONNECT]}
    }

    def __init__(self, packet: Union[int, PacketTypes], name: str = "Success", identifier: int = -1):
        """
        packetType: the type of the packet, such as PacketTypes.CONNECT that this reason code will be used with.
            Some reason codes have different names for the same identifier when used a different packet type.

        name: the String name of the reason code to be created.  Ignored if the identifier is set.

        identifier: an integer value of the reason code to be created.
        """
        self.packetType = packet
        if identifier == -1:
            if packet == PacketTypes.DISCONNECT and name == "Success":
                name = "Normal disconnection"
            self.set(name)
        else:
            self.value = identifier
            self.getName()  # check it's good

    def __str__(self):
        return self.getName()

    def __eq__(self, other: "ReasonCodes"):
        if isinstance(other, int):
            return self.value == other
        if isinstance(other, str):
            return self.value == str(self)
        if isinstance(other, ReasonCodes):
            return self.value == other.value
        return False

    def __getName__(self, packet: Union[int, PacketTypes], identifier: int) -> str:
        """
        Get the reason code string name for a specific identifier.
        The name can vary by packet type for the same identifier, which is why the packet type is also required.
        Used when displaying the reason code.
        """
        if names := self.names.get(identifier):
            namelist = [name for name, value in names.items() if packet in value]
            if len(namelist) != 1:
                raise ValueError(f"{packet} is not a valid reason code")
            return namelist[0]
        raise ValueError(f"{identifier} is not a valid reason code")

    def getId(self, name: str) -> int:
        """
        Get the numeric id corresponding to a reason code name.
        Used when setting the reason code for a packetType check that only valid codes for the packet are set.
        """
        identifier = None
        for code, value in self.names.items():
            if name in value:
                if self.packetType in value[name]:
                    identifier = code
                break
        assert identifier is not None, name
        return identifier

    def set(self, name: str) -> NoReturn:
        self.value = self.getId(name)

    def unpack(self, buffer: bytes) -> int:
        c = buffer[0]
        name = self.__getName__(self.packetType, c)
        self.value = self.getId(name)
        return 1

    def getName(self) -> str:
        """
        Returns the reason code name corresponding to the numeric value which is set.
        """
        return self.__getName__(self.packetType, self.value)

    def json(self) -> str:
        return self.getName()

    def pack(self) -> bytearray:
        return bytearray([self.value])
