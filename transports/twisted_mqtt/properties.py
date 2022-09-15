import sys

from typing import Any, NoReturn, Optional, Tuple, Union
from .packettypes import PacketTypes
from .utils import (
    MQTTException,
    writeInt16, readInt16, writeInt32, readInt32, writeUTF, readUTF, writeBytes, readBytes
)


class VariableByteIntegers:  # Variable Byte Integer
    """
    MQTT variable byte integer helper class.  Used
    in several places in MQTT v5.0 properties.
    """
    @staticmethod
    def encode(x: int) -> bytes:
        """
        Convert an integer 0 <= x <= 268435455 into multi-byte format.
        Returns the buffer convered from the integer.
        """
        assert 0 <= x <= 268435455
        buffer = b""
        while 1:
            digit = x % 128
            x //= 128
            if x > 0:
                digit |= 0x80
            if sys.version_info[0] >= 3:
                buffer += bytes([digit])
            else:
                buffer += bytes(chr(digit))
            if x == 0:
                break
        return buffer

    @staticmethod
    def decode(buffer: bytes) -> Tuple[int, int]:
        """
        Get the value of a multi-byte integer from a buffer
        Return the value, and the number of bytes used.

        [MQTT-1.5.5-1] the encoded value MUST use the minimum number of bytes necessary to represent the value
        """
        multiplier = 1
        value = 0
        byte_count = 0
        while 1:
            byte_count += 1
            digit = buffer[0]
            buffer = buffer[1:]
            value += (digit & 127) * multiplier
            if digit & 128 == 0:
                break
            multiplier *= 128
        return value, byte_count


class Properties:
    """
    MQTT v5.0 properties class.
    See Properties.names for a list of accepted property names along with their numeric values.
    See Properties.properties for the data type of each property.
    Example of use:
        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.UserProperty = ("a", "2")
        publish_properties.UserProperty = ("c", "3")

    First the object is created with packet type as argument, no properties will be present at
    this point.  Then properties are added as attributes, the name of which is the string property
    name without the spaces.
    """
    packetType: Union[int, PacketTypes]
    types = ["Byte", "Two Byte Integer", "Four Byte Integer", "Variable Byte Integer", "Binary Data", "UTF-8 Encoded String", "UTF-8 String Pair"]
    names = {
        "Payload Format Indicator": 1,
        "Message Expiry Interval": 2,
        "Content Type": 3,
        "Response Topic": 8,
        "Correlation Data": 9,
        "Subscription Identifier": 11,
        "Session Expiry Interval": 17,
        "Assigned Client Identifier": 18,
        "Server Keep Alive": 19,
        "Authentication Method": 21,
        "Authentication Data": 22,
        "Request Problem Information": 23,
        "Will Delay Interval": 24,
        "Request Response Information": 25,
        "Response Information": 26,
        "Server Reference": 28,
        "Reason String": 31,
        "Receive Maximum": 33,
        "Topic Alias Maximum": 34,
        "Topic Alias": 35,
        "Maximum QoS": 36,
        "Retain Available": 37,
        "User Property": 38,
        "Maximum Packet Size": 39,
        "Wildcard Subscription Available": 40,
        "Subscription Identifier Available": 41,
        "Shared Subscription Available": 42
    }
    properties = {
        # id:  type, packets
        # payload format indicator
        1: (types.index("Byte"), [PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE]),
        2: (types.index("Four Byte Integer"), [PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE]),
        3: (types.index("UTF-8 Encoded String"), [PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE]),
        8: (types.index("UTF-8 Encoded String"), [PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE]),
        9: (types.index("Binary Data"), [PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE]),
        11: (types.index("Variable Byte Integer"), [PacketTypes.PUBLISH, PacketTypes.SUBSCRIBE]),
        17: (types.index("Four Byte Integer"), [PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.DISCONNECT]),
        18: (types.index("UTF-8 Encoded String"), [PacketTypes.CONNACK]),
        19: (types.index("Two Byte Integer"), [PacketTypes.CONNACK]),
        21: (types.index("UTF-8 Encoded String"), [PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.AUTH]),
        22: (types.index("Binary Data"), [PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.AUTH]),
        23: (types.index("Byte"), [PacketTypes.CONNECT]),
        24: (types.index("Four Byte Integer"), [PacketTypes.WILLMESSAGE]),
        25: (types.index("Byte"), [PacketTypes.CONNECT]),
        26: (types.index("UTF-8 Encoded String"), [PacketTypes.CONNACK]),
        28: (types.index("UTF-8 Encoded String"), [PacketTypes.CONNACK, PacketTypes.DISCONNECT]),
        31: (types.index("UTF-8 Encoded String"), [PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.PUBREL, PacketTypes.PUBCOMP, PacketTypes.SUBACK, PacketTypes.UNSUBACK, PacketTypes.DISCONNECT, PacketTypes.AUTH]),
        33: (types.index("Two Byte Integer"), [PacketTypes.CONNECT, PacketTypes.CONNACK]),
        34: (types.index("Two Byte Integer"), [PacketTypes.CONNECT, PacketTypes.CONNACK]),
        35: (types.index("Two Byte Integer"), [PacketTypes.PUBLISH]),
        36: (types.index("Byte"), [PacketTypes.CONNACK]),
        37: (types.index("Byte"), [PacketTypes.CONNACK]),
        38: (types.index("UTF-8 String Pair"), [PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.PUBLISH, PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.PUBREL, PacketTypes.PUBCOMP, PacketTypes.SUBSCRIBE, PacketTypes.SUBACK, PacketTypes.UNSUBSCRIBE, PacketTypes.UNSUBACK, PacketTypes.DISCONNECT, PacketTypes.AUTH, PacketTypes.WILLMESSAGE]),
        39: (types.index("Four Byte Integer"), [PacketTypes.CONNECT, PacketTypes.CONNACK]),
        40: (types.index("Byte"), [PacketTypes.CONNACK]),
        41: (types.index("Byte"), [PacketTypes.CONNACK]),
        42: (types.index("Byte"), [PacketTypes.CONNACK]),
    }

    def __init__(self, packet: Union[int, PacketTypes]):
        self.packetType = packet

    def __setattr__(self, name: str, value: Any):
        name = name.replace(" ", "")
        if name in ("packetType", "types", "names", "properties"):
            object.__setattr__(self, name, value)
        else:
            # the name could have spaces in, or not.  Remove spaces before assignment
            if name not in [aname.replace(" ", "") for aname in self.names]:
                raise MQTTException(f"Property name must be one of {list(self.names.keys())}")
            # check that this attribute applies to the packet type
            if self.packetType not in self.properties[self.getIdentFromName(name)][1]:
                raise MQTTException(f"Property {name} does not apply to packet type {PacketTypes.Names[self.packetType]}")

            # Check for forbidden values
            if not isinstance(value, list):
                if name in ["ReceiveMaximum", "TopicAlias"] and (value < 1 or value > 65535):
                    raise MQTTException(f"{name} property value must be in the range 1-65535")
                if name in ["TopicAliasMaximum"] and (value < 0 or value > 65535):
                    raise MQTTException(f"{name} property value must be in the range 0-65535")
                if name in ["MaximumPacketSize", "SubscriptionIdentifier"] and (value < 1 or value > 268435455):
                    raise MQTTException(f"{name} property value must be in the range 1-268435455")
                if name in ["RequestResponseInformation", "RequestProblemInformation", "PayloadFormatIndicator"] and (value not in (0, 1)):
                    raise MQTTException(f"{name} property value must be 0 or 1")

            if self.allowsMultiple(name):
                if not isinstance(value, list):
                    value = [value]
                if hasattr(self, name):
                    value = object.__getattribute__(self, name) + value
            object.__setattr__(self, name, value)

    def __str__(self):
        buffer = "["
        first = True
        for name in self.names:
            compressedName = name.replace(" ", "")
            if hasattr(self, compressedName):
                if not first:
                    buffer += ", "
                buffer += f"{compressedName} : {getattr(self, compressedName)}"
                first = False
        buffer += "]"
        return buffer

    def allowsMultiple(self, compressedName: str) -> bool:
        return self.getIdentFromName(compressedName) in [11, 38]

    def getIdentFromName(self, compressedName: str) -> int:
        # return the identifier corresponding to the property name
        result = -1
        for name, value in self.names.items():
            if compressedName == name.replace(" ", ""):
                result = value
                break
        return result

    def json(self) -> dict:
        data = {}
        for name in self.names:
            compressedName = name.replace(" ", "")
            if hasattr(self, compressedName):
                val = getattr(self, compressedName)
                if compressedName == "CorrelationData" and isinstance(val, bytes):
                    data[compressedName] = val.hex()
                else:
                    data[compressedName] = val
        return data

    def isEmpty(self) -> bool:
        for name in self.names:
            compressedName = name.replace(" ", "")
            if hasattr(self, compressedName):
                return False
        return True

    def clear(self) -> NoReturn:
        for name in self.names:
            compressedName = name.replace(" ", "")
            if hasattr(self, compressedName):
                delattr(self, compressedName)

    def writeProperty(self, identifier: int, type_: int, value: Union[bytes, int, str]) -> bytes:
        buffer = b""
        buffer += VariableByteIntegers.encode(identifier)  # identifier
        if type_ == self.types.index("Byte"):  # value
            buffer += bytes([value])
        elif type_ == self.types.index("Two Byte Integer"):
            buffer += writeInt16(value)
        elif type_ == self.types.index("Four Byte Integer"):
            buffer += writeInt32(value)
        elif type_ == self.types.index("Variable Byte Integer"):
            buffer += VariableByteIntegers.encode(value)
        elif type_ == self.types.index("Binary Data"):
            buffer += writeBytes(value)
        elif type_ == self.types.index("UTF-8 Encoded String"):
            buffer += writeUTF(value)
        elif type_ == self.types.index("UTF-8 String Pair"):
            buffer += writeUTF(value[0]) + writeUTF(value[1])
        return buffer

    def pack(self) -> bytes:
        # serialize properties into buffer for sending over network
        buffer = b""
        for name in self.names:
            compressedName = name.replace(" ", "")
            if hasattr(self, compressedName):
                identifier = self.getIdentFromName(compressedName)
                attr_type = self.properties[identifier][0]
                if self.allowsMultiple(compressedName):
                    for prop in getattr(self, compressedName):
                        buffer += self.writeProperty(identifier, attr_type, prop)
                else:
                    buffer += self.writeProperty(identifier, attr_type, getattr(self, compressedName))
        return VariableByteIntegers.encode(len(buffer)) + buffer

    def readProperty(self, buffer: bytes, type_: int, propslen: Optional[int]) -> Tuple[Union[bytes, int, str, Tuple[str, str]], int]:
        if type_ == self.types.index("Byte"):
            return buffer[0], 1
        if type_ == self.types.index("Two Byte Integer"):
            return readInt16(buffer), 2
        if type_ == self.types.index("Four Byte Integer"):
            return readInt32(buffer), 4
        if type_ == self.types.index("Variable Byte Integer"):
            return VariableByteIntegers.decode(buffer)
        if type_ == self.types.index("Binary Data"):
            return readBytes(buffer)
        if type_ == self.types.index("UTF-8 Encoded String"):
            return readUTF(buffer, propslen)
        if type_ == self.types.index("UTF-8 String Pair"):
            value, valuelen = readUTF(buffer, propslen)
            buffer = buffer[valuelen:]  # strip the bytes used by the value
            value1, valuelen1 = readUTF(buffer, propslen - valuelen)
            value = (value, value1)
            valuelen += valuelen1
            return value, valuelen
        return b"", 0

    def getNameFromIdent(self, identifier: int) -> Optional[str]:
        for name, value in self.names.items():
            if value == identifier:
                return name
        return None

    def unpack(self, buffer: bytes) -> Tuple["Properties", int]:
        self.clear()
        # deserialize properties into attributes from buffer received from network
        propslen, VBIlen = VariableByteIntegers.decode(buffer)
        buffer = buffer[VBIlen:]  # strip the bytes used by the VBI
        propslenleft = propslen
        while propslenleft > 0:  # properties length is 0 if there are none
            identifier, VBIlen2 = VariableByteIntegers.decode(buffer)  # property identifier
            buffer = buffer[VBIlen2:]  # strip the bytes used by the VBI
            propslenleft -= VBIlen2
            attr_type = self.properties[identifier][0]
            value, valuelen = self.readProperty(buffer, attr_type, propslenleft)
            buffer = buffer[valuelen:]  # strip the bytes used by the value
            propslenleft -= valuelen
            propname = self.getNameFromIdent(identifier)
            compressedName = propname.replace(" ", "")
            if not self.allowsMultiple(compressedName) and hasattr(self, compressedName):
                raise MQTTException(f"Property '{property}' must not exist more than once")
            setattr(self, propname, value)
        return self, propslen + VBIlen
