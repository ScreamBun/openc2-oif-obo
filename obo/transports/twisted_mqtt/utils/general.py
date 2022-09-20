import string
import struct

from typing import NoReturn, Tuple, Union
from .enums import ConnAckCodes, ErrorValues
from .exceptions import MalformedPacket
from ..matcher import MQTTMatcher


# Client/Factory Utils
def base62(num: int, base: str = string.digits + string.ascii_letters, padding: int = 1) -> str:
    """
    Convert a number to base-62 representation.
    """
    assert num >= 0
    digits = []
    while num:
        num, rest = divmod(num, 62)
        digits.append(base[rest])
    digits.extend(base[0] for _ in range(len(digits), padding))
    return "".join(reversed(digits))


def connack_string(connack_code: Union[int, ConnAckCodes]) -> str:
    """
    Return the string associated with a CONNACK result.
    """
    return {
        ConnAckCodes.ACCEPTED: "Connection Accepted.",
        ConnAckCodes.REFUSED_PROTOCOL_VERSION: "Connection Refused: unacceptable protocol version.",
        ConnAckCodes.REFUSED_IDENTIFIER_REJECTED: "Connection Refused: identifier rejected.",
        ConnAckCodes.REFUSED_SERVER_UNAVAILABLE: "Connection Refused: broker unavailable.",
        ConnAckCodes.REFUSED_BAD_USERNAME_PASSWORD: "Connection Refused: bad user name or password.",
        ConnAckCodes.REFUSED_NOT_AUTHORIZED: "Connection Refused: not authorised."
    }.get(connack_code, "Connection Refused: unknown reason.")


def error_string(mqtt_errno: Union[int, ErrorValues]) -> str:
    """
    Return the error string associated with a mqtt error number.
    """
    return {
        ErrorValues.SUCCESS: "No error.",
        ErrorValues.NOMEM: "Out of memory.",
        ErrorValues.PROTOCOL: "A network protocol error occurred when communicating with the broker.",
        ErrorValues.INVAL: "Invalid function arguments provided.",
        ErrorValues.NO_CONN: "The client is not currently connected.",
        ErrorValues.CONN_REFUSED: "The connection was refused.",
        ErrorValues.NOT_FOUND: "Message not found (internal error).",
        ErrorValues.CONN_LOST: "The connection was lost.",
        ErrorValues.TLS: "A TLS error occurred.",
        ErrorValues.PAYLOAD_SIZE: "Payload too large.",
        ErrorValues.NOT_SUPPORTED: "This feature is not supported.",
        ErrorValues.AUTH: "Authorisation failed.",
        ErrorValues.ACL_DENIED: "Access denied by ACL.",
        ErrorValues.UNKNOWN: "Unknown error.",
        ErrorValues.ERRNO: "Error defined by errno.",
        ErrorValues.QUEUE_SIZE: "Message queue full.",
        ErrorValues.KEEPALIVE: "Client or broker did not communicate in the keepalive interval."
    }.get(mqtt_errno, "Unknown error.")


def topic_matches_sub(sub: str, topic: Union[bytes, str]) -> bool:
    """
    Check whether a topic matches a subscription.
    For example:
        foo/bar would match the subscription foo/# or +/bar
        non/matching would not match the subscription non/+/+
    """
    matcher = MQTTMatcher()
    matcher[sub] = True
    try:
        next(matcher.iter_match(topic))
        return True
    except StopIteration:
        return False


def filter_wildcard_len_check(sub: bytes) -> ErrorValues:
    if len(sub) == 0 or len(sub) > 65535 or any(b"+" in p or b"#" in p for p in sub.split(b"/") if len(p) > 1) or b"#/" in sub:
        return ErrorValues.INVAL
    return ErrorValues.SUCCESS


def topic_wildcard_len_check(topic: bytes) -> ErrorValues:
    # Search for + or # in a topic. Return ErrorValues.INVAL if found.
    # Also returns ErrorValues.INVAL if the topic string is too long.
    # Returns ErrorValues.SUCCESS if everything is fine.
    if b"+" in topic or b"#" in topic or len(topic) > 65535:
        return ErrorValues.INVAL
    return ErrorValues.SUCCESS


# PDU Utils
def pack_remaining_length(packet: bytearray, remaining_length: int) -> bytearray:
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


def pack_str16(packet: bytearray, data: Union[bytes, str]) -> NoReturn:
    if isinstance(data, str):
        data = data.encode("utf-8")
    packet.extend(struct.pack("!H", len(data)))
    packet.extend(data)


# Properties Utils
def readBytes(buffer: bytes) -> Tuple[bytes, int]:
    length = readInt16(buffer)
    return buffer[2:2+length], length+2


def readInt16(buf: bytes) -> int:
    # deserialize a 16-bit integer from network format
    return struct.unpack("!H", buf[:2])[0]


def readInt32(buf: bytes) -> int:
    # deserialize a 32-bit integer from network format
    return struct.unpack("!L", buf[:4])[0]


def readUTF(buffer: bytes, maxlen: int) -> Tuple[str, int]:
    if maxlen >= 2:
        length = readInt16(buffer)
    else:
        raise MalformedPacket("Not enough data to read string length")
    maxlen -= 2
    if length > maxlen:
        raise MalformedPacket("Length delimited string too long")
    buf = buffer[2:2+length].decode("utf-8")
    # look for chars which are invalid for MQTT
    for c in buf: # look for D800-DFFF in the UTF string
        ord_c = ord(c)
        if 0xD800 <= ord_c <= 0xDFFF:
            raise MalformedPacket("[MQTT-1.5.4-1] D800-DFFF found in UTF-8 data")
        if ord_c == 0x00:  # look for null in the UTF string
            raise MalformedPacket("[MQTT-1.5.4-2] Null found in UTF-8 data")
        if ord_c == 0xFEFF:
            raise MalformedPacket("[MQTT-1.5.4-3] U+FEFF in UTF-8 data")
    return buf, length+2


def writeBytes(buffer: bytes) -> bytes:
    return writeInt16(len(buffer)) + buffer


def writeInt16(val: int) -> bytes:
    # serialize a 16-bit integer to network format
    return struct.pack("!H", val)


def writeInt32(val: int) -> bytes:
    # serialize a 32-bit integer to network format
    return struct.pack("!L", val)


def writeUTF(data: Union[bytes, str]) -> bytes:
    # data could be a string, or bytes. If string, encode into bytes with utf-8
    data = data if isinstance(data, bytes) else bytes(data, "utf-8")
    return writeInt16(len(data)) + data

