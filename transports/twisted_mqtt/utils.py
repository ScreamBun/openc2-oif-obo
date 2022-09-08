import socket
import string
import struct

from typing import Union
from .enums import ConnAckCodes, ErrorValues
from .exceptions import MalformedPacket
from .matcher import MQTTMatcher


def error_string(mqtt_errno: Union[int, ErrorValues]) -> str:
    """Return the error string associated with an mqtt error number."""
    if mqtt_errno == ErrorValues.SUCCESS:
        return "No error."
    elif mqtt_errno == ErrorValues.NOMEM:
        return "Out of memory."
    elif mqtt_errno == ErrorValues.PROTOCOL:
        return "A network protocol error occurred when communicating with the broker."
    elif mqtt_errno == ErrorValues.INVAL:
        return "Invalid function arguments provided."
    elif mqtt_errno == ErrorValues.NO_CONN:
        return "The client is not currently connected."
    elif mqtt_errno == ErrorValues.CONN_REFUSED:
        return "The connection was refused."
    elif mqtt_errno == ErrorValues.NOT_FOUND:
        return "Message not found (internal error)."
    elif mqtt_errno == ErrorValues.CONN_LOST:
        return "The connection was lost."
    elif mqtt_errno == ErrorValues.TLS:
        return "A TLS error occurred."
    elif mqtt_errno == ErrorValues.PAYLOAD_SIZE:
        return "Payload too large."
    elif mqtt_errno == ErrorValues.NOT_SUPPORTED:
        return "This feature is not supported."
    elif mqtt_errno == ErrorValues.AUTH:
        return "Authorisation failed."
    elif mqtt_errno == ErrorValues.ACL_DENIED:
        return "Access denied by ACL."
    elif mqtt_errno == ErrorValues.UNKNOWN:
        return "Unknown error."
    elif mqtt_errno == ErrorValues.ERRNO:
        return "Error defined by errno."
    elif mqtt_errno == ErrorValues.QUEUE_SIZE:
        return "Message queue full."
    elif mqtt_errno == ErrorValues.KEEPALIVE:
        return "Client or broker did not communicate in the keepalive interval."
    else:
        return "Unknown error."


def connack_string(connack_code: Union[int, ConnAckCodes]) -> str:
    """Return the string associated with a CONNACK result."""
    if connack_code == ConnAckCodes.ACCEPTED:
        return "Connection Accepted."
    elif connack_code == ConnAckCodes.REFUSED_PROTOCOL_VERSION:
        return "Connection Refused: unacceptable protocol version."
    elif connack_code == ConnAckCodes.REFUSED_IDENTIFIER_REJECTED:
        return "Connection Refused: identifier rejected."
    elif connack_code == ConnAckCodes.REFUSED_SERVER_UNAVAILABLE:
        return "Connection Refused: broker unavailable."
    elif connack_code == ConnAckCodes.REFUSED_BAD_USERNAME_PASSWORD:
        return "Connection Refused: bad user name or password."
    elif connack_code == ConnAckCodes.REFUSED_NOT_AUTHORIZED:
        return "Connection Refused: not authorised."
    else:
        return "Connection Refused: unknown reason."


def base62(num: int, base: str = string.digits + string.ascii_letters, padding: int = 1) -> str:
    """Convert a number to base-62 representation."""
    assert num >= 0
    digits = []
    while num:
        num, rest = divmod(num, 62)
        digits.append(base[rest])
    digits.extend(base[0] for _ in range(len(digits), padding))
    return ''.join(reversed(digits))


def topic_matches_sub(sub, topic):
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


def socketpair_compat():
    """TCP/IP socketpair including Windows support"""
    listensock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_IP)
    listensock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listensock.bind(("127.0.0.1", 0))
    listensock.listen(1)

    iface, port = listensock.getsockname()
    sock1 = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_IP)
    sock1.setblocking(0)
    try:
        sock1.connect(("127.0.0.1", port))
    except BlockingIOError:
        pass
    sock2, address = listensock.accept()
    sock2.setblocking(0)
    listensock.close()
    return sock1, sock2


def writeInt16(length):
    # serialize a 16-bit integer to network format
    return bytearray(struct.pack("!H", length))


def readInt16(buf):
    # deserialize a 16-bit integer from network format
    return struct.unpack("!H", buf[:2])[0]


def writeInt32(length):
    # serialize a 32-bit integer to network format
    return bytearray(struct.pack("!L", length))


def readInt32(buf):
    # deserialize a 32-bit integer from network format
    return struct.unpack("!L", buf[:4])[0]


def writeUTF(data):
    # data could be a string, or bytes. If string, encode into bytes with utf-8
    data = data if isinstance(data, bytes) else bytes(data, "utf-8")
    return writeInt16(len(data)) + data


def readUTF(buffer, maxlen):
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


def writeBytes(buffer):
    return writeInt16(len(buffer)) + buffer


def readBytes(buffer):
    length = readInt16(buffer)
    return buffer[2:2+length], length+2


def filter_wildcard_len_check(sub: bytes) -> ErrorValues:
    if len(sub) == 0 or len(sub) > 65535 or any(b'+' in p or b'#' in p for p in sub.split(b'/') if len(p) > 1) or b'#/' in sub:
        return ErrorValues.INVAL
    else:
        return ErrorValues.SUCCESS
