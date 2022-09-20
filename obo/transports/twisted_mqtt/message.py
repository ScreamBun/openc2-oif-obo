import time

from threading import Condition
from typing import NoReturn, Optional, Union
from .properties import Properties
from .utils import ErrorValues, MessageStates, error_string


class MQTTMessageInfo:
    """
    This is a class returned from Client.publish() and can be used to find
    out the mid of the message that was published, and to determine whether the
    message has been published, and/or wait until it is published.
    """
    mid: int
    rc: int = 0
    _published: bool = False
    _condition: Condition = Condition()
    _iterpos: int

    def __init__(self, mid: int):
        self.mid = mid
        self._iterpos = 0

    def __str__(self):
        return str((self.rc, self.mid))

    def __iter__(self):
        self._iterpos = 0
        return self

    def __next__(self):
        return self.next()

    def __getitem__(self, index: int):
        if index == 0:
            return self.rc
        if index == 1:
            return self.mid
        raise IndexError("index out of range")

    def next(self) -> int:
        if self._iterpos == 0:
            self._iterpos = 1
            return self.rc
        if self._iterpos == 1:
            self._iterpos = 2
            return self.mid
        raise StopIteration

    def _set_as_published(self) -> NoReturn:
        with self._condition:
            self._published = True
            self._condition.notify()

    def wait_for_publish(self, timeout: Optional[int]) -> NoReturn:
        """
        Block until the message associated with this object is published, or
        until the timeout occurs. If timeout is None, this will never time out.
        Set timeout to a positive number of seconds, e.g. 1.2, to enable the timeout.
        Raises ValueError if the message was not queued due to the outgoing queue being full.
        Raises RuntimeError if the message was not published for another reason.
        """
        if self.rc == ErrorValues.QUEUE_SIZE:
            raise ValueError("Message is not queued due to ERR_QUEUE_SIZE")
        if self.rc == ErrorValues.AGAIN:
            pass
        elif self.rc > 0:
            raise RuntimeError(f"Message publish failed: {error_string(self.rc)}")

        timeout_time = None if timeout is None else time.time() + timeout
        timeout_tenth = None if timeout is None else timeout / 10.

        def timed_out():
            return False if timeout is None else time.time() > timeout_time

        with self._condition:
            while not self._published and not timed_out():
                self._condition.wait(timeout_tenth)

    def is_published(self) -> bool:
        """
        Returns True if the message associated with this object has been published, else returns False.
        """
        if self.rc == ErrorValues.QUEUE_SIZE:
            raise ValueError("Message is not queued due to ERR_QUEUE_SIZE")
        if self.rc == ErrorValues.AGAIN:
            pass
        elif self.rc > 0:
            raise RuntimeError(f"Message publish failed: {error_string(self.rc)}")

        with self._condition:
            return self._published


class MQTTMessage:
    """
    This is a class that describes an incoming or outgoing message. It is
    passed to the on_message callback as the message parameter.

    Members:
        topic : String. topic that the message was published on.
        payload : Bytes/Byte array. the message payload.
        qos : Integer. The message Quality of Service 0, 1 or 2.
        retain : Boolean. If true, the message is a retained message and not fresh.
        mid : Integer. The message id.
        properties: Properties class. In MQTT v5.0, the properties associated with the message.
    """
    timestamp: int
    state: MessageStates = MessageStates.INVALID
    dup: bool = False
    mid: int
    payload: bytes = b""
    qos: int = 0
    retain: bool = False
    info: MQTTMessageInfo
    properties: Optional[Properties]
    _topic: bytes

    def __init__(self, mid: int = 0, topic: Union[bytes, str] = b""):
        self.timestamp = 0
        self.mid = mid
        self._topic = topic.encode("utf-8") if isinstance(topic, str) else topic
        self.info = MQTTMessageInfo(mid)

    def __eq__(self, other: "MQTTMessage") -> bool:
        """Override the default Equals behavior"""
        if isinstance(other, self.__class__):
            return self.mid == other.mid
        return False

    def __ne__(self, other: "MQTTMessage") -> bool:
        """Define a non-equality test"""
        return not self.__eq__(other)

    @property
    def topic(self) -> str:
        return self._topic.decode("utf-8")

    @topic.setter
    def topic(self, value: Union[bytes, str]) -> NoReturn:
        self._topic = value.encode("utf-8") if isinstance(value, str) else value
