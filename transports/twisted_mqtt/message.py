import threading
import time

from .enums import ErrorValues, MessageStates
from .utils import error_string


class MQTTMessageInfo:
    """
    This is a class returned from Client.publish() and can be used to find
    out the mid of the message that was published, and to determine whether the
    message has been published, and/or wait until it is published.
    """

    __slots__ = 'mid', '_published', '_condition', 'rc', '_iterpos'

    def __init__(self, mid):
        self.mid = mid
        self._published = False
        self._condition = threading.Condition()
        self.rc = 0
        self._iterpos = 0

    def __str__(self):
        return str((self.rc, self.mid))

    def __iter__(self):
        self._iterpos = 0
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self._iterpos == 0:
            self._iterpos = 1
            return self.rc
        elif self._iterpos == 1:
            self._iterpos = 2
            return self.mid
        else:
            raise StopIteration

    def __getitem__(self, index):
        if index == 0:
            return self.rc
        elif index == 1:
            return self.mid
        else:
            raise IndexError("index out of range")

    def _set_as_published(self):
        with self._condition:
            self._published = True
            self._condition.notify()

    def wait_for_publish(self, timeout=None):
        """Block until the message associated with this object is published, or
        until the timeout occurs. If timeout is None, this will never time out.
        Set timeout to a positive number of seconds, e.g. 1.2, to enable the
        timeout.

        Raises ValueError if the message was not queued due to the outgoing
        queue being full.

        Raises RuntimeError if the message was not published for another
        reason.
        """
        if self.rc == ErrorValues.QUEUE_SIZE:
            raise ValueError('Message is not queued due to ERR_QUEUE_SIZE')
        elif self.rc == ErrorValues.AGAIN:
            pass
        elif self.rc > 0:
            raise RuntimeError('Message publish failed: %s' % (error_string(self.rc)))

        timeout_time = None if timeout is None else time.time() + timeout
        timeout_tenth = None if timeout is None else timeout / 10.
        def timed_out():
            return False if timeout is None else time.time() > timeout_time

        with self._condition:
            while not self._published and not timed_out():
                self._condition.wait(timeout_tenth)

    def is_published(self):
        """Returns True if the message associated with this object has been
        published, else returns False."""
        if self.rc == ErrorValues.QUEUE_SIZE:
            raise ValueError('Message is not queued due to ERR_QUEUE_SIZE')
        elif self.rc == ErrorValues.AGAIN:
            pass
        elif self.rc > 0:
            raise RuntimeError('Message publish failed: %s' % (error_string(self.rc)))

        with self._condition:
            return self._published


class MQTTMessage:
    """ This is a class that describes an incoming or outgoing message. It is
    passed to the on_message callback as the message parameter.

    Members:

    topic : String. topic that the message was published on.
    payload : Bytes/Byte array. the message payload.
    qos : Integer. The message Quality of Service 0, 1 or 2.
    retain : Boolean. If true, the message is a retained message and not fresh.
    mid : Integer. The message id.
    properties: Properties class. In MQTT v5.0, the properties associated with the message.
    """

    __slots__ = 'timestamp', 'state', 'dup', 'mid', '_topic', 'payload', 'qos', 'retain', 'info', 'properties'

    def __init__(self, mid=0, topic=b""):
        self.timestamp = 0
        self.state = MessageStates.INVALID
        self.dup = False
        self.mid = mid
        self._topic = topic
        self.payload = b""
        self.qos = 0
        self.retain = False
        self.info = MQTTMessageInfo(mid)

    def __eq__(self, other):
        """Override the default Equals behavior"""
        if isinstance(other, self.__class__):
            return self.mid == other.mid
        return False

    def __ne__(self, other):
        """Define a non-equality test"""
        return not self.__eq__(other)

    @property
    def topic(self):
        return self._topic.decode('utf-8')

    @topic.setter
    def topic(self, value):
        self._topic = value
