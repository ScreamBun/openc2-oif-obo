import uuid

from dataclasses import fields
from typing import Any, Callable, NoReturn, Optional, Tuple, Union
from twisted.internet import reactor as Reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import inlineCallbacks
from twisted.internet.endpoints import TCP4ClientEndpoint, TCP6ClientEndpoint
from twisted.logger import Logger
from twisted.python.failure import Failure
from .message import MQTTMessageInfo
from .properties import Properties
from .protocol import MQTTProtocol, Subscriptions
from .service import MQTTService
from .subscribeoptions import SubscribeOptions
from .utils import Callbacks, ErrorValues, Versions

log = Logger(namespace="mqtt")


class MQTTFactory(ReconnectingClientFactory):
    protocol: MQTTProtocol
    _reactor: Reactor
    _addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]
    _broker: str
    _service: MQTTService
    _protocol: Versions
    _userdata: Any
    _client_id: bytes
    # Connection
    _keepalive = 60
    _connect_timeout = 5.0
    _clean_session: bool = None
    # Authentication & Security
    _username: bytes = None
    _password: bytes = None
    # Assorted
    _subs: Subscriptions = []
    _callbacks: Callbacks

    def __init__(self, reactor: Reactor, client_id: Union[bytes, str] = "", clean_session=None, userdata=None, version=Versions.v311, subs: Subscriptions = None, callbacks=Callbacks()) -> None:
        super().__init__()
        self._reactor = reactor
        self._protocol = version
        self._userdata = userdata
        self._keepalive = 60
        self._connect_timeout = 5.0
        self._subs = subs or []
        self._callbacks = callbacks

        if version == Versions.v5:
            if clean_session is not None:
                log.error("Clean session is not used for MQTT 5.0")
                raise ValueError("Clean session is not used for MQTT 5.0")
        else:
            if clean_session is None:
                clean_session = True
            if not clean_session and (client_id == "" or client_id is None):
                log.error("A client id must be provided if clean session is False.")
                raise ValueError("A client id must be provided if clean session is False.")
            self._clean_session = clean_session

        # [MQTT-3.1.3-4] Client ID must be UTF-8 encoded string.
        if client_id == "" or client_id is None:
            self._client_id = (f"{uuid.uuid4().int}"[:20] if version == Versions.v31 else "").encode("utf-8")
        else:
            self._client_id = client_id
        if isinstance(self._client_id, str):
            self._client_id = self._client_id.encode("utf-8")

    # Twisted Interface
    def buildProtocol(self, addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]) -> MQTTProtocol:
        self._addr = addr
        self._broker = f"{self._addr.host}:{self._addr.port}"
        log.info("Build protocol for address: {addr}", addr=addr)
        self.protocol = MQTTProtocol(self, addr)
        Reactor.callLater(0.5, self.connectToBroker)  # pylint: disable=no-member
        return self.protocol

    def clientConnectionLost(self, connector: Any, reason: Failure) -> NoReturn:  # pylint: disable=W0237
        log.warn("Lost connection. Reason {reason!r}:", reason=reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector: Any, reason: Failure) -> NoReturn:
        log.warn("Connection failed. Reason {reason!r}:", reason=reason)
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    @inlineCallbacks
    def connectToBroker(self):
        """
        Connect to MQTT broker
        """
        try:
            yield self.protocol.connect()
        except Exception as err:
            log.error("Connecting to broker at {broker} raised {excp!s}", broker=self._broker, excp=err)
            raise err
        else:
            log.info("Connected to broker at {broker}", broker=self._broker)

    # MQTT pass through
    def username_pw_set(self, username: str, password: Optional[str] = None) -> NoReturn:
        # [MQTT-3.1.3-11] User name must be UTF-8 encoded string
        self._username = None if username is None else username.encode("utf-8")
        self._password = password.encode("utf-8") if isinstance(password, str) else password

    def addCallback(self, fun: Callable, key: str = None) -> Callable:
        key = key or fun.__name__
        if key in [f.name for f in fields(self._callbacks)]:
            setattr(self._callbacks, key, fun)
            return fun
        raise KeyError(f"Unknown callback name of {key}")

    def message_callback_add(self, sub: str, callback: Callable) -> NoReturn:
        return self.protocol.message_callback_add(sub, callback)

    def message_callback_remove(self, sub) -> NoReturn:
        return self.protocol.message_callback_remove(sub)

    def publish(self, topic: str, payload: Union[bytes, bytearray, int, float, str, None] = None, qos: int = 0, retain: bool = False, properties: Properties = None) -> MQTTMessageInfo:
        return self.protocol.publish(topic=topic, payload=payload, qos=qos, retain=retain, properties=properties)

    def subscribe(self, topic: Union[str, Tuple[str, SubscribeOptions]], qos: int = 0, options: Optional[SubscribeOptions] = None, properties: Properties = None) -> Tuple[ErrorValues, int]:
        return self.protocol.subscribe(topic=topic, qos=qos, options=options, properties=properties)
