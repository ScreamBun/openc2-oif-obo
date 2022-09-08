import uuid

from typing import Any, List, Literal, Tuple, Union
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.endpoints import TCP4ClientEndpoint, TCP6ClientEndpoint
from twisted.logger import Logger
from .consts import MQTT_CLIENT
from .enums import Versions
from .protocol import MQTTProtocol, SUBSCRIPTIONS

log = Logger(namespace='mqtt')


class MQTTFactory(ReconnectingClientFactory):
    _protocol: Versions
    _userdata: Any
    _client_id: bytes
    _keepalive = 60
    _connect_timeout = 5.0
    _client_mode: Literal[0, 1] = MQTT_CLIENT
    _clean_session: bool = None
    _subs: SUBSCRIPTIONS = []

    def __init__(self, client_id: Union[bytes, str] = "", clean_session=None, subs: SUBSCRIPTIONS = None, userdata=None, protocol=Versions.v311):
        super().__init__()
        self._protocol = protocol
        self._userdata = userdata
        self._keepalive = 60
        self._connect_timeout = 5.0
        self._client_mode = MQTT_CLIENT
        self._subs = subs or []

        if protocol == Versions.v5:
            if clean_session is not None:
                log.error('Clean session is not used for MQTT 5.0')
                raise ValueError('Clean session is not used for MQTT 5.0')
        else:
            if clean_session is None:
                clean_session = True
            if not clean_session and (client_id == "" or client_id is None):
                log.error('A client id must be provided if clean session is False.')
                raise ValueError('A client id must be provided if clean session is False.')
            self._clean_session = clean_session

        # [MQTT-3.1.3-4] Client ID must be UTF-8 encoded string.
        if client_id == "" or client_id is None:
            self._client_id = (f"{uuid.uuid4().int}"[:20] if protocol == Versions.v31 else "").encode("utf-8")
        else:
            self._client_id = client_id
        if isinstance(self._client_id, str):
            self._client_id = self._client_id.encode('utf-8')

    # Twisted Interface
    def buildProtocol(self, addr: Union[TCP4ClientEndpoint, TCP6ClientEndpoint]):
        log.info('Build protocol for address: {addr}:', addr=addr)
        proto = MQTTProtocol(self, addr)
        return proto

    def clientConnectionLost(self, connector, reason):
        log.warn('Lost connection. Reason {reason!r}:', reason=reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        log.warn('Connection failed. Reason {reason!r}:', reason=reason)
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

