from typing import List, Literal, Tuple
from twisted.internet import reactor as Reactor
from twisted.application.internet import ClientService, backoffPolicy
from twisted.internet.endpoints import clientFromString
from .protocol import MQTTProtocol

SUBSCRIPTIONS = List[Tuple[str, Literal[0, 1, 2]]]


class MQTTService(ClientService):
    protocol: MQTTProtocol
    _reactor: Reactor
    _connectString: str

    def __init__(self, reactor: Reactor, host: str, port: int, factory: 'MQTTFactory', cert: str = None, key: str = None):
        self._reactor = reactor
        self._connectString = f"ssl:{host}:{port}:privateKey={key}:certKey={cert}" if cert and key else f"tcp:{host}:{port}"
        endpoint = clientFromString(reactor, self._connectString)
        ClientService.__init__(self, endpoint, factory, retryPolicy=backoffPolicy())
        self.startService()
