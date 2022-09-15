from typing import List, Literal, Tuple
from twisted.internet import reactor as Reactor
from twisted.application.internet import ClientService, backoffPolicy
from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
from twisted.internet.endpoints import clientFromString
from twisted.internet.interfaces import IStreamClientEndpoint
from twisted.logger import Logger
from .protocol import MQTTProtocol

log = Logger(namespace='mqtt')

SUBSCRIPTIONS = List[Tuple[str, Literal[0, 1, 2]]]


class MQTTService(ClientService):
    broker: str
    protocol: MQTTProtocol
    subs: SUBSCRIPTIONS = []
    task: task.LoopingCall
    _reactor: Reactor
    _endpoint: IStreamClientEndpoint

    def __init__(self, reactor: Reactor, host: str, port: int, factory: 'MQTTFactory', subscriptions: SUBSCRIPTIONS = None, cert: str = None, key: str = None, ):
        self._reactor = reactor
        self._connectString = f"ssl:{host}:{port}:privateKey={key}:certKey={cert}" if cert and key else f"tcp:{host}:{port}"
        endpoint = clientFromString(reactor, self._connectString)
        ClientService.__init__(self, endpoint, factory, retryPolicy=backoffPolicy())
        self.broker = f"mqtt://{endpoint._host}:{endpoint._port}"
        self.subs = subscriptions or []
        self.startService()

    def startService(self):
        # invoke whenConnected() inherited method
        self.whenConnected().addCallback(self.connectToBroker)
        ClientService.startService(self)

    @inlineCallbacks
    def connectToBroker(self, protocol):
        """Connect to MQTT broker"""
        self.protocol = protocol
        try:
            yield self.protocol.connect()
            for sub in self.subs:
                yield self.protocol.subscribe(*sub)
        except Exception as err:
            log.error("Connecting to {broker} raised {excp!s}", broker=self.broker, excp=err)
            raise err
        else:
            log.info("Connected to {broker}", broker=self.broker)
