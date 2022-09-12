from typing import List, Literal, Tuple
from twisted.application.internet import ClientService, backoffPolicy
from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
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

    def __init__(self, endpoint: IStreamClientEndpoint, factory: 'MQTTFactory', subscriptions: SUBSCRIPTIONS = None):
        ClientService.__init__(self, endpoint, factory, retryPolicy=backoffPolicy())
        self.broker = f"mqtt://{endpoint._host}:{endpoint._port}"
        self.subs = subscriptions or []

    def startService(self):
        # invoke whenConnected() inherited method
        self.whenConnected().addCallback(self.connectToBroker)
        ClientService.startService(self)

    @inlineCallbacks
    def connectToBroker(self, protocol):
        """Connect to MQTT broker"""
        self.protocol = protocol
        self.protocol.setWindowSize(3)
        try:
            yield self.protocol.connect()
            for sub in self.subs:
                yield self.protocol.subscribe(*sub)
        except Exception as err:
            log.error("Connecting to {broker} raised {excp!s}", broker=self.broker, excp=err)
            raise err
        else:
            log.info("Connected to {broker}", broker=self.broker)
