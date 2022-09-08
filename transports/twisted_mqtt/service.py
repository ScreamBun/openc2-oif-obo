from typing import List, Literal, Tuple, Union
from twisted.application.internet import ClientService, backoffPolicy
from twisted.internet import task
from twisted.internet.endpoints import TCP4ClientEndpoint, TCP6ClientEndpoint
from twisted.internet.defer import inlineCallbacks
from twisted.logger import Logger
from .factory import MQTTFactory

log = Logger(namespace='mqtt')

SUBSCRIPTIONS = List[Tuple[str, Literal[0, 1, 2]]]


class MQTTService(ClientService):
    broker: str
    protocol: 'MQTTProtocol'
    subs: SUBSCRIPTIONS = []
    task: task.LoopingCall

    def __init__(self, endpoint: Union[TCP4ClientEndpoint, TCP6ClientEndpoint], factory: MQTTFactory, subscriptions: SUBSCRIPTIONS = None):
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
        self.protocol.onPublish = self.onPublish
        self.protocol.onDisconnection = self.onDisconnection
        self.protocol.setWindowSize(3)
        # self.task = task.LoopingCall(self.publish)
        # self.task.start(5.0, now=False)
        try:
            yield self.protocol.connect()
            for sub in self.subs:
                yield self.subscribe(*sub)
        except Exception as err:
            log.error("Connecting to {broker} raised {excp!s}", broker=self.broker, excp=err)
            raise err
        else:
            log.info("Connected to {broker}", broker=self.broker)

    def publish(self, topic: str, message: str, qos: int = 2):
        pub = self.protocol.publish(topic=topic, qos=qos, message=message)
        pub.addErrback(self._logFailure)
        return pub

    def subscribe(self, topic: str, qos: int = 2):
        sub = self.protocol.subscribe(topic, qos)
        sub.addCallbacks(self._logSuccess, self._logFailure)
        return sub

    def _logFailure(self, failure):
        print(f"error reported {failure.getErrorMessage()}")
        return failure

    def _logSuccess(self, success):
        print(f"success reported {success.getErrorMessage()}")
        return success

    def onPublish(self, topic: str, payload: Union[bytes, str], qos: int, dup, retain, msgId):
        """Callback Receiving messages from publisher"""
        print(f"msg={payload}")

    def onDisconnection(self, reason):
        """get notified of disconnections and get a deferred for a new protocol object (next retry)"""
        print(f"<Connection was lost !> <reason={reason}>")
        self.whenConnected().addCallback(self.connectToBroker)
