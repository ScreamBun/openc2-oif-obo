from twisted.application.internet import ClientService, backoffPolicy
from twisted.internet import task
from twisted.internet.defer import inlineCallbacks, DeferredList
from twisted.logger import Logger

from common import startLogging, setLogLevel


class MQTTPubService(ClientService):

    def __init__(self, endpoint, factory):
        ClientService.__init__(self, endpoint, factory, retryPolicy=backoffPolicy())

    def startService(self):

        log = Logger()
        startLogging()
        setLogLevel(namespace='mqtt', levelStr='debug')
        setLogLevel(namespace='__main__', levelStr='debug')

        print("starting MQTT Client Publisher Service")
        # invoke whenConnected() inherited method
        self.whenConnected().addCallback(self.connectToBroker)
        ClientService.startService(self)

    @inlineCallbacks
    def connectToBroker(self, protocol):
        '''
        Connect to MQTT broker
        '''

        # TODO: Get broker config

        self.protocol = protocol
        self.protocol.onDisconnection = self.onDisconnection
        # We are issuing 3 publish in a row
        # if order matters, then set window size to 1
        # Publish requests beyond window size are enqueued
        self.protocol.setWindowSize(3)
        self.task = task.LoopingCall(self.publish)
        self.task.start(5.0)
        try:
            yield self.protocol.connect("TwistedMQTT-pub", keepalive=60)
        except Exception as e:
            # log.error("Connecting to {broker} raised {excp!s}",
            #           broker=BROKER, excp=e)
            print("Error : " + e)
        else:
            # log.info("Connected to {broker}", broker=BROKER)
            print("Connect to broker")

    def onDisconnection(self, reason):
        '''
        get notfied of disconnections
        and get a deferred for a new protocol object (next retry)
        '''
        # log.debug(" >< Connection was lost ! ><, reason={r}", r=reason)
        print(" >< Connection was lost ! ><, reason= " + reason)
        self.whenConnected().addCallback(self.connectToBroker)

    def publish(self):

        def _logFailure(failure):
            # log.debug("reported {message}", message=failure.getErrorMessage())
            print("reported {message}", message=failure.getErrorMessage())
            return failure

        def _logAll(*args):
            # log.debug("all publishing complete args={args!r}", args=args)
            print("all publishing complete args={args!r}", args=args)

        # log.debug(" >< Starting one round of publishing >< ")
        print(" >< Starting one round of publishing >< ")
        d1 = self.protocol.publish(topic="foo/bar/baz1", qos=0, message="hello world 0")
        d1.addErrback(_logFailure)
        d2 = self.protocol.publish(topic="foo/bar/baz2", qos=1, message="hello world 1")
        d2.addErrback(_logFailure)
        d3 = self.protocol.publish(topic="foo/bar/baz3", qos=2, message="hello world 2")
        d3.addErrback(_logFailure)
        dlist = DeferredList([d1, d2, d3], consumeErrors=True)
        dlist.addCallback(_logAll)
        return dlist
