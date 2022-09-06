from twisted.application.internet import ClientService, backoffPolicy
from twisted.internet.defer import inlineCallbacks, DeferredList


class MQTTSubService(ClientService):

    def __init__(self, endpoint, factory):
        ClientService.__init__(self, endpoint, factory, retryPolicy=backoffPolicy())

    def startService(self):
        print("starting MQTT Client Subscriber Service")
        self.whenConnected().addCallback(self.connectToBroker)
        ClientService.startService(self)

    @inlineCallbacks
    def connectToBroker(self, protocol):
        self.protocol = protocol
        self.protocol.onPublish = self.onPublish
        self.protocol.onDisconnection = self.onDisconnection
        self.protocol.setWindowSize(3)
        try:
            yield self.protocol.connect("TwistedMQTT-subs", keepalive=60)
            yield self.subscribe()
        except Exception as e:
            print("Connecting to {broker} raised {excp!s}")
        else:
            print("Connected and subscribed to {broker}")

    def subscribe(self):

        def _logFailure(failure):
            print("reported {message}")
            return failure

        def _logGrantedQoS(value):
            print("response {value!r}")
            return True

        def _logAll(*args):
            print("all subscriptions complete args={args!r}")

        d1 = self.protocol.subscribe("foo/bar/baz1", 2)
        d1.addCallbacks(_logGrantedQoS, _logFailure)

        d2 = self.protocol.subscribe("foo/bar/baz2", 2)
        d2.addCallbacks(_logGrantedQoS, _logFailure)

        d3 = self.protocol.subscribe("foo/bar/baz3", 2)
        d3.addCallbacks(_logGrantedQoS, _logFailure)

        dlist = DeferredList([d1, d2, d3], consumeErrors=True)
        dlist.addCallback(_logAll)
        return dlist

    def onPublish(self, topic, payload, qos, dup, retain, msgId):
        # Callback Receiving messages from publisher
        print("msg={payload}")

    def onDisconnection(self, reason):
        # get notified of disconnections
        # and get a deferred for a new protocol object (next retry)
        print(" >< Connection was lost ! ><, reason={r}")
        self.whenConnected().addCallback(self.connectToBroker)
