from mqtt.client.factory import MQTTFactory
from twisted.application.service import Application
from twisted.internet import reactor
from twisted.internet.endpoints import clientFromString
from twisted.web.resource import Resource
from twisted.web.server import Site

from common.config import get_config_data
from services.http import HTTPService
from services.mqtt import MQTTSubService, MQTTPubService


if __name__ == '__main__':
    config_data = get_config_data()
    print("Config data: " + config_data["mqtt"]["broker"])

    broker = config_data["mqtt"]["broker"]

    sub_factory = MQTTFactory(profile=MQTTFactory.SUBSCRIBER)
    sub_endpoint = clientFromString(reactor, broker)
    sub_serv = MQTTSubService(sub_endpoint, sub_factory)
    sub_serv.startService()

    pub_factory = MQTTFactory(profile=MQTTFactory.PUBLISHER)
    pub_endpoint = clientFromString(reactor, broker)
    pub_serv = MQTTPubService(pub_endpoint, pub_factory)
    pub_serv.startService()

    root = Resource()
    root.putChild(b"", HTTPService())
    root.putChild(b"main", HTTPService())
    application = Application("OBO Web Service")
    factory = Site(root)

    # TODO: Read from config
    reactor.listenTCP(8880, factory)

    reactor.run()
