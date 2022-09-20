import json

from time import time
from typing import Any
from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
from transports.twisted_http import setupWebServer
from transports.twisted_mqtt import MQTTFactory, MQTTMessage, MQTTProtocol, MQTTService, Versions
from utils import get_config_data, setLogLevel, startLogging

subs = [
    ("oc2/cmd", 1),
    ("oc2/cmd/all", 1)
]


def onMessage(proto: MQTTProtocol, userdata: Any, message: MQTTMessage):
    """Callback Receiving messages from message"""
    print(f"msg={message.payload}")
    print(f"userdata={userdata}")
    payload = {
        "headers": {
            "request_id": "bee2166a-caf3-45f6-975f-7c14f6c53356",
            "created": round(time() * 1000),
            "from": "Twisted1"
        },
        "body": {
            "openc2": {
                "response": {
                    "status": 500,
                    "status_text": "unknown actuator"
                }
            }
        }
    }
    proto.publish("oc2/rsp", payload=json.dumps(payload))


if __name__ == '__main__':
    # Load config
    config = get_config_data()

    # Setup logging
    log = Logger()
    startLogging()
    setLogLevel(namespace='__main__', levelStr='debug')
    setLogLevel(namespace='http', levelStr='debug')
    setLogLevel(namespace='mqtt', levelStr='debug')

    # Setup reactor
    print(config)
    # HTTP
    setupWebServer(reactor, config.https.port)

    # MQTT
    factory = MQTTFactory(
        reactor=reactor,
        client_id="Twisted-368207455685",
        subs=subs,
        version=Versions.v5
    )
    factory.addCallback(onMessage, "on_message")
    service = MQTTService(
        reactor=reactor,
        # host="localhost",
        host="mosquitto.olympus.mtn",
        port=1883,
        factory=factory,
    )

    print("Reactor Running")
    reactor.run()
