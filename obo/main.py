import json

from time import time
from typing import Any
from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
from transports.twisted_http import setupWebServer, setupWebServerSSL
from transports.twisted_mqtt import MQTTFactory, MQTTMessage, MQTTProtocol, MQTTService, Versions
from transports.twisted_websockets import setupWebSocket, setupWebSocketSSL
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
            "from": config.mqtt.client_id
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
    config = get_config_data()

    log = Logger()
    startLogging()
    loggers = ("__main__", "http", "mqtt", "websockets")
    level = "debug"
    for name in loggers:
        setLogLevel(namespace=name, levelStr=level)

    print(config)

    if config.https:
        log.info("Initializing HTTP")
        if config.https.key and config.https.cert:
            setupWebServerSSL(reactor, config.https.port, config.https.key, config.https.cert)
        else:
            setupWebServer(reactor, config.https.port)

    if config.websockets:
        log.info("Initializing Websockets")
        if config.websockets.key and config.websockets.cert:
            setupWebSocketSSL(reactor, config.websockets.host, config.websockets.port, config.websockets.key, config.websockets.cert)
        else:
            setupWebSocket(reactor, config.websockets.host, config.websockets.port)

    if config.mqtt:
        log.info("Initializing MQTT")
        factory = MQTTFactory(
            reactor=reactor,
            client_id=config.mqtt.client_id,
            subs=subs,
            version=Versions.v5
        )
        factory.username_pw_set(
            username=config.mqtt.username,
            password=config.mqtt.password
        )
        factory.addCallback(onMessage, "on_message")
        service = MQTTService(
            reactor=reactor,
            host=config.mqtt.host,
            port=config.mqtt.port,
            key=config.mqtt.cert,
            cert=config.mqtt.key,
            factory=factory,
        )

    log.info("Reactor Running")
    reactor.run()
