import json

from time import time
from typing import Any
from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
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


if __name__ == "__main__":
    config = get_config_data()

    if config.mqtt:
        print(config.mqtt)

    log = Logger()
    startLogging()
    setLogLevel(namespace='__main__', levelStr='debug')
    setLogLevel(namespace='mqtt', levelStr='debug')

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
        factory=factory,
    )

    print("Reactor Running")
    reactor.run()
