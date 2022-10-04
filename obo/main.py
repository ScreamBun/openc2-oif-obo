import json

from time import time
from typing import Any
from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
from transports.twisted_https import setupWebServer, setupWebServerSSL
from transports.twisted_mqtt import MQTTFactory, MQTTMessage, MQTTProtocol, MQTTService, Versions
from transports.twisted_websockets import setupWebSocket, setupWebSocketSSL
from utils import get_config_data, setLogLevel, startLogging
#actuator imports
from actuators import routing, actuator
#oc2 types
from openc2_types import OpenC2CmdFields, OpenC2RspFields, OpenC2Headers, OpenC2Msg, OpenC2Body, OpenC2Rsp

subs = [
    ("oc2/cmd", 1),
    ("oc2/cmd/all", 1)
]


def onMessage(proto: MQTTProtocol, userdata: Any, message: MQTTMessage):
    """Callback Receiving messages from message"""
    print(f"msg={message.payload}")
    print(f"userdata={userdata}")

    try:
        openc2_msg = OpenC2Msg(**message)
    except ValidationError as e:
        # logging.error(e)
        openc2_rsp = OpenC2RspFields(status=500, status_text='Malformed OpenC2 message')
        return self.create_response_msg(openc2_rsp, encode=encode)
    except KeyError as e:
        # logging.error(e)
        openc2_rsp = OpenC2RspFields(status=500, status_text='Malformed OpenC2 message')
        return self.create_response_msg(openc2_rsp, encode=encode)

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
    loggers = ("__main__", "https", "mqtt", "websockets")
    level = "debug"
    for name in loggers:
        setLogLevel(namespace=name, levelStr=level)
    log.info("{config}", config=str(config))

    if config.https:
        log.info("Initializing HTTPS")
        if config.https.key and config.https.cert:
            setupWebServerSSL(reactor, config.https.port, config.https.key, config.https.cert)
        else:
            setupWebServer(reactor, config.https.port)

    if config.websockets:
        log.info("Initializing WebSockets")
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


    def create_response_msg(self, response_body,  encode: str, headers: OpenC2Headers = None) -> Union[str, bytes]:
        """
        Creates and serializes an OpenC2 Response.
        :param response_body: Information to populate OpenC2 Response fields.
        :param headers: Information to populate OpenC2 Response headers.
        :param encode: String specifying the serialization format for the Response.
        :return: Serialized OpenC2 Response
        """
        print("creating response")

        if headers is None:
            # print("debugging: no headers")
            headers = OpenC2Headers(from_="Yuuki")

        else:
            # print("debugging: found headers")
            headers = OpenC2Headers(
                request_id=headers.request_id,
                from_="Yuuki",
                to=headers.from_,
                created=round(time() * 1000)
            )

        message = OpenC2Msg(headers=headers, body=OpenC2Body(openc2=OpenC2Rsp(response=response_body)))
        response = message.dict(by_alias=True, exclude_unset=True, exclude_none=True)
        print("debugging: response on the way")
        return self.serializations[encode].serialize(response)