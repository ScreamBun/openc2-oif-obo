import json
import sys

from time import time
from typing import Any
from twisted.internet import reactor
from twisted.logger import (
    Logger, LogLevel, FilteringLogObserver, LogLevelFilterPredicate, globalLogBeginner, textFileLogObserver,
)
from transports.twisted_mqtt import MQTTFactory, MQTTMessage, MQTTProtocol, MQTTService, Versions

logLevelFilterPredicate = LogLevelFilterPredicate(defaultLogLevel=LogLevel.info)
BROKER = "tcp:mosquitto.olympus.mtn:1883"
# BROKER = "tcp:localhost:1883"
subs = [
    ("oc2/cmd", 1),
    ("oc2/cmd/all", 1)
]


def startLogging(console=True, filepath=None):
    """
    Starts the global Twisted logger subsystem with maybe
    stdout and/or a file specified in the config file
    """
    global logLevelFilterPredicate

    observers = []
    if console:
        observers.append(FilteringLogObserver(observer=textFileLogObserver(sys.stdout), predicates=[logLevelFilterPredicate]))

    if filepath is not None and filepath != "":
        observers.append(FilteringLogObserver(observer=textFileLogObserver(open(filepath, 'a')), predicates=[logLevelFilterPredicate]))
    globalLogBeginner.beginLoggingTo(observers)


def setLogLevel(namespace=None, levelStr='info'):
    """
    Set a new log level for a given namespace
    LevelStr is: 'critical', 'error', 'warn', 'info', 'debug'
    """
    level = LogLevel.levelWithName(levelStr)
    logLevelFilterPredicate.setLogLevelForNamespace(namespace=namespace, level=level)


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


if __name__ == "__main__":
    log = Logger()
    startLogging()
    setLogLevel(namespace='mqtt', levelStr='debug')
    setLogLevel(namespace='__main__', levelStr='debug')

    print("Startup")
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
