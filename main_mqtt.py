import sys

from twisted.internet import reactor
from twisted.internet.endpoints import clientFromString
from twisted.logger import (
    Logger, LogLevel, FilteringLogObserver, LogLevelFilterPredicate, globalLogBeginner, textFileLogObserver,
)
from transports.twisted_mqtt import MQTTFactory, MQTTService, Versions

logLevelFilterPredicate = LogLevelFilterPredicate(defaultLogLevel=LogLevel.info)
BROKER = "tcp:mosquitto.olympus.mtn:1883"
subs = [
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


if __name__ == "__main__":
    log = Logger()
    startLogging()
    setLogLevel(namespace='mqtt',     levelStr='debug')
    setLogLevel(namespace='__main__', levelStr='debug')

    print("Startup")
    factory = MQTTFactory(client_id="Twisted-368207455685", subs=subs, protocol=Versions.v5)
    myEndpoint = clientFromString(reactor, BROKER)
    serv = MQTTService(myEndpoint, factory)
    serv.startService()
    print("Reactor Running")
    reactor.run()
