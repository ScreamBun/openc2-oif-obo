from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
from transports.twisted_http import setupWebServer, setupWebServerSSL
from utils import get_config_data, setLogLevel, startLogging


if __name__ == '__main__':
    config = get_config_data()

    if config.https:
        print(config.https)

    log = Logger()
    startLogging()
    setLogLevel(namespace='__main__', levelStr='debug')
    setLogLevel(namespace='http', levelStr='debug')

    if config.https.key and config.https.cert:
        setupWebServerSSL(reactor, config.https.port, config.https.key, config.https.cert, config.https.paths)
    else:
        setupWebServer(reactor, config.https.port, config.https.paths)

    log.info("Reactor Running")
    reactor.run()
