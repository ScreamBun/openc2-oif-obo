from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
from transports.twisted_http import setupWebServer, setupWebServerSSL
from utils import get_config_data, setLogLevel, startLogging


if __name__ == '__main__':
    # Load config
    config = get_config_data()

    # Setup logging
    log = Logger()
    startLogging()
    setLogLevel(namespace='__main__', levelStr='debug')
    setLogLevel(namespace='http', levelStr='debug')

    # Setup reactor
    print(config)
    if config.https.key and config.https.cert:
        # Secure
        setupWebServerSSL(reactor, config.https.port, config.https.key, config.https.cert, config.https.paths)
    else:
        # Unsecure
        setupWebServer(reactor, config.https.port, config.https.paths)
    reactor.run()
