from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
from transports.twisted_http import setupWebServer
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
    setupWebServer(reactor, config.https.port)
    reactor.run()
