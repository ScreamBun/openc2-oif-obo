from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
from transports.twisted_websockets import setupWebSocket, setupWebSocketSSL
from utils import get_config_data, setLogLevel, startLogging


if __name__ == '__main__':
    # Load config
    config = get_config_data()

    # Setup logging
    log = Logger()
    startLogging()
    setLogLevel(namespace='__main__', levelStr='debug')
    setLogLevel(namespace='websockets', levelStr='debug')

    # Setup reactor
    print(config)
    if config.websockets.key and config.websockets.cert:
        # Secure
        setupWebSocketSSL(reactor, config.websockets.port, config.websockets.key, config.websockets.cert)
    else:
        # Unsecure
        setupWebSocket(reactor, config.websockets.port)
    reactor.run()
