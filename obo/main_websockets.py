from twisted.internet import reactor
from twisted.logger import Logger
# Local imports
from transports.twisted_websockets import setupWebSocket, setupWebSocketSSL
from utils import get_config_data, setLogLevel, startLogging

if __name__ == '__main__':
    config = get_config_data()

    log = Logger()
    startLogging()
    setLogLevel(namespace='__main__', levelStr=config.websockets.log_level)
    setLogLevel(namespace='websockets', levelStr=config.websockets.log_level)
    log.info(config.websockets)

    if config.websockets.key and config.websockets.cert:
        setupWebSocketSSL(reactor, config.websockets.host, config.websockets.port, config.websockets.key, config.websockets.cert)
    else:
        setupWebSocket(reactor, config.websockets.host, config.websockets.port)

    log.info("Reactor Running")
    reactor.run()
