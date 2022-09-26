from autobahn.twisted.websocket import WebSocketServerFactory, WebSocketServerProtocol
from twisted.internet import reactor, ssl
from twisted.logger import Logger

__all__ = ["setupWebSocket"]

log = Logger(namespace="websockets")


class WebSocketProtocol(WebSocketServerProtocol):
    def onConnect(self, request):
        log.info("Client connecting: {peer}", peer=request.peer)

    def onOpen(self):
        log.info("WebSocket connection open.")

    def onMessage(self, payload, isBinary):
        if isBinary:
            log.info(f"Binary message received: {payload} bytes", payload=len(payload))
        else:
            log.info(f"Text message received: {payload}", payload=(payload.decode('utf8')))

        # echo back message verbatim
        self.sendMessage(payload, isBinary)

    def onClose(self, wasClean, code, reason):
        log.info("WebSocket connection closed: {reason}", reason=reason)


def createWsConnectionStr(host: str, port: int, secure: bool = False) -> str:
    return f"ws{'s' if secure else ''}://{host}:{port}"


def setupWebSocket(core: reactor, host: str, port: int) -> None:
    ws_connection = createWsConnectionStr(host, port)
    factory = WebSocketServerFactory(ws_connection)
    factory.protocol = WebSocketProtocol
    core.listenTCP(port, factory)


def setupWebSocketSSL(core: reactor, host: str, port: int, key: str, cert: str) -> None:
    ws_connection = createWsConnectionStr(host, port, True)
    factory = WebSocketServerFactory(ws_connection)
    factory.protocol = WebSocketProtocol
    core.listenSSL(port, factory, ssl.DefaultOpenSSLContextFactory(key, cert))
