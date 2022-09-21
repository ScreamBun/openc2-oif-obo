from typing import NoReturn, Union
from autobahn.twisted.websocket import WebSocketServerFactory, WebSocketServerProtocol
from autobahn.websocket.types import ConnectionRequest
from twisted.logger import Logger
from twisted.internet import reactor, ssl

__all__ = ["setupWebSocket"]

log = Logger(namespace="websockets")


class WebSocketProtocol(WebSocketServerProtocol):
    def onOpen(self):
        log.info("WebSocket connection open")

    def onClose(self, wasClean: bool, code: int, reason: str):
        log.info("WebSocket connection closed")

    def onConnect(self, request: ConnectionRequest):
        log.info(f"WebSocket connection request: {request}")

    def onMessage(self, payload: Union[bytes, str], isBinary: bool):
        # self.sendMessage(payload, isBinary)
        log.info(f"Websocket Received: {payload}")


class WebSocketFactory(WebSocketServerFactory):
    protocol = WebSocketProtocol


def setupWebSocket(core: reactor, port: int) -> NoReturn:
    factory = WebSocketServerFactory()
    core.listenTCP(port, factory)


def setupWebSocketSSL(core: reactor, port: int, key: str, cert: str) -> NoReturn:
    factory = WebSocketServerFactory()
    core.listenSSL(port, factory, ssl.DefaultOpenSSLContextFactory(key, cert))
