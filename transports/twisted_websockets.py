from typing import NoReturn, Union
from autobahn.twisted.websocket import WebSocketServerFactory, WebSocketServerProtocol
from autobahn.websocket.types import ConnectionRequest
from twisted.internet import reactor, ssl

__all__ = ["setupWebSocket"]


class WebSocketProtocol(WebSocketServerProtocol):
    def onOpen(self):
        print("WebSocket connection open")

    def onClose(self, wasClean: bool, code: int, reason: str):
        print("WebSocket connection closed")

    def onConnect(self, request: ConnectionRequest):
        print(f"WebSocket connection request: {request}")

    def onMessage(self, payload: Union[bytes, str], isBinary: bool):
        # self.sendMessage(payload, isBinary)
        print(f"Websocket Received: {payload}")


class WebSocketFactory(WebSocketServerFactory):
    protocol = WebSocketProtocol


def setupWebSocket(core: reactor, port: int) -> NoReturn:
    factory = WebSocketServerFactory()
    core.listenTCP(port, factory)


def setupWebSocketSSL(core: reactor, port: int, key: str, crt: str) -> NoReturn:
    factory = WebSocketServerFactory()
    core.listenSSL(port, factory, ssl.DefaultOpenSSLContextFactory(key, crt))
