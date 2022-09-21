from typing import List, NoReturn
from twisted.internet import reactor as Reactor, ssl
from twisted.logger import Logger
from twisted.web.resource import Resource
from twisted.web.server import Site

__all__ = [
    "setupWebServer",
    "setupWebServerSSL"
]

log = Logger(namespace="https")


class WebProtocol(Resource):
    def getChild(self, path, request):
        if path == '':
            return self
        return super().getChild(path, request)

    def render_GET(self, request):
        # Do some work
        return b"hit"

    def render_POST(self, request):
        for k, v in request.__dict__:
            log.info(f"{k}: {v}")
        # Do some work
        return request.content.getvalue()


def setupWebServer(reactor: Reactor, port: int, paths: List[str] = None) -> NoReturn:
    root = WebProtocol()
    if paths:
        for path in paths:
            root.putChild(path.encode("utf-8"), WebProtocol())
    site = Site(root)
    reactor.listenTCP(port, site)


def setupWebServerSSL(reactor: Reactor, port: int, key: str, crt: str, paths: List[str] = None) -> NoReturn:
    root = WebProtocol()
    if paths:
        for path in paths:
            root.putChild(path.encode("utf-8"), WebProtocol())
    site = Site(root)
    reactor.listenSSL(port, site, ssl.DefaultOpenSSLContextFactory(key, crt))
