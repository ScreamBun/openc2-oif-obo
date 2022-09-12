from typing import List, NoReturn
from twisted.web.resource import Resource
from twisted.web.server import Site
from twisted.internet import reactor, ssl

__all__ = ["setupWebServer", "setupWebServerSSL"]


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
            print(f"{k}: {v}")
        # Do some work
        return request.content.getvalue()


def setupWebServer(core: reactor, port: int, paths: List[str] = None) -> NoReturn:
    root = WebProtocol()
    for path in paths:
        root.putChild(path, WebProtocol())
    site = Site(root)
    core.listenTCP(port, site)


def setupWebServerSSL(core: reactor, port: int, key: str, crt: str, paths: List[str] = None) -> NoReturn:
    root = WebProtocol()
    for path in paths:
        root.putChild(path, WebProtocol())
    site = Site(root)
    core.listenSSL(port, site, ssl.DefaultOpenSSLContextFactory(key, crt))
