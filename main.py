from pprint import pprint

from twisted.application.service import Application
from twisted.internet import reactor
from twisted.web.resource import Resource
from twisted.web.server import Site


class FormPage(Resource):
    def render_GET(self, request):
        # Do some work
        return b"hit"

    def render_POST(self, request):
        pprint(request.__dict__)
        newdata = request.content.getvalue()
        # Do some work
        return newdata


print("Startup")
root = Resource()
root.putChild(b"", FormPage())
root.putChild(b"main", FormPage())
application = Application("OBO Web Service")
factory = Site(root)
reactor.listenTCP(8880, factory)
reactor.run()
print("Reactor Running, listening on port 8880")