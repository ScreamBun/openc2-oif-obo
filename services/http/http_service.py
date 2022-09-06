from pprint import pprint

from twisted.web.resource import Resource


class HTTPService(Resource):

    # def __init__(self) -> object:
    #     print("init")

    def render_GET(self, request):
        # Do some work
        return b"hit"

    def render_POST(self, request):
        pprint(request.__dict__)
        newdata = request.content.getvalue()
        # Do some work
        return newdata