

class MQTTException(Exception):
    pass


class MalformedPacket(MQTTException):
    pass


class WebsocketConnectionError(ValueError):
    pass
