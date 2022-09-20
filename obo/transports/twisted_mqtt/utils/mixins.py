from typing import Any, Callable, List, NoReturn, Optional
from .context import DummyLock


class CallbackMixin:
    # Lock
    _callback_mutex: DummyLock = DummyLock()
    # Callbacks
    _on_log: Callable[["MQTTProtocol", Any, "LogLevels", str], NoReturn] = None
    _on_connect: Callable[["MQTTProtocol", Any, dict, int, Optional["Properties"]], NoReturn] = None
    _on_connect_fail: Callable[["MQTTProtocol", Any], NoReturn] = None
    _on_subscribe: Callable[["MQTTProtocol", Any, int, int], NoReturn] = None
    _on_message: Callable[["MQTTProtocol", Any, "MQTTMessage"], NoReturn] = None
    _on_publish: Callable[["MQTTProtocol", Any, int], NoReturn] = None
    _on_unsubscribe: Callable[["MQTTProtocol", Any, int, Optional["Properties"], Optional[List["ReasonCodes"]]], NoReturn] = None
    _on_disconnect: Callable[["MQTTProtocol", Any, int, Optional["Properties"]], NoReturn] = None

    @property
    def on_log(self):
        return self._on_log

    @on_log.setter
    def on_log(self, func):
        self._on_log = func

    def log_callback(self):
        def decorator(func):
            self.on_log = func
            return func
        return decorator

    @property
    def on_connect(self):
        return self._on_connect

    @on_connect.setter
    def on_connect(self, func):
        with self._callback_mutex:
            self._on_connect = func

    def connect_callback(self):
        def decorator(func):
            self.on_connect = func
            return func
        return decorator

    @property
    def on_connect_fail(self):
        return self._on_connect_fail

    @on_connect_fail.setter
    def on_connect_fail(self, func):
        with self._callback_mutex:
            self._on_connect_fail = func

    def connect_fail_callback(self):
        def decorator(func):
            self.on_connect_fail = func
            return func
        return decorator

    @property
    def on_subscribe(self):
        return self._on_subscribe

    @on_subscribe.setter
    def on_subscribe(self, func):
        with self._callback_mutex:
            self._on_subscribe = func

    def subscribe_callback(self):
        def decorator(func):
            self.on_subscribe = func
            return func
        return decorator

    @property
    def on_message(self):
        return self._on_message

    @on_message.setter
    def on_message(self, func):
        with self._callback_mutex:
            self._on_message = func

    def message_callback(self):
        def decorator(func):
            self.on_message = func
            return func
        return decorator

    @property
    def on_publish(self):
        return self._on_publish

    @on_publish.setter
    def on_publish(self, func):
        with self._callback_mutex:
            self._on_publish = func

    def publish_callback(self):
        def decorator(func):
            self.on_publish = func
            return func
        return decorator

    @property
    def on_unsubscribe(self):
        return self._on_unsubscribe

    @on_unsubscribe.setter
    def on_unsubscribe(self, func):
        with self._callback_mutex:
            self._on_unsubscribe = func

    def unsubscribe_callback(self):
        def decorator(func):
            self.on_unsubscribe = func
            return func
        return decorator

    @property
    def on_disconnect(self):
        return self._on_disconnect

    @on_disconnect.setter
    def on_disconnect(self, func):
        with self._callback_mutex:
            self._on_disconnect = func

    def disconnect_callback(self):
        def decorator(func):
            self.on_disconnect = func
            return func
        return decorator
