"""
Base: https://medium.com/analytics-vidhya/how-to-create-a-thread-safe-singleton-class-in-python-822e1170a7f6
"""
import threading


class Singleton:
    __instance__ = None
    __lock__ = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls.__instance__:
            with cls.__lock__:
                # another thread could have created the instance
                # before we acquired the lock. So check that the
                # instance is still nonexistent.
                if not cls.__instance__:
                    cls.__instance__ = super(Singleton, cls).__new__(cls)
        return cls.__instance__
