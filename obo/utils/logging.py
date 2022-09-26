import sys

from twisted.logger import (
    LogLevelFilterPredicate, LogLevel, FilteringLogObserver, globalLogBeginner, textFileLogObserver
)

logLevelFilterPredicate = LogLevelFilterPredicate(defaultLogLevel=LogLevel.info)


def setLogLevel(namespace=None, levelStr='info'):
    """
    Set a new log level for a given namespace
    LevelStr is: 'critical', 'error', 'warn', 'info', 'debug'
    """
    level = LogLevel.levelWithName(levelStr)
    logLevelFilterPredicate.setLogLevelForNamespace(namespace=namespace, level=level)


def startLogging(console=True, filepath=None):
    """
    Starts the global Twisted logger subsystem with maybe
    stdout and/or a file specified in the config file
    """
    observers = []
    if console:
        observers.append(FilteringLogObserver(observer=textFileLogObserver(sys.stdout), predicates=[logLevelFilterPredicate]))

    if filepath is not None and filepath != "":
        with open(filepath, "a", encoding="utf-8") as f:
            observers.append(FilteringLogObserver(observer=textFileLogObserver(f), predicates=[logLevelFilterPredicate]))
    globalLogBeginner.beginLoggingTo(observers)
