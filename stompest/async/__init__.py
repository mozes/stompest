"""
The ``async.Stomp`` client is based on `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. It supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect and disconnect timeouts.
"""
from client import Stomp
