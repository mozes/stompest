"""The :mod:`~.protocol` package is a collection of generic components each of which you can use independently for your own STOMP related functionality:
"""
# TODO: STOMP 1.1 - deal with repeated headers -> http://stomp.github.com/stomp-specification-1.1.html#Repeated_Header_Entries

from failover import StompFailoverProtocol, StompFailoverUri
from frame import StompFrame
from parser import StompParser
from spec import StompSpec
from session import StompSession
