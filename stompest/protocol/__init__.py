# TODO: STOMP 1.1 - deal with repeated headers -> http://stomp.github.com/stomp-specification-1.1.html#Repeated_Header_Entries
# TODO: UTF-8 encoding of STOMP frames

from failover import StompConfig, StompFailoverProtocol, StompFailoverUri
from frame import StompFrame
from parser import StompParser
from spec import StompSpec
from session import StompSession