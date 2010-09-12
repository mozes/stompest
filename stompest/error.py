
class StompError(Exception):
    """Base class for STOMP errors
    """

class StompFrameError(Exception):
    """Raised for error parsing STOMP frames
    """

class StompProtocolError(StompError):
    """Raised for STOMP protocol errors
    """
    
class StompConnectTimeout(StompError):
    """Raised for timeout waiting for connect response from broker
    """