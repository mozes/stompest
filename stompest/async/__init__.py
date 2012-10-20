from client import StompClient, StompCreator
from failover import StompFailoverClient

import stompest.protocol

class StompConfig(stompest.protocol.StompConfig):
    def __init__(self, host, port, **kwargs):
        import warnings
        warnings.warn('stompest.async.StompConfig is deprecated. Use stompest.protocol.StompConfig instead!')
        super(StompConfig, self).__init__(uri=self._parseUri(host, port), **kwargs)
    
    def _parseUri(self, host, port):
        if not (host and port):
            raise ValueError('host or port missing')
        return 'tcp://%s:%d' % (host, port)
