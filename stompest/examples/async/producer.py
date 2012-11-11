import json
import logging

from twisted.internet import defer, reactor

from stompest.async import Stomp
from stompest.config import StompConfig

class Producer(object):
    QUEUE = '/queue/testIn'

    def __init__(self, config=None):
        if config is None:
            config = StompConfig('tcp://localhost:61613')
        self.config = config
        
    @defer.inlineCallbacks
    def run(self):
        stomp = yield Stomp(self.config).connect()
        for j in range(10):
            stomp.send(self.QUEUE, json.dumps({'count': j}), receipt='message-%d' % j)
        yield stomp.disconnect() # graceful disconnect: waits until all receipts have arrived
        reactor.stop()
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    Producer().run()
    reactor.run()