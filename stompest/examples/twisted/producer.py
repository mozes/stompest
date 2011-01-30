import logging
import simplejson
from twisted.internet import reactor, defer
from stompest.async import StompConfig, StompCreator

class Producer(object):
    
    QUEUE = '/queue/testIn'

    def __init__(self, config=None):
        if config is None:
            config = StompConfig('localhost', 61613)
        self.config = config
        
    @defer.inlineCallbacks
    def run(self):
        #Establish connection
        stomp = yield StompCreator(self.config).getConnection()
        #Enqueue 10 messages
        try:
            for x in range(10):
                stomp.send(self.QUEUE, simplejson.dumps({'count': x}))
        finally:
            #Give the reactor time to complete the writes
            reactor.callLater(1, reactor.stop)
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    prdcr = Producer()
    prdcr.run()
    reactor.run()