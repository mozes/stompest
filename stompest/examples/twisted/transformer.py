import logging
import simplejson
from twisted.internet import reactor, defer
from stompest.async import StompConfig, StompCreator

class IncrementTransformer(object):
    
    IN_QUEUE = '/queue/testIn'
    OUT_QUEUE = '/queue/testOut'
    ERROR_QUEUE = '/queue/testTransformerError'

    def __init__(self, config=None):
        if config is None:
            config = StompConfig('localhost', 61613)
        self.config = config
        
    @defer.inlineCallbacks
    def run(self):
        #Establish connection
        stomp = yield StompCreator(self.config).getConnection()
        #Subscribe to inbound queue
        headers = {
            #client-individual mode is only supported in AMQ >= 5.2 but necessary for concurrent processing
            'ack': 'client-individual',
            #this is the maximum messages the broker will let you work on at the same time
            'activemq.prefetchSize': 100, 
        }
        stomp.subscribe(self.IN_QUEUE, self.addOne, headers, errorDestination=self.ERROR_QUEUE)
    
    def addOne(self, stomp, frame):
        """
        NOTE: you can return a Deferred here
        """
        data = simplejson.loads(frame['body'])
        data['count'] += 1
        stomp.send(self.OUT_QUEUE, simplejson.dumps(data))
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    trnsfrmr = IncrementTransformer()
    trnsfrmr.run()
    reactor.run()