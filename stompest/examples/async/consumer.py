import json
import logging

from twisted.internet import reactor, defer

from stompest.async import Stomp
from stompest.config import StompConfig

class Consumer(object):
    QUEUE = '/queue/testOut'
    ERROR_QUEUE = '/queue/testConsumerError'

    def __init__(self, config=None):
        if config is None:
            config = StompConfig('tcp://localhost:61613')
        self.config = config
        
    @defer.inlineCallbacks
    def run(self):
        stomp = yield Stomp(self.config).connect()
        headers = {
            # client-individual mode is necessary for concurrent processing
            # (requires ActiveMQ >= 5.2)
            'ack': 'client-individual',
            # the maximal number of messages the broker will let you work on at the same time
            'activemq.prefetchSize': '100', 
        }
        stomp.subscribe(self.QUEUE, self.consume, headers, errorDestination=self.ERROR_QUEUE)
    
    def consume(self, client, frame):
        """
        NOTE: you can return a Deferred here
        """
        data = json.loads(frame.body)
        print 'Received frame with count %d' % data['count']
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    Consumer().run()
    reactor.run()