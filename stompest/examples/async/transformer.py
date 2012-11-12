import json
import logging

from twisted.internet import reactor, defer

from stompest.async import Stomp
from stompest.config import StompConfig

class IncrementTransformer(object):    
    IN_QUEUE = '/queue/testIn'
    OUT_QUEUE = '/queue/testOut'
    ERROR_QUEUE = '/queue/testTransformerError'

    def __init__(self, config=None):
        if config is None:
            config = StompConfig('tcp://localhost:61613')
        self.config = config
        
    @defer.inlineCallbacks
    def run(self):
        client = yield Stomp(self.config).connect()
        headers = {
            # client-individual mode is necessary for concurrent processing
            # (requires ActiveMQ >= 5.2)
            'ack': 'client-individual',
            # the maximal number of messages the broker will let you work on at the same time
            'activemq.prefetchSize': '100', 
        }
        client.subscribe(self.IN_QUEUE, self.addOne, headers, errorDestination=self.ERROR_QUEUE)
    
    def addOne(self, client, frame):
        """
        NOTE: you can return a Deferred here
        """
        data = json.loads(frame.body)
        data['count'] += 1
        client.send(self.OUT_QUEUE, json.dumps(data))
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    IncrementTransformer().run()
    reactor.run()