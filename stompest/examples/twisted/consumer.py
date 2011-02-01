"""
Copyright 2011 Mozes, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import logging
import simplejson
from twisted.internet import reactor, defer
from stompest.async import StompConfig, StompCreator

class Consumer(object):
    
    QUEUE = '/queue/testOut'
    ERROR_QUEUE = '/queue/testConsumerError'

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
        stomp.subscribe(self.QUEUE, self.consume, headers, errorDestination=self.ERROR_QUEUE)
    
    def consume(self, stomp, frame):
        """
        NOTE: you can return a Deferred here
        """
        data = simplejson.loads(frame['body'])
        print "Received msg with count %s" % data['count']
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    Consumer().run()
    reactor.run()