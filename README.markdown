stomp, stomper, stompest!

Stompest is a no-nonsense STOMP implementation for Python including both a synchronous and a Twisted implementation.

Unlike other python STOMP implementations, the synchronous client does not assume anything about your concurrency model (thread vs process).  Modeled after the Perl Net::Stomp module, it mostly stays out of your way and lets you do what you want.

The Twisted client builds additional functionality on top of the stomper library to provide a full-featured client.

Examples
========

Simple Producer
---------------

    from stompest.simple import Stomp

    QUEUE = '/queue/simpleTest'

    stomp = Stomp('localhost', 61613)
    stomp.connect()
    stomp.send(QUEUE, 'test message1')
    stomp.send(QUEUE, 'test message2')
    stomp.disconnect()
    
Simple Consumer
---------------

from stompest.simple import Stomp

QUEUE = '/queue/simpleTest'

stomp = Stomp('localhost', 61613)
stomp.connect()
stomp.subscribe(QUEUE, {'ack': 'client'})

while(True):
    frame = stomp.receiveFrame()
    print "Got message frame: %s" % frame
    stomp.ack(frame)
    
stomp.disconnect()

Twisted Producer
----------------

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
        
Twisted Transformer
-------------------

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

Twisted Consumer
----------------

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
        cnsmr = Consumer()
        cnsmr.run()
        reactor.run()

Features
========

TODO - list them

Caveats
=======

TODO - list them
