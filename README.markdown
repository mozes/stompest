stomp, stomper, stompest!

Stompest is a no-nonsense [STOMP](http://stomp.codehaus.org/) implementation for Python including both synchronous and [Twisted](http://twistedmatrix.com/) clients.

Modeled after the Perl [Net::Stomp](http://search.cpan.org/dist/Net-Stomp/lib/Net/Stomp.pm) module, the synchronous client is dead simple.  It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.

The Twisted client is a full-featured STOMP protocol client built on top of the [stomper](http://code.google.com/p/stomper/) library.  It supports destination-specific message handlers, concurrent message processing, graceful shutdown, "poison pill" error handling, and connection timeout.

This module is thoroughly unit tested and production hardened for the functionality used by [Mozes](http://www.mozes.com/): persistent queueing on [ActiveMQ](http://activemq.apache.org/).  Minor enhancements may be required to use this STOMP adapter with other brokers or certain feature sets like transactions or binary messages.

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
        Producer().run()
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
        IncrementTransformer().run()
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
        Consumer().run()
        reactor.run()

Features
========

Simple
------
Basically the same as [Net::Stomp](http://search.cpan.org/dist/Net-Stomp/lib/Net/Stomp.pm)

Twisted
-------
* Graceful shutdown - On disconnect or error, the client stops processing new messages and waits for all outstanding message handlers to finish before issuing the DISCONNECT command
* Error handling - the client assumes that if an exception is not handled by a message handler, it's not necessarily safe to continue processing so it shutdowns down gracefully.  By passing the errorDestination parameter to the subscribe() method, it can be configured to send a copy of offending message to a configured destination.  This will allow you to avoid the "poison pill" scenario where the broker keeps redelivering a bad message infinitely.
* Fully unit-tested including a simulated STOMP broker
* Configurable connection timeout


Caveats
=======
* Does not support binary data - support for the content-length header is not (yet) implemented
* Tested with ActiveMQ versions 4.3 and above.  Mileage may vary with other STOMP implementations

Twisted
-------
* Written before the advent of defer.inlineCallbacks so it could be simpler
