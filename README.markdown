stomp, stomper, stompest!

Stompest is a no-nonsense [STOMP 1.0](http://stomp.github.com/) implementation for Python including both synchronous and [Twisted](http://twistedmatrix.com/) clients.

Modeled after the Perl [Net::Stomp](http://search.cpan.org/dist/Net-Stomp/lib/Net/Stomp.pm) module, the synchronous client is dead simple.  It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want. The optional session layer adds failover logic and subscription state to the simple client.

The Twisted client is a full-featured STOMP protocol client.  It supports destination-specific message handlers, concurrent message processing, graceful shutdown, "poison pill" error handling, and connection timeout.

This module is thoroughly unit tested and production hardened for the functionality used by [Mozes](http://www.mozes.com/): persistent queueing on [ActiveMQ](http://activemq.apache.org/).  Minor enhancements may be required to use this STOMP adapter with other brokers or certain feature sets like transactions.

Examples
======== 

Note
----
If you use ActiveMQ to run these examples, make sure you enable the Stomp connector in the ActiveMQ config file, activemq.xml.
   
    <transportConnector name="stomp"  uri="stomp://0.0.0.0:61613"/>
   
See http://activemq.apache.org/stomp.html for details.

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


Simple + Session
----------------

In the simple client examples above, replace

    from stompest.simple import Stomp
    stomp = Stomp('localhost', 61613)

by

    from stompest.sync import Stomp
    stomp = Stomp('failover:(tcp://localhost:61614,tcp://localhost:61613)?randomize=false,maxReconnectAttempts=2')

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
            data = simplejson.loads(frame.body)
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
            data = simplejson.loads(frame.body)
            print "Received msg with count %s" % data['count']
    
    if __name__ == '__main__':
        logging.basicConfig(level=logging.DEBUG)
        Consumer().run()
        reactor.run()

Features
========

Protocol layer
--------------
* Transport and client agnostic
* Supports STOMP 1.0 and 1.1
* Supports binary message bodies

Session layer
-------------
* Manages the state of a connection
* Mimics the [broker failover](http://activemq.apache.org/failover-transport-reference.html) behavior of the native Java client
* Replays subscriptions upon reconnect
* Not yet fully client agnostic (currently only works on top of the simple client; support for the Twisted client is planned)

Simple
------
* Basically the same as [Net::Stomp](http://search.cpan.org/dist/Net-Stomp/lib/Net/Stomp.pm)
* Now supports STOMP 1.1 (NACK, heartbeat)

Twisted
-------
* Graceful shutdown - On disconnect or error, the client stops processing new messages and waits for all outstanding message handlers to finish before issuing the DISCONNECT command
* Error handling - STOMP 1.0 does not have a NACK command so you have two options for error handling:
    * Client error handling - passing the errorDestination parameter to the subscribe() method will cause unhandled messages to be forwarded to that destination.
    * Disconnecting - if you do not configure an errorDestination and an exception propagates up from a message handler, then the client will gracefully disconnect.  This is effectively a NACK for the message.
    You can [configure ActiveMQ](http://activemq.apache.org/message-redelivery-and-dlq-handling.html) with a redelivery policy to avoid the "poison pill" scenario where the broker keeps redelivering a bad message infinitely.
* Fully unit-tested including a simulated STOMP broker
* Configurable connection timeout

Caveats
=======
* Tested with ActiveMQ versions 4.3 and above.  Mileage may vary with other STOMP implementations

Twisted
-------
* Written before the advent of defer.inlineCallbacks so it could be simpler

Options
=======

Twisted
-------
* StompCreator
    * alwaysDisconnectOnUnhandledMsg (defaults to False)
        * For backward-compatibility, you can set this option to True and the client will always disconnect in the case of an unhandled error in a message handler, even if an error destination has been
        configured.

Changes
=======
* 1.0.4 - Bug fix thanks to [Njal Karevoll](https://github.com/nkvoll).  No longer relies on newline after the null-byte frame separator.  Library is now compatible with RabbitMQ stomp adapter.
* 1.1.1 - Thanks to [nikipore](https://github.com/nikipore) for adding support for binary messages.
* 1.1.2 - Fixed issue with stomper adding a space in ACK message-id header.  AMQ 5.6.0 no longer tolerates this.
