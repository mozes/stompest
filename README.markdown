stomp, stomper, stompest!

stompest is a full-featured [STOMP](http://stomp.github.com/) [1.0](http://stomp.github.com//stomp-specification-1.0.html) and [1.1](http://stomp.github.com//stomp-specification-1.1.html) implementation for Python including both synchronous and [Twisted](http://twistedmatrix.com/) clients.

Modeled after the Perl [Net::Stomp](http://search.cpan.org/dist/Net-Stomp/lib/Net/Stomp.pm) module, the `sync` client is dead simple. It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.

The `async` client is based on [Twisted](http://twistedmatrix.com/), a very mature and powerful asynchronous programming framework framework. It supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect and disconnect timeouts.

Both clients make use of a generic set of components which can be used independently to roll your own STOMP client, viz.

* a faithful implementation of the syntax of the STOMP 1.0 and 1.1 protocols with a simple stateless function API,

* a separate implementation of the STOMP 1.0 and 1.1 session state semantics, such as protocol version negotiation at connect time, transaction and subscription handling (including a generic subscription replay scheme which may be used to reconstruct the session state after a forced disconnect),

* a wire-level STOMP frame parser and compiler,

* and a [failover transport](http://activemq.apache.org/failover-transport-reference.html) URI scheme akin to ActiveMQ.

This module is thoroughly unit tested and production hardened for the functionality used by [Mozes](http://www.mozes.com/): persistent queueing on ActiveMQ. Other users also use it in serious production environments. Minor enhancements may be required to use this STOMP adapter with other brokers.

Examples
======== 

Note
----
If you use ActiveMQ to run these examples, make sure you enable the Stomp connector in the config file, activemq.xml (see http://activemq.apache.org/stomp.html for details):
   
    <transportConnector name="stomp"  uri="stomp://0.0.0.0:61613"/>
   

Synchronous Producer
--------------------

    from stompest.protocol import StompConfig
    from stompest.sync import Stomp
    
    CONFIG = StompConfig('tcp://localhost:61613')
    QUEUE = '/queue/test'
    
    client = Stomp(CONFIG)
    client.connect()
    client.send(QUEUE, 'test message 1')
    client.send(QUEUE, 'test message 2')
    client.disconnect()
        
Sychronous Consumer
-------------------

    from stompest.protocol import StompConfig
    from stompest.sync import Stomp
    
    CONFIG = StompConfig('tcp://localhost:61613')
    QUEUE = '/queue/test'
    
    client = Stomp(CONFIG)
    client.connect()
    client.subscribe(QUEUE, {'ack': 'client'})
    
    while True:
        frame = client.receiveFrame()
        print 'Got %s' % frame.info()
        client.ack(frame)
        
    client.disconnect()
    
Twisted Producer
----------------

    import logging
    import json
    
    from twisted.internet import defer, reactor, task
    
    from stompest.async import Stomp
    from stompest.protocol import StompConfig
    
    class Producer(object):
        QUEUE = '/queue/testIn'
    
        def __init__(self, config=None):
            if config is None:
                config = StompConfig('tcp://localhost:61613')
            self.config = config
            
        @defer.inlineCallbacks
        def run(self):
            # establish connection
            stomp = yield Stomp(self.config).connect()
            # enqueue 10 messages
            try:
                for j in range(10):
                    stomp.send(self.QUEUE, json.dumps({'count': j}))
            finally:
                # give the reactor time to complete the writes
                yield task.deferLater(reactor, 1, stomp.disconnect)
        
    if __name__ == '__main__':
        logging.basicConfig(level=logging.DEBUG)
        Producer().run()
        reactor.run()
                 
Twisted Transformer
-------------------

    import logging
    import json
    
    from twisted.internet import reactor, defer
    
    from stompest.async import Stomp
    from stompest.protocol import StompConfig
    
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
            # establish connection
            client = yield Stomp(self.config).connect()
            # subscribe to inbound queue
            headers = {
                # client-individual mode is only supported in AMQ >= 5.2 but necessary for concurrent processing
                'ack': 'client-individual',
                # this is the maximal number of messages the broker will let you work on at the same time
                'activemq.prefetchSize': 100, 
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
        
Twisted Consumer
----------------

    import logging
    import json
    
    from twisted.internet import reactor, defer
    
    from stompest.async import Stomp
    from stompest.protocol import StompConfig
    
    class Consumer(object):
        QUEUE = '/queue/testOut'
        ERROR_QUEUE = '/queue/testConsumerError'
    
        def __init__(self, config=None):
            if config is None:
                config = StompConfig('tcp://localhost:61613')
            self.config = config
            
        @defer.inlineCallbacks
        def run(self):
            # establish connection
            stomp = yield Stomp(self.config).connect()
            # subscribe to inbound queue
            headers = {
                # client-individual mode is only supported in AMQ >= 5.2 but necessary for concurrent processing
                'ack': 'client-individual',
                # this is the maximal number of messages the broker will let you work on at the same time
                'activemq.prefetchSize': 100, 
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
            
Features
========

Commands layer
--------------
* Transport and client agnostic.
* Full-featured implementation of all STOMP 1.0 and 1.1 client commands.
* Client-side handling of STOMP 1.0 and 1.1 commands received from the broker.
* Stateless and simple function API.

Session layer
-------------
* Manages the state of a connection.
* Replays subscriptions upon reconnect.
* Not yet fully client agnostic (currently only works on top of the simple client; support for the Twisted client is planned).
* Heartbeat handling (not yet implemented).

Failover layer
--------------
* Mimics the [failover transport](http://activemq.apache.org/failover-transport-reference.html) behavior of the native ActiveMQ Java client.
* Produces a (possibly infinite) sequence of broker network addresses and connect delay times.

Parser layer
------------
* Abstract frame definition.
* Transformation between these abstract frames and a wire-level byte stream of STOMP frames.

Clients
=======

`sync`
------
* Built on top of the abstract layers, the synchronous client adds a TCP connection and a synchronous API.
* The concurrency scheme (synchronous, threaded, ...) is free to choose by the user.

`async`
-------
* Based on the Twisted asynchronous framework.
* Fully unit-tested including a simulated STOMP broker.
* Graceful shutdown: on disconnect or error, the client stops processing new messages and waits for all outstanding message handlers to finish before issuing the DISCONNECT command.
* Error handling - fully customizable on a per-subscription level:
    * Disconnect: if you do not configure an errorDestination and an exception propagates up from a message handler, then the client will gracefully disconnect. This is effectively a NACK for the message. You can [configure ActiveMQ](http://activemq.apache.org/message-redelivery-and-dlq-handling.html) with a redelivery policy to avoid the "poison pill" scenario where the broker keeps redelivering a bad message infinitely.
    * Default error handling: passing an error destination parameter at subscription time will cause unhandled messages to be forwarded to that destination.
    * Custom hook: you can override the default behavior with any customized error handling scheme.
* Separately configurable timeouts for wire-level connection, the broker's CONNECTED reply, and graceful disconnect (in-flight handlers that do not finish in time).

Caveats
=======
* Tested with ActiveMQ versions 5.5 and 5.6. Mileage may vary with other STOMP implementations.
* stompest 2 is probably even better tested than stompest 1.x and used in production by one of the authors, but it has gained heavily on functionality. In the thorough redesign, the authors valued consistency, simplicity and symmetry over full backward compatibility to stompest 1.x. The migration is nevertheless very simple and straightforward, and will make your code simpler and more Pythonic.
* It is planned to add more features in the near future. Thus, the API should not be considered stable, which is why stompest 2 is still marked as Alpha software.

To Do
=====
* Python doc style documentation of the API.
* `RECEIPT` frame handling (with timeout) for `sync` and `async` clients.
* `@connected` decorators which absorb the "check connected" boilerplate code (both clients).
* manual (both clients) and automatic (`async` only) `HEARTBEAT` handling.
* The URI scheme supports only TCP, no SSL (the authors don't need it because the client is run in "safe" production environments). For the `async` client, however, it should be straightforward to enhance the URI scheme by means of the [Endpoint API](http://twistedmatrix.com/documents/current/api/twisted.internet.endpoints.html). Contributions are welcome!
* [STOMP 1.2 protocol](http://stomp.github.com/stomp-specification-1.2.html) (not before there is a reference broker implementation available).

Changes
=======
* 1.0.4 - Bug fix thanks to [Njal Karevoll](https://github.com/nkvoll).  No longer relies on newline after the null-byte frame separator.  Library is now compatible with RabbitMQ stomp adapter.
* 1.1.1 - Thanks to [nikipore](https://github.com/nikipore) for adding support for binary messages.
* 1.1.2 - Fixed issue with stomper adding a space in ACK message-id header. ActiveMQ 5.6.0 no longer tolerates this.
* 2.0a1 - Complete redesign: feature-complete implementation of STOMP 1.0 and 1.1 (except heartbeat). Broker failover. Decoupled from [stomper](http://code.google.com/p/stomper/).