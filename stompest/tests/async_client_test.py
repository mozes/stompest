# -*- coding: iso-8859-1 -*-
"""
Copyright 2011, 2012 Mozes, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import logging

from twisted.internet import defer, reactor
from twisted.internet.protocol import Factory
from twisted.python import log
from twisted.trial import unittest

from stompest.async import Stomp
from stompest.error import StompCancelledError, StompConnectionError, StompConnectTimeout, StompProtocolError

from stompest.protocol import StompConfig
from stompest.tests.broker_simulator import BlackHoleStompServer, ErrorOnConnectStompServer, ErrorOnSendStompServer, RemoteControlViaFrameStompServer

observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig(level=logging.DEBUG)

class AsyncClientBaseTestCase(unittest.TestCase):
    protocols = []
    
    def setUp(self):
        self.connections = map(self._create_connection, self.protocols)
    
    def _create_connection(self, protocol):
        factory = Factory()
        factory.protocol = protocol
        connection = reactor.listenTCP(0, factory) #@UndefinedVariable
        return connection
        
    def tearDown(self):
        for connection in self.connections:
            connection.stopListening()

class AsyncClientConnectTimeoutTestCase(AsyncClientBaseTestCase):
    protocols = [BlackHoleStompServer]
    TIMEOUT = 0.02

    def test_connection_timeout(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)
        try:
            yield client.connect(connectTimeout=self.TIMEOUT, connectedTimeout=self.TIMEOUT)
        except StompConnectTimeout:
            pass
        else:
            raise

    def test_connection_timeout_after_failover(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=2,initialReconnectDelay=0,randomize=false' % port)
        client = Stomp(config)
        try:
            yield client.connect(connectTimeout=self.TIMEOUT, connectedTimeout=self.TIMEOUT)
        except StompConnectTimeout:
            pass
        else:
            raise
    
    @defer.inlineCallbacks
    def test_not_connected(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)
        try:
            yield client.send('/queue/fake')
        except StompConnectionError:
            pass
        
class AsyncClientConnectErrorTestCase(AsyncClientBaseTestCase):
    protocols = [ErrorOnConnectStompServer]

    def test_stomp_protocol_error_on_connect(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = Stomp(config)
        return self.assertFailure(client.connect(), StompProtocolError)

class AsyncClientErrorAfterConnectedTestCase(AsyncClientBaseTestCase):
    protocols = [ErrorOnSendStompServer]

    @defer.inlineCallbacks
    def test_disconnect_on_stomp_protocol_error(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=1,initialReconnectDelay=0,randomize=false' % port)
        client = Stomp(config)
        
        yield client.connect()
        client.send('/queue/fake', 'fake message')
        try:
            yield client.disconnected
        except StompProtocolError:
            pass
        else:
            raise

class AsyncClientFailoverOnDisconnectTestCase(AsyncClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer, ErrorOnSendStompServer]
    
    @defer.inlineCallbacks
    def test_failover_on_connection_lost(self):
        ports = tuple(c.getHost().port for c in self.connections)
        config = StompConfig(uri='failover:(tcp://localhost:%d,tcp://localhost:%d)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,randomize=false,maxReconnectAttempts=1' % ports)
        client = Stomp(config)
        
        yield client.connect()
        self.connections[0].stopListening()
        client.send('/queue/fake', 'shutdown')
        try:
            client = yield client.disconnected
        except StompConnectionError:
            yield client.connect()
        client.send('/queue/fake', 'fake message')
        
        try:
            yield client.disconnected
        except StompProtocolError:
            pass

class AsyncClientReplaySubscriptionTestCase(AsyncClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer]
    
    @defer.inlineCallbacks
    def test_replay_after_failover(self):
        ports = tuple(c.getHost().port for c in self.connections)
        config = StompConfig(uri='failover:(tcp://localhost:%d)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,maxReconnectAttempts=1' % ports)
        client = Stomp(config)
        try:
            client.subscribe('/queue/bla', self._on_message) # client is not connected, so it won't accept subscriptions
        except StompConnectionError:
            pass
        else:
            raise
        
        self.assertEquals(client.session._subscriptions, {}) # check that no subscriptions have been accepted
        yield client.connect()
        
        self.shutdown = True # the callback handler will kill the broker connection ... 
        client.subscribe('/queue/bla', self._on_message)
        try:
            client = yield client.disconnected # the callback handler has killed the broker connection
        except StompConnectionError:
            pass
        else:
            raise
            
        self.shutdown = False # the callback handler will not kill the broker connection, but callback self._got_message
        self._got_message = defer.Deferred()
        
        yield client.connect()
        self.assertNotEquals(client.session._subscriptions, []) # the subscriptions have been replayed ...
        
        result = yield self._got_message
        self.assertEquals(result, None) # ... and the message comes back
        
        yield client.disconnect()
        self.assertEquals(list(client.session.replay()), []) # after a clean disconnect, the subscriptions are forgotten.

    def _on_message(self, client, msg):
        self.assertTrue(isinstance(client, Stomp))
        self.assertEquals(msg.body, 'hi')
        if self.shutdown:
            client.send('/queue/fake', 'shutdown')
        else:
            self._got_message.callback(None)
        
class AsyncClientDisconnectTimeoutTestCase(AsyncClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer]
    
    @defer.inlineCallbacks
    def test_disconnect_timeout(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port, version='1.1')
        client = Stomp(config)
        yield client.connect()
        self._got_message = defer.Deferred()
        client.subscribe('/queue/bla', self._on_message, headers={'id': 4711}, ack=False) # we're acking the frames ourselves
        yield self._got_message
        try:
            yield client.disconnect(timeout=0.02)
        except StompCancelledError:
            pass
        else:
            raise
        self.wait.callback(None)
        
    @defer.inlineCallbacks
    def test_disconnect_connection_lost_unexpectedly(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port, version='1.1')
        client = Stomp(config)
        
        yield client.connect()
        
        self._got_message = defer.Deferred()
        client.subscribe('/queue/bla', self._on_message, headers={'id': 4711}, ack=False) # we're acking the frames ourselves
        yield self._got_message
        
        disconnected = client.disconnect()
        client.send('/queue/fake', 'shutdown') # tell the broker to drop the connection
        try:
            yield disconnected
        except StompCancelledError:
            pass
        else:
            raise
        
        self.wait.callback(None)

    @defer.inlineCallbacks
    def _on_message(self, client, msg):
        client.nack(msg)
        self.wait = defer.Deferred()
        self._got_message.callback(None)
        yield self.wait

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
