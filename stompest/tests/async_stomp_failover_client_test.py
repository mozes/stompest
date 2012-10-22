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

from stompest.async import StompFailoverClient
from stompest.error import StompConnectionError, StompConnectTimeout, StompProtocolError
from stompest.protocol import StompConfig
from stompest.tests.broker_simulator import BlackHoleStompServer, ErrorOnConnectStompServer, ErrorOnSendStompServer, RemoteControlViaFrameStompServer

observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig(level=logging.DEBUG)

class AsyncFailoverClientBaseTestCase(unittest.TestCase):
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

class AsyncFailoverClientConnectTimeoutTestCase(AsyncFailoverClientBaseTestCase):
    protocols = [BlackHoleStompServer]

    def test_connection_timeout(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = StompFailoverClient(config, connectTimeout=0.01)
        return self.assertFailure(client.connect(), StompConnectTimeout)

    def test_connection_timeout_after_failover(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=2,initialReconnectDelay=0,randomize=false' % port)
        client = StompFailoverClient(config, connectTimeout=0.01)
        return self.assertFailure(client.connect(), StompConnectTimeout)
    
    @defer.inlineCallbacks
    def test_not_connected(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = StompFailoverClient(config, connectTimeout=0.01)
        try:
            yield client.send('/queue/fake')
        except StompConnectionError:
            pass
        
class AsyncFailoverClientConnectErrorTestCase(AsyncFailoverClientBaseTestCase):
    protocols = [ErrorOnConnectStompServer]

    def test_stomp_protocol_error_on_connect(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = StompFailoverClient(config)
        return self.assertFailure(client.connect(), StompProtocolError)

class AsyncFailoverClientErrorAfterConnectedTestCase(AsyncFailoverClientBaseTestCase):
    protocols = [ErrorOnSendStompServer]

    @defer.inlineCallbacks
    def test_disconnect_on_stomp_protocol_error(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=1,initialReconnectDelay=0,randomize=false' % port)
        client = StompFailoverClient(config)
        yield client.connect()
        client.send('/queue/fake', 'fake message')
        try:
            yield client.disconnected
        except StompProtocolError:
            pass
    
class AsyncFailoverClientFailoverOnDisconnectTestCase(AsyncFailoverClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer, ErrorOnSendStompServer]
    
    @defer.inlineCallbacks
    def test_failover_on_connection_lost(self):
        ports = tuple(c.getHost().port for c in self.connections)
        config = StompConfig(uri='failover:(tcp://localhost:%d,tcp://localhost:%d)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,randomize=false,maxReconnectAttempts=1' % ports)
        client = StompFailoverClient(config)
        
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

class AsyncFailoverClientReplaySubscription(AsyncFailoverClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer]
    
    @defer.inlineCallbacks
    def test_replay_after_failover(self):
        ports = tuple(c.getHost().port for c in self.connections)
        config = StompConfig(uri='failover:(tcp://localhost:%d)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,maxReconnectAttempts=1' % ports)
        client = StompFailoverClient(config)
        try:
            client.subscribe('/queue/bla', self._on_message) # client is not connected, so it won't accept subscriptions
        except StompConnectionError:
            pass
        else:
            self.assertTrue(False)
            
        self.assertEquals(client._session._subscriptions, {}) # check that no subscriptions have been accepted
        yield client.connect()
        
        self.shutdown = True # the callback handler will kill the broker connection ... 
        client.subscribe('/queue/bla', self._on_message)
        try:
            client = yield client.disconnected # the callback handler has killed the broker connection
        except StompConnectionError:
            pass
        else:
            self.assertFalse(True)
            
        self.shutdown = False # the callback handler will not kill the broker connection, but callback self._got_message
        self._got_message = defer.Deferred()
        
        yield client.connect()
        self.assertNotEquals(client._session._subscriptions, []) # the subscriptions have been replayed ...
        
        result = yield self._got_message
        self.assertEquals(result, None) # ... and the message comes back
        
        yield client.disconnect()
        self.assertEquals(list(client._session.replay()), []) # after a clean disconnect, the subscriptions are forgotten.

    def _on_message(self, client, msg):
        self.assertTrue(isinstance(client, StompFailoverClient))
        self.assertEquals(msg['body'], 'hi')
        if self.shutdown:
            client.send('/queue/fake', 'shutdown')
        else:
            self._got_message.callback(None)
        
if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
