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

from stompest.async import StompConfig, StompFailoverClient
from stompest.error import StompConnectTimeout, StompProtocolError
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
        connection = reactor.listenTCP(0, factory)
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

class AsyncFailoverClientConnectErrorTestCase(AsyncFailoverClientBaseTestCase):
    protocols = [ErrorOnConnectStompServer]

    def test_stomp_error_before_connected(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='tcp://localhost:%d' % port)
        client = StompFailoverClient(config)
        return self.assertFailure(client.connect(), StompProtocolError)

class AsyncFailoverClientErrorAfterConnectedTestCase(AsyncFailoverClientBaseTestCase):
    protocols = [ErrorOnSendStompServer]

    def test_failover_stomp_error_after_connected(self):
        port = self.connections[0].getHost().port
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=1,initialReconnectDelay=0,randomize=false' % port)
        
        client = StompFailoverClient(config)
        deferred = defer.Deferred()
        self._connect_and_send(client, deferred)
        
        return self.assertFailure(deferred, StompProtocolError)
    
    @defer.inlineCallbacks
    def _connect_and_send(self, client, deferred):
        yield client.connect()
        client.getDisconnectedDeferred().chainDeferred(deferred)
        client.send('/queue/fake', 'fake message')

class AsyncFailoverClientFailoverOnDisconnectTestCase(AsyncFailoverClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer, ErrorOnSendStompServer]
    
    def test_failover_stomp_failover_on_disconnect(self):
        ports = tuple(c.getHost().port for c in self.connections)
        config = StompConfig(uri='failover:(tcp://localhost:%d,tcp://localhost:%d)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,randomize=false,maxReconnectAttempts=1' % ports)
        client = StompFailoverClient(config)
        deferred = defer.Deferred()
        self._connect_and_send(client, deferred)
        
        return self.assertFailure(deferred, StompProtocolError)
    
    @defer.inlineCallbacks
    def _connect_and_send(self, client, deferred):
        yield client.connect()
        client.getReconnectedDeferred().addCallback(self._on_reconnect, deferred)
        yield self.connections[0].stopListening()
        client.send('/queue/fake', 'disconnect')
    
    def _on_reconnect(self, client, deferred):
        client.getDisconnectedDeferred().chainDeferred(deferred)
        client.send('/queue/fake', 'fake message')

"""
class AsyncFailoverClientReplaySubscription(AsyncFailoverClientBaseTestCase):
    protocols = [RemoteControlViaFrameStompServer]
    
    def test_failover_stomp_failover_on_disconnect(self):
        ports = tuple(c.getHost().port for c in self.connections)
        config = StompConfig(uri='failover:(tcp://localhost:%d)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,maxReconnectAttempts=1' % ports)
        client = StompFailoverClient(config)
        deferred = defer.Deferred()
        self._connect_and_send(client, deferred)
        
        return deferred.addCallback(self.assertTrue, deferred.called)
    
    @defer.inlineCallbacks
    def _connect_and_send(self, client, deferred):
        yield client.connect()
        client.getReconnectedDeferred().addCallback(self._on_reconnect, deferred)
        client.subscribe('/queue/bla', self._on_message)
        client.send('/queue/fake', 'disconnect')
    
    def _on_reconnect(self, client, deferred):
        client.getDisconnectedDeferred().chainDeferred(deferred)
        client.disconnect()
        
    def _on_message(self, *args, **kwargs):
        print '!!!!!!!!!', args, kwargs
"""

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
