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

from twisted.internet import defer
from twisted.python import log

from stompest.async import StompConfig, StompFailoverClient
from stompest.error import StompConnectTimeout, StompProtocolError
from stompest.tests.async_stomp_client_test import AsyncClientBaseTestCase
from stompest.tests.broker_simulator import BlackHoleStompServer, DisconnectOnSendStompServer, ErrorOnConnectStompServer, ErrorOnSendStompServer

observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig(level=logging.DEBUG)

class AsyncFailoverClientConnectTimeoutTestCase(AsyncClientBaseTestCase):
    protocol = BlackHoleStompServer

    def test_connection_timeout(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        client = StompFailoverClient(config, connectTimeout=0.01)
        return self.assertFailure(client.connect(), StompConnectTimeout)

    def test_connection_timeout_after_failover(self):
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=2,initialReconnectDelay=0,randomize=false' % self.testPort)
        client = StompFailoverClient(config, connectTimeout=0.01)
        return self.assertFailure(client.connect(), StompConnectTimeout)

class AsyncFailoverClientConnectErrorTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnConnectStompServer

    def test_stomp_error_before_connected(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        client = StompFailoverClient(config)
        return self.assertFailure(client.connect(), StompProtocolError)

class AsyncFailoverClientErrorAfterConnectedTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnSendStompServer

    def test_failover_stomp_error_after_connected(self):
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=1,initialReconnectDelay=0,randomize=false' % self.testPort)
        
        client = StompFailoverClient(config)
        deferred = defer.Deferred()
        self._connect_and_send(client, deferred)
        
        return self.assertFailure(deferred, StompProtocolError)
    
    @defer.inlineCallbacks
    def _connect_and_send(self, client, deferred):
        yield client.connect()
        client.getDisconnectedDeferred().chainDeferred(deferred)
        client.send('/queue/fake', 'fake message')

"""
class AsyncFailoverClientFailoverOnDisconnectTestCase(AsyncClientBaseTestCase):
    protocol = DisconnectOnSendStompServer

    def test_failover_stomp_failover_on_disconnect(self):
        config = StompConfig(uri='failover:(tcp://localhost:%d,tcp://nosuchhost:65535)?startupMaxReconnectAttempts=0,initialReconnectDelay=0,randomize=false,maxReconnectAttempts=1' % self.testPort)
        client = StompFailoverClient(config)
        d = client.getReconnectedDeferred().addCallback(self.onReconnect)
        self._connect_and_send(client)
        
        return d
        
    @defer.inlineCallbacks
    def _connect_and_send(self, client):
        yield client.connect()
        client.send('/queue/fake', 'fake message')

    def onReconnect(self, reason):
        print 20 * '$', reason
        reason.disconnect()
"""

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
