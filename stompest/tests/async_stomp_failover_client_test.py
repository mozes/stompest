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
from stompest.tests.broker_simulator import BlackHoleStompServer, ErrorOnConnectStompServer, ErrorOnSendStompServer

observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig(level=logging.DEBUG)

class AsyncFailoverClientConnectTimeoutTestCase(AsyncClientBaseTestCase):
    protocol = BlackHoleStompServer

    def test_connection_timeout(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        client = StompFailoverClient(config, connectTimeout=0.01)
        return self.assertFailure(client.connect(), StompConnectTimeout)

"""
class AsyncFailoverClientConnectErrorTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnConnectStompServer

    def test_stomp_error_before_connected(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        client = StompFailoverClient(config)
        return self.assertFailure(client.connect(), StompProtocolError)
"""

class AsyncFailoverClientErrorAfterConnectedTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnSendStompServer

    def setUp(self):
        AsyncClientBaseTestCase.setUp(self)
        self.disconnected = defer.Deferred()

    def test_failover_stomp_error_after_connected(self):
        config = StompConfig(uri='failover:(tcp://nosuchhost:65535,tcp://localhost:%d)?startupMaxReconnectAttempts=1,initialReconnectDelay=0,randomize=false' % self.testPort)
        client = StompFailoverClient(config)
        client.connect().addCallback(self.onConnected)
        return self.assertFailure(self.disconnected, StompProtocolError)

    def onConnected(self, client):
        client.getDisconnectedDeferred().chainDeferred(self.disconnected)
        client.send('/queue/fake', 'fake message')

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
       
