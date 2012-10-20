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

from stompest.async import StompConfig as DeprecatedStompConfig
from stompest.async import StompCreator 
from stompest.protocol import StompConfig
from stompest.error import StompConnectTimeout, StompProtocolError, StompConnectionError
from stompest.tests.broker_simulator import BlackHoleStompServer, ErrorOnConnectStompServer, ErrorOnSendStompServer, RemoteControlViaFrameStompServer

observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig(level=logging.DEBUG)

class AsyncClientBaseTestCase(unittest.TestCase):
    protocol = None

    def setUp(self):
        self.factory = Factory()
        self.factory.protocol = self.protocol
        self.port = reactor.listenTCP(0, self.factory)
        self.testPort = self.port.getHost().port

    def tearDown(self):
        self.port.stopListening()

class AsyncClientConnectTimeoutTestCase(AsyncClientBaseTestCase):
    protocol = BlackHoleStompServer

    def test_connection_timeout(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        creator = StompCreator(config, connectTimeout=0.01)
        return self.assertFailure(creator.getConnection(), StompConnectTimeout)

class AsyncClientConnectErrorTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnConnectStompServer

    def test_stomp_error_before_connected(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        creator = StompCreator(config)
        return self.assertFailure(creator.getConnection(), StompProtocolError)

class AsyncClientErrorAfterConnectedTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnSendStompServer

    def test_stomp_error_after_connected(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        creator = StompCreator(config)
        deferred = defer.Deferred()
        self._connect_and_send(creator, deferred)
        return self.assertFailure(deferred, StompProtocolError)
        
    def test_stomp_error_after_connected_deprecated_config(self):
        config = DeprecatedStompConfig('localhost', self.testPort)
        creator = StompCreator(config)
        deferred = defer.Deferred()
        self._connect_and_send(creator, deferred)
        return self.assertFailure(deferred, StompProtocolError)
        
    @defer.inlineCallbacks
    def _connect_and_send(self, creator, deferred):
        stomp = yield creator.getConnection()
        stomp.getDisconnectedDeferred().chainDeferred(deferred)
        stomp.send('/queue/fake', 'fake message')

class AsyncClientErrorAfterConnectionLostTestCase(AsyncClientBaseTestCase):
    protocol = RemoteControlViaFrameStompServer

    def test_stomp_error_after_connection_lost(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        creator = StompCreator(config)
        deferred = defer.Deferred()
        self._connect_and_send(creator, deferred)
        return self.assertFailure(deferred, StompConnectionError)
        
    @defer.inlineCallbacks
    def _connect_and_send(self, creator, deferred):
        stomp = yield creator.getConnection()
        stomp.getDisconnectedDeferred().chainDeferred(deferred)
        stomp.send('/queue/fake', 'shutdown')

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()

        
        
