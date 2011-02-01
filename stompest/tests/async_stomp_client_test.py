"""
Copyright 2011 Mozes, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import logging
import stomper
from twisted.trial import unittest
from stompest.async import StompConfig, StompCreator, StompClient
from stompest.error import StompProtocolError, StompConnectTimeout
from stompest.tests.broker_simulator import BlackHoleStompServer, ErrorOnConnectStompServer, ErrorOnSendStompServer
from twisted.internet import error, reactor, defer
from twisted.internet.protocol import Factory 
from twisted.python import log

observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig(level=logging.DEBUG)

class AsyncStompClientTestCase(unittest.TestCase):
    
    def test_bad_host_tcp_error(self):
        config = StompConfig('nosuchhost', 61613)
        creator = StompCreator(config)
        return self.assertFailure(creator.getConnection(), error.DNSLookupError)

    def test_bad_port_tcp_error(self):
        config = StompConfig('localhost', 987654321)
        creator = StompCreator(config)
        return self.assertFailure(creator.getConnection(), error.ConnectionRefusedError)
          
    def feedDataToStomp(self, stomp, data):
        for line in data.split('\n')[:-1]:
            stomp.lineReceived(line)
    
    def getFrame(self, cmd, headers, body):
        sFrame = stomper.Frame()
        sFrame.cmd = cmd
        sFrame.headers = headers
        sFrame.body = body
        return sFrame.pack()

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
        config = StompConfig('localhost', self.testPort)
        creator = StompCreator(config, connectTimeout=1)
        return self.assertFailure(creator.getConnection(), StompConnectTimeout)

class AsyncClientConnectErrorTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnConnectStompServer

    def test_stomp_error_before_connected(self):
        config = StompConfig('localhost', self.testPort)
        creator = StompCreator(config)
        return self.assertFailure(creator.getConnection(), StompProtocolError)

class AsyncClientErrorAfterConnectedTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnSendStompServer

    def setUp(self):
        AsyncClientBaseTestCase.setUp(self)
        self.disconnected = defer.Deferred()

    def test_stomp_error_after_connected(self):
        config = StompConfig('localhost', self.testPort)
        creator = StompCreator(config)
        creator.getConnection().addCallback(self.onConnected)
        return self.assertFailure(self.disconnected, StompProtocolError)

    def onConnected(self, stomp):
        stomp.getDisconnectedDeferred().chainDeferred(self.disconnected)
        stomp.send('/queue/fake', 'fake message')

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()

    
        
        
        
