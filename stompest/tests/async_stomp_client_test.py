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
import binascii
import logging
import mock

from twisted.internet import error, reactor, defer
from twisted.internet.protocol import Factory 
from twisted.python import log
from twisted.trial import unittest

from stompest.async import StompClient, StompConfig, StompCreator
from stompest.error import StompConnectTimeout, StompFrameError, StompProtocolError
from stompest.protocol import commands
from stompest.protocol.frame import StompFrame
from stompest.tests.broker_simulator import BlackHoleStompServer, ErrorOnConnectStompServer, ErrorOnSendStompServer

observer = log.PythonLoggingObserver()
observer.start()
logging.basicConfig(level=logging.DEBUG)

class AsyncStompClientTestCase(unittest.TestCase):
    
    def test_bad_host_tcp_error(self):
        config = StompConfig(uri='tcp://nosuchhost:61613')
        creator = StompCreator(config)
        return self.assertFailure(creator.getConnection(), StompConnectTimeout)

    def test_bad_port_tcp_error(self):
        config = StompConfig(uri='tcp://localhost:65535')
        creator = StompCreator(config)
        return self.assertFailure(creator.getConnection(), StompConnectTimeout)
        
    def test_dataReceived_multiple_messages(self):
        hdrs = {'foo': '1'}
        body = 'blah'
        frame = {'cmd': 'MESSAGE', 'headers': hdrs, 'body': body}
        frameBytes = 2* str(StompFrame(**frame))
        
        stomp = StompClient()
        stomp._handlers[frame['cmd']] = mock.Mock()
        
        stomp.dataReceived(frameBytes)
        
        self.assertEquals(2, stomp._handlers[frame['cmd']].call_count)
        stomp._handlers[frame['cmd']].assert_called_with({'cmd': frame['cmd'], 'headers': hdrs, 'body': body})

    def test_dataReceived_partial_message(self):
        hdrs = {'foo': '1'}
        body = 'blah'
        frame = {'cmd': 'MESSAGE', 'headers': hdrs, 'body': body}
        frameBytes = str(StompFrame(**frame))
        split = 8
        
        stomp = StompClient()
        stomp._handlers[frame['cmd']] = mock.Mock()
        
        stomp.dataReceived(frameBytes[:split])
        stomp.dataReceived(frameBytes[split:])
                
        self.assertEquals(1, stomp._handlers[frame['cmd']].call_count)
        stomp._handlers[frame['cmd']].assert_called_with({'cmd': frame['cmd'], 'headers': hdrs, 'body': body})
        
    def test_dataReceived_binary(self):
        body = binascii.a2b_hex('f0000a09')
        hdrs = {'foo': '1', 'content-length': str(len(body))}
        frame = {'cmd': 'MESSAGE', 'headers': hdrs, 'body': body}
        frameBytes = str(StompFrame(**frame))
        
        stomp = StompClient()
        stomp._handlers[frame['cmd']] = mock.Mock()
        
        stomp.dataReceived(frameBytes)
                
        self.assertEquals(1, stomp._handlers[frame['cmd']].call_count)
        stomp._handlers[frame['cmd']].assert_called_with({'cmd': frame['cmd'], 'headers': hdrs, 'body': body})
        
    def test_dataReceived_unexpected_exception(self):
        class MyError(Exception):
            pass
        
        stomp = StompClient()
        stomp._handlers['DISCONNECT'] = mock.Mock(side_effect=MyError)
        
        self.assertRaises(MyError, stomp.dataReceived, str(commands.disconnect()))

    def test_dataReceived_bad_command(self):
        self.assertRaises(StompFrameError, StompClient().dataReceived, 'BAD_CMD\n\n\x00\n')

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
        creator = StompCreator(config, connectTimeout=1)
        return self.assertFailure(creator.getConnection(), StompConnectTimeout)

class AsyncClientConnectErrorTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnConnectStompServer

    def test_stomp_error_before_connected(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
        creator = StompCreator(config)
        return self.assertFailure(creator.getConnection(), StompConnectTimeout)

class AsyncClientErrorAfterConnectedTestCase(AsyncClientBaseTestCase):
    protocol = ErrorOnSendStompServer

    def setUp(self):
        AsyncClientBaseTestCase.setUp(self)
        self.disconnected = defer.Deferred()

    def test_stomp_error_after_connected(self):
        config = StompConfig(uri='tcp://localhost:%d' % self.testPort)
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

    
        
        
        
