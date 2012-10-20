# -*- coding: iso-8859-1 -*-
"""
Copyright 2012 Mozes, Inc.

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
import time
import unittest

from stompest.error import StompConnectionError
from stompest.protocol.failover import StompConfig
from stompest.protocol.spec import StompSpec
from stompest.sync import Stomp

logging.basicConfig(level=logging.DEBUG)

class SimpleStompIntegrationTest(unittest.TestCase):
    DEST = '/queue/stompUnitTest'
    
    def test_0_integration(self):
        stomp = Stomp(StompConfig(uri='tcp://localhost:61613', login='', passcode=''))
        stomp.connect()
        stomp.subscribe({StompSpec.DESTINATION_HEADER: self.DEST, StompSpec.ACK_HEADER: 'client'})
        stomp.subscribe({StompSpec.DESTINATION_HEADER: self.DEST, StompSpec.ID_HEADER: 'bla', StompSpec.ACK_HEADER: 'client'})
        while stomp.canRead(1):
            stomp.ack(stomp.receiveFrame())
        stomp.disconnect()

    def test_1_integration(self):
        stomp = Stomp(StompConfig(uri='tcp://localhost:61613'))
        stomp.connect()
        stomp.send(self.DEST, 'test message 1')
        stomp.send(self.DEST, 'test message 2')
        self.assertFalse(stomp.canRead(1))
        stomp.subscribe({StompSpec.DESTINATION_HEADER: self.DEST, StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(stomp.canRead(1))
        stomp.ack(stomp.receiveFrame())
        self.assertTrue(stomp.canRead(1))
        stomp.ack(stomp.receiveFrame())
        self.assertFalse(stomp.canRead(1))
        stomp.disconnect()

    def test_2_timeout(self):
        tolerance = .01
        
        stomp = Stomp(StompConfig(uri='failover:(tcp://localhost:61614,tcp://localhost:61615)?startupMaxReconnectAttempts=2,backOffMultiplier=3'))
        expectedTimeout = time.time() + 40 / 1000.0 # 40 ms = 10 ms + 3 * 10 ms
        self.assertRaises(StompConnectionError, stomp.connect)
        self.assertTrue(abs(time.time() - expectedTimeout) < tolerance)
        
        stomp = Stomp(StompConfig(uri='failover:(tcp://localhost:61614,tcp://localhost:61613)?randomize=false')) # default is startupMaxReconnectAttempts = 0
        expectedTimeout = time.time() + 0
        self.assertRaises(StompConnectionError, stomp.connect)
        self.assertTrue(abs(time.time() - expectedTimeout) < tolerance)

        stomp = Stomp(StompConfig(uri='failover:(tcp://localhost:61614,tcp://localhost:61613)?startupMaxReconnectAttempts=1,randomize=false'))
        stomp.connect()
        stomp.connect()
        stomp.disconnect()
        
    def test_3_socket_failure_and_replay(self):
        stomp = Stomp(StompConfig(uri='tcp://localhost:61613'))
        stomp.connect()
        stomp.send(self.DEST, 'test message 1')
        headers = {StompSpec.DESTINATION_HEADER: self.DEST, StompSpec.ACK_HEADER: 'client-individual'}
        stomp.subscribe(headers)
        self.assertEqual(stomp._session._subscriptions, [(headers, None)])
        stomp._stomp.socket.close()
        self.assertRaises(StompConnectionError, stomp.receiveFrame)
        stomp.connect()
        stomp.ack(stomp.receiveFrame())
        stomp.unsubscribe({StompSpec.DESTINATION_HEADER: self.DEST})
        self.assertEqual(stomp._session._subscriptions, [])
        stomp.send(self.DEST, 'test message 2')
        headers = {StompSpec.DESTINATION_HEADER: self.DEST, 'id': 'bla', StompSpec.ACK_HEADER: 'client-individual'}
        stomp.subscribe(headers)
        self.assertEqual(stomp._session._subscriptions, [(headers, None)])
        stomp._stomp.socket.close()
        self.assertRaises(StompConnectionError, stomp.receiveFrame)
        stomp.connect()
        stomp.ack(stomp.receiveFrame())
        stomp.unsubscribe({'id': 'bla'})
        self.assertEqual(stomp._session._subscriptions, [])
        stomp.disconnect()
        
if __name__ == '__main__':
    unittest.main()