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
from stompest.protocol.frame import StompFrame

logging.basicConfig(level=logging.DEBUG)

class SimpleStompIntegrationTest(unittest.TestCase):
    DESTINATION = '/queue/stompUnitTest'
    TIMEOUT = 0.25
    
    def test_0_cleanup(self):
        config = StompConfig(uri='tcp://localhost:61613')
        stomp = Stomp(config)
        stomp.connect()
        stomp.subscribe(self.DESTINATION, {StompSpec.ACK_HEADER: 'client'})
        stomp.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 'bla', StompSpec.ACK_HEADER: 'client'})
        while stomp.canRead(self.TIMEOUT):
            stomp.ack(stomp.receiveFrame())
        stomp.disconnect()

    def test_1_integration(self):
        config = StompConfig(uri='tcp://localhost:61613')
        stomp = Stomp(config)
        stomp.connect()
        
        stomp.send(self.DESTINATION, 'test message 1')
        stomp.send(self.DESTINATION, 'test message 2')
        self.assertFalse(stomp.canRead(self.TIMEOUT))
        stomp.subscribe(self.DESTINATION, {StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(stomp.canRead(self.TIMEOUT))
        stomp.ack(stomp.receiveFrame())
        self.assertTrue(stomp.canRead(self.TIMEOUT))
        stomp.ack(stomp.receiveFrame())
        self.assertFalse(stomp.canRead(self.TIMEOUT))
        
    def _test_2_transaction(self):
        config = StompConfig(uri='tcp://localhost:61613')
        stomp = Stomp(config)
        stomp.connect()
        
        with stomp.transaction(4711) as transaction:
            self.assertEquals(transaction, '4711')
            stomp.send(self.DESTINATION, 'test message 1')
            self.assertFalse(stomp.canRead(self.TIMEOUT))
        self.assertTrue(stomp.canRead(2 * self.TIMEOUT))
        stomp.ack(stomp.receiveFrame())
        
        with stomp.transaction(4711, receipt='4712') as transaction:
            self.assertEquals(transaction, '4711')
            self.assertEquals(stomp.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712'}))
            stomp.send(self.DESTINATION, 'test message 1')
            self.assertFalse(stomp.canRead(self.TIMEOUT))
        self.assertEquals(stomp.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712'}))
        stomp.ack(stomp.receiveFrame())
        
        try:
            with stomp.transaction(4711) as transaction:
                self.assertEquals(transaction, '4711')
                stomp.send(self.DESTINATION, 'test message 1')
                raise
        except:
            pass
        self.assertFalse(stomp.canRead(self.TIMEOUT))
        
        stomp.disconnect()

    def test_3_timeout(self):
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
        stomp.disconnect()
        
    def test_3_socket_failure_and_replay(self):
        stomp = Stomp(StompConfig(uri='tcp://localhost:61613', version='1.0'))
        stomp.connect()
        stomp.send(self.DESTINATION, 'test message 1')
        headers = {StompSpec.ACK_HEADER: 'client-individual'}
        token = stomp.subscribe(self.DESTINATION, headers)
        stomp.sendFrame(StompFrame('DISCONNECT')) # DISCONNECT frame is out-of-band, as far as the session is concerned -> unexpected disconnect
        self.assertRaises(StompConnectionError, stomp.receiveFrame)
        stomp.connect()
        stomp.ack(stomp.receiveFrame())
        stomp.unsubscribe(token)
        stomp.send(self.DESTINATION, 'test message 2')
        headers = {'id': 'bla', StompSpec.ACK_HEADER: 'client-individual'}
        stomp.subscribe(self.DESTINATION, headers)
        headers[StompSpec.DESTINATION_HEADER] = self.DESTINATION
        stomp.sendFrame(StompFrame('DISCONNECT')) # DISCONNECT frame is out-of-band, as far as the session is concerned -> unexpected disconnect
        self.assertRaises(StompConnectionError, stomp.receiveFrame)
        stomp.connect()
        stomp.ack(stomp.receiveFrame())
        stomp.unsubscribe(('id', 'bla'))
        stomp.disconnect()
        
    def test_4_integration_stomp_1_1(self):
        stomp = Stomp(StompConfig(uri='tcp://localhost:61613', version='1.1'))
        stomp.connect()
        if stomp.session.version == '1.0':
            print 'Broker localhost:61613 does not support STOMP protocol version 1.1'
            stomp.disconnect()
            return
        stomp.send(self.DESTINATION, 'test message 1')
        stomp.send(self.DESTINATION, 'test message 2')
        self.assertFalse(stomp.canRead(self.TIMEOUT))
        token = stomp.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 4711, StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(stomp.canRead(self.TIMEOUT))
        stomp.ack(stomp.receiveFrame())
        self.assertTrue(stomp.canRead(self.TIMEOUT))
        stomp.ack(stomp.receiveFrame())
        self.assertFalse(stomp.canRead(self.TIMEOUT))
        stomp.unsubscribe(token)
        stomp.send(self.DESTINATION, 'test message 3', receipt='4711')
        self.assertTrue(stomp.canRead(self.TIMEOUT))
        self.assertEquals(stomp.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4711'}))
        self.assertFalse(stomp.canRead(self.TIMEOUT))
        stomp.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 4711, StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(stomp.canRead(self.TIMEOUT))
        stomp.ack(stomp.receiveFrame())
        self.assertFalse(stomp.canRead(self.TIMEOUT))
        stomp.disconnect(receipt='4712')
        self.assertEquals(stomp.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712'}))
        self.assertTrue(stomp.canRead(self.TIMEOUT))
        self.assertRaises(StompConnectionError, stomp.receiveFrame)
        stomp.connect()
        stomp.disconnect(receipt='4711')
        self.assertEquals(stomp.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4711'}))
        self.assertTrue(stomp.canRead(self.TIMEOUT))
        stomp.close()
        self.assertRaises(StompConnectionError, stomp.canRead, self.TIMEOUT)
        
if __name__ == '__main__':
    unittest.main()
