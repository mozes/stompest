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
import unittest

from stompest.config import StompConfig
from stompest.error import StompConnectionError
from stompest.protocol.frame import StompFrame, StompSpec
from stompest.sync import Stomp

logging.basicConfig(level=logging.DEBUG)
LOG_CATEGORY = __name__

class SimpleStompIntegrationTest(unittest.TestCase):
    DESTINATION = '/queue/stompUnitTest'
    TIMEOUT = 0.1
    log = logging.getLogger(LOG_CATEGORY)

    def setUp(self):
        config = StompConfig(uri='tcp://localhost:61613')
        client = Stomp(config)
        client.connect()
        client.subscribe(self.DESTINATION, {StompSpec.ACK_HEADER: 'auto'})
        client.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 'bla', StompSpec.ACK_HEADER: 'auto'})
        while client.canRead(self.TIMEOUT):
            frame = client.receiveFrame()
            self.log.debug('Dequeued old %s' % frame.info())
        client.disconnect()

    def test_1_integration(self):
        config = StompConfig(uri='tcp://localhost:61613')
        client = Stomp(config)
        client.connect()

        client.send(self.DESTINATION, 'test message 1')
        client.send(self.DESTINATION, 'test message 2')
        self.assertFalse(client.canRead(self.TIMEOUT))
        client.subscribe(self.DESTINATION, {StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertFalse(client.canRead(self.TIMEOUT))

    def test_2_transaction(self):
        config = StompConfig(uri='tcp://localhost:61613')
        client = Stomp(config)
        client.connect()
        client.subscribe(self.DESTINATION, {StompSpec.ACK_HEADER: 'client-individual'})
        self.assertFalse(client.canRead(self.TIMEOUT))

        with client.transaction(4711) as transaction:
            self.assertEquals(transaction, '4711')
            client.send(self.DESTINATION, 'test message', {StompSpec.TRANSACTION_HEADER: transaction})
            self.assertFalse(client.canRead(0))
        self.assertTrue(client.canRead(self.TIMEOUT))
        frame = client.receiveFrame()
        self.assertEquals(frame.body, 'test message')
        client.ack(frame)

        with client.transaction(4711, receipt='4712') as transaction:
            self.assertEquals(transaction, '4711')
            client.send(self.DESTINATION, 'test message', {StompSpec.TRANSACTION_HEADER: transaction})
            self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712-begin'}))
            client.send(self.DESTINATION, 'test message without transaction')
            self.assertTrue(client.canRead(self.TIMEOUT))
            frame = client.receiveFrame()
            self.assertEquals(frame.body, 'test message without transaction')
            client.ack(frame)
            self.assertFalse(client.canRead(0))
        self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712-commit'}))
        frame = client.receiveFrame()
        self.assertEquals(frame.body, 'test message')
        client.ack(frame)

        try:
            with client.transaction(4711) as transaction:
                self.assertEquals(transaction, '4711')
                client.send(self.DESTINATION, 'test message', {StompSpec.TRANSACTION_HEADER: transaction})
                raise RuntimeError('poof')
        except RuntimeError as e:
            self.assertEquals(str(e), 'poof')
        else:
            raise
        self.assertFalse(client.canRead(self.TIMEOUT))

        client.disconnect()

    def test_3_timeout(self):
        timeout = 0.2
        client = Stomp(StompConfig(uri='failover:(tcp://localhost:61614,tcp://localhost:61613)?startupMaxReconnectAttempts=1,randomize=false'))
        client.connect(connectTimeout=timeout)
        client.disconnect()

        client = Stomp(StompConfig(uri='failover:(tcp://localhost:61614,tcp://localhost:61615)?startupMaxReconnectAttempts=1,backOffMultiplier=3'))
        self.assertRaises(StompConnectionError, client.connect, connectTimeout=timeout)

        client = Stomp(StompConfig(uri='failover:(tcp://localhost:61614,tcp://localhost:61613)?randomize=false')) # default is startupMaxReconnectAttempts = 0
        self.assertRaises(StompConnectionError, client.connect, connectTimeout=timeout)

    def test_3_socket_failure_and_replay(self):
        client = Stomp(StompConfig(uri='tcp://localhost:61613', version='1.0'))
        client.connect()
        headers = {StompSpec.ACK_HEADER: 'client-individual'}
        token = client.subscribe(self.DESTINATION, headers)
        client.sendFrame(StompFrame('DISCONNECT')) # DISCONNECT frame is out-of-band, as far as the session is concerned -> unexpected disconnect
        self.assertRaises(StompConnectionError, client.receiveFrame)
        client.connect()
        client.send(self.DESTINATION, 'test message 1')
        client.ack(client.receiveFrame())
        client.unsubscribe(token)
        headers = {'id': 'bla', StompSpec.ACK_HEADER: 'client-individual'}
        client.subscribe(self.DESTINATION, headers)
        headers[StompSpec.DESTINATION_HEADER] = self.DESTINATION
        client.sendFrame(StompFrame('DISCONNECT')) # DISCONNECT frame is out-of-band, as far as the session is concerned -> unexpected disconnect
        self.assertRaises(StompConnectionError, client.receiveFrame)
        client.connect()
        client.send(self.DESTINATION, 'test message 2')
        client.ack(client.receiveFrame())
        client.unsubscribe(('id', 'bla'))
        client.disconnect()

    def test_4_integration_stomp_1_1(self):
        client = Stomp(StompConfig(uri='tcp://localhost:61613', version='1.1'))
        client.connect()
        if client.session.version == '1.0':
            print 'Broker localhost:61613 does not support STOMP protocol version 1.1'
            client.disconnect()
            return
        client.send(self.DESTINATION, 'test message 1')
        client.send(self.DESTINATION, 'test message 2')
        self.assertFalse(client.canRead(self.TIMEOUT))
        token = client.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 4711, StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertFalse(client.canRead(self.TIMEOUT))
        client.unsubscribe(token)
        client.send(self.DESTINATION, 'test message 3', receipt='4711')
        self.assertTrue(client.canRead(self.TIMEOUT))
        self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4711'}))
        self.assertFalse(client.canRead(self.TIMEOUT))
        client.subscribe(self.DESTINATION, {StompSpec.ID_HEADER: 4711, StompSpec.ACK_HEADER: 'client-individual'})
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.ack(client.receiveFrame())
        self.assertFalse(client.canRead(self.TIMEOUT))
        client.disconnect(receipt='4712')
        self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4712'}))
        self.assertTrue(client.canRead(self.TIMEOUT))
        self.assertRaises(StompConnectionError, client.receiveFrame)
        client.connect()
        client.disconnect(receipt='4711')
        self.assertEquals(client.receiveFrame(), StompFrame(StompSpec.RECEIPT, {'receipt-id': '4711'}))
        self.assertTrue(client.canRead(self.TIMEOUT))
        client.close()
        self.assertRaises(StompConnectionError, client.canRead, self.TIMEOUT)

if __name__ == '__main__':
    unittest.main()
