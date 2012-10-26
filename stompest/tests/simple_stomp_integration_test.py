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
import time
import unittest

from stompest.error import StompProtocolError
from stompest.protocol.spec import StompSpec
from stompest.simple import Stomp

class SimpleStompIntegrationTest(unittest.TestCase):
    DEST = '/queue/stompUnitTest'
    
    def setUp(self):
        stomp = Stomp('localhost', 61613)
        stomp.connect()
        stomp.subscribe(self.DEST, {StompSpec.ACK_HEADER: 'client'})
        while (stomp.canRead(1)):
            stomp.ack(stomp.receiveFrame())
        
    def test_integration_1(self):
        stomp = Stomp('localhost', 61613)
        stomp.connect()
        stomp.send(self.DEST, 'test message1')
        stomp.send(self.DEST, 'test message2')
        self.assertFalse(stomp.canRead(1))
        stomp.subscribe(self.DEST, {StompSpec.ACK_HEADER: 'client'})
        self.assertTrue(stomp.canRead(1))
        frame = stomp.receiveFrame()
        stomp.ack(frame)
        self.assertTrue(stomp.canRead(1))
        frame = stomp.receiveFrame()
        stomp.ack(frame)
        self.assertFalse(stomp.canRead(1))
        stomp.disconnect()

    def test_integration_2(self):
        stomp = Stomp('localhost', 61613)
        stomp.connect()
        stomp.send(self.DEST, 'test message1')
        stomp.send(self.DEST, 'test message2')
        stomp.send(self.DEST, 'test message3')
        self.assertFalse(stomp.canRead(0))
        stomp.subscribe(self.DEST, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 2})
        time.sleep(1)
        self.assertTrue(stomp.canRead(0))
        frame1 = stomp.receiveFrame()
        self.assertTrue(stomp.canRead(0))
        frame2 = stomp.receiveFrame()
        self.assertFalse(stomp.canRead(0))
        stomp.ack(frame2)
        self.assertTrue(stomp.canRead(1))
        frame3 = stomp.receiveFrame()
        self.assertFalse(stomp.canRead(0))
        stomp.ack(frame3)
        stomp.ack(frame1)
        self.assertFalse(stomp.canRead(0))
        stomp.disconnect()

    def test_integration_stomp_version_1_1(self):
        stomp = Stomp('localhost', 61613, version='1.1')
        try:
            stomp.connect()
        except StompProtocolError as e:
            self.assertEquals(str(e), 'Incompatible server version: 1.0 [client version: 1.1]')
            return
                
        stomp.send(self.DEST, 'test message1')
        stomp.send(self.DEST, 'test message2')
        self.assertFalse(stomp.canRead(0))
        stomp.subscribe(self.DEST, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1, 'id': '4711'})
        time.sleep(1)
        self.assertTrue(stomp.canRead(0))
        frame1 = stomp.receiveFrame()
        self.assertFalse(stomp.canRead(0))
        stomp.nack(frame1)
        self.assertTrue(stomp.canRead(1))
        frame2 = stomp.receiveFrame()
        self.assertFalse(stomp.canRead(0))
        stomp.ack(frame2)
        stomp.disconnect()

if __name__ == '__main__':
    unittest.main()