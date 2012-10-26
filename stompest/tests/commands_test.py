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
import unittest

from stompest.error import StompProtocolError
from stompest.protocol import commands, StompSpec, StompFrame
   
class CommandsTest(unittest.TestCase):
    def test_connect(self):
        self.assertRaises(StompProtocolError, commands.connect)
        self.assertRaises(StompProtocolError, lambda: commands.connect(login='hi'))
        self.assertRaises(StompProtocolError, lambda: commands.connect(passcode='there'))
        self.assertEquals(commands.connect('hi', 'there'), StompFrame(StompSpec.CONNECT, headers={StompSpec.LOGIN_HEADER: 'hi', StompSpec.PASSCODE_HEADER: 'there'}))
        self.assertEquals(commands.connect('hi', 'there', {'4711': '0815'}), StompFrame(StompSpec.CONNECT, headers={StompSpec.LOGIN_HEADER: 'hi', StompSpec.PASSCODE_HEADER: 'there', '4711': '0815'}))
        
        self.assertEquals(commands.connect('hi', 'there', versions=[StompSpec.VERSION_1_0]), StompFrame(StompSpec.CONNECT, headers={StompSpec.LOGIN_HEADER: 'hi', StompSpec.PASSCODE_HEADER: 'there'}))
        self.assertRaises(StompProtocolError, lambda: commands.connect(versions=[StompSpec.VERSION_1_0]))
        
        frame = commands.connect(versions=[StompSpec.VERSION_1_0, StompSpec.VERSION_1_1])
        frame.headers.pop(StompSpec.HOST_HEADER)
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={StompSpec.ACCEPT_VERSION_HEADER: '1.0,1.1'}))
        
        frame = commands.connect(versions=[StompSpec.VERSION_1_1])
        frame.headers.pop(StompSpec.HOST_HEADER)
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={StompSpec.ACCEPT_VERSION_HEADER: StompSpec.VERSION_1_1}))

        frame = commands.connect(versions=[StompSpec.VERSION_1_1], login='hi', passcode='there', host='earth')
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={'accept-version': '1.1', 'login': 'hi', 'passcode': 'there', 'host': 'earth'}))

        frame = commands.connect(versions=[StompSpec.VERSION_1_1], host='earth', headers={'4711': '0815', StompSpec.HEART_BEAT_HEADER: '1,2'})
        self.assertEquals(frame, StompFrame(StompSpec.CONNECT, headers={'accept-version': '1.1', '4711': '0815', 'host': 'earth', 'heart-beat': '1,2'}))
        stompFrame = commands.stomp([StompSpec.VERSION_1_1], 'earth', 'hi', 'there', {'4711': '0815'})
        self.assertEquals(stompFrame.cmd, StompSpec.STOMP)
        self.assertEquals(stompFrame.headers, commands.connect('hi', 'there', {'4711': '0815'}, [StompSpec.VERSION_1_1], 'earth').headers)
        self.assertRaises(StompProtocolError, lambda: commands.stomp([StompSpec.VERSION_1_0], 'earth', 'hi', 'there', {'4711': '0815'}))
        self.assertRaises(StompProtocolError, lambda: commands.stomp(None, 'earth', 'hi', 'there', {'4711': '0815'}))

    def test_disconnect(self):
        self.assertEquals(commands.disconnect(), StompFrame(StompSpec.DISCONNECT))
        self.assertRaises(StompProtocolError, lambda: commands.disconnect(receipt=4711))
        self.assertEquals(commands.disconnect(receipt=4711, version=StompSpec.VERSION_1_1), StompFrame(StompSpec.DISCONNECT, {StompSpec.RECEIPT_HEADER: 4711}))
        
    def test_connected(self):
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'})), ('1.0', None, 'hi'))
        self.assertRaises(StompProtocolError, lambda: commands.connected(StompFrame(StompSpec.CONNECTED, {})))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'}), version='1.1'), ('1.0', None, 'hi'))
        self.assertRaises(StompProtocolError, lambda: commands.connected(StompFrame(StompSpec.MESSAGE, {}), version='1.0'))
        self.assertRaises(StompProtocolError, lambda: commands.connected(StompFrame(StompSpec.MESSAGE, {}), version='1.1'))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi', StompSpec.VERSION_HEADER: '1.1'}), version='1.1'), ('1.1', None, 'hi'))
        self.assertEquals(commands.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SERVER_HEADER: 'moon', StompSpec.VERSION_HEADER: '1.1'}), version='1.1'), ('1.1', 'moon', None))
        
    def test_ack(self):
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi'})), StompFrame(cmd='ACK', headers={'message-id': 'hi'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there'})), StompFrame(cmd='ACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'})), StompFrame(cmd='ACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', '4711': '0815', StompSpec.TRANSACTION_HEADER: 'man'})), StompFrame(cmd='ACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), version='1.1'), StompFrame(cmd='ACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertEquals(commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', '4711': '0815', StompSpec.TRANSACTION_HEADER: 'man'}), version='1.1'), StompFrame(cmd='ACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertRaises(StompProtocolError, lambda: commands.ack(StompFrame(StompSpec.CONNECTED, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), version='1.0'))
        self.assertRaises(StompProtocolError, lambda: commands.ack(StompFrame(StompSpec.CONNECTED, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), version='1.1'))
        self.assertRaises(StompProtocolError, lambda: commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.SUBSCRIPTION_HEADER: 'hi'})))
        self.assertRaises(StompProtocolError, lambda: commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.SUBSCRIPTION_HEADER: 'hi'}), version='1.1'))
        self.assertRaises(StompProtocolError, lambda: commands.ack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), version='1.1'))

    def _test_nack(self):
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there'})), StompFrame(cmd='NACK', headers={'message-id': 'hi', 'subscription': 'there'}))
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'})), StompFrame(cmd='NACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertEquals(commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', '4711': '0815', StompSpec.TRANSACTION_HEADER: 'man'})), StompFrame(cmd='NACK', headers={'message-id': 'hi', 'subscription': 'there', 'transaction': 'man'}))
        self.assertRaises(StompProtocolError, lambda: commands.nack(StompFrame(StompSpec.CONNECTED, {}), version='1.1'))
        self.assertRaises(StompProtocolError, lambda: commands.nack(StompFrame(StompSpec.CONNECTED, {}), version='1.0'))
        self.assertRaises(StompProtocolError, lambda: commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi', StompSpec.SUBSCRIPTION_HEADER: 'there', StompSpec.TRANSACTION_HEADER: 'man'}), version='1.0'))
        self.assertRaises(StompProtocolError, lambda: commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.SUBSCRIPTION_HEADER: 'hi'}), version='1.1'))
        self.assertRaises(StompProtocolError, lambda: commands.nack(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: 'hi'}), version='1.1'))

if __name__ == '__main__':
    unittest.main()