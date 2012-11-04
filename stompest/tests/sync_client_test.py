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
import unittest

from mock import Mock

from stompest.error import StompConnectionError, StompProtocolError
from stompest.protocol import StompConfig, StompFrame, StompSpec, commands
from stompest.sync import Stomp

logging.basicConfig(level=logging.DEBUG)

HOST = 'fakeHost'
PORT = 61613

CONFIG = StompConfig('tcp://%s:%s' % (HOST, PORT), check=False)

class SimpleStompTest(unittest.TestCase):
    def _get_transport_mock(self, receive=None, config=None):
        stomp = Stomp(config or CONFIG)
        stomp._transport = Mock()
        if receive:
            stomp._transport.receive.return_value = receive
        return stomp
    
    def _get_connect_mock(self, receive=None, config=None):
        stomp = Stomp(config or CONFIG)
        stomp.factory = Mock()
        transport = stomp.factory.return_value = Mock()
        transport.host = 'mock'
        transport.port = 0
        if receive:
            transport.receive.return_value = receive
        return stomp
        
    def test_receiveFrame(self):
        frame_ = StompFrame('MESSAGE', {'x': 'y'}, 'testing 1 2 3')        
        stomp = self._get_transport_mock(frame_)
        frame = stomp.receiveFrame()
        self.assertEquals(frame_, frame)
        self.assertEquals(1, stomp._transport.receive.call_count)

    def test_canRead_raises_exception_before_connect(self):
        stomp = Stomp(CONFIG)
        self.assertRaises(Exception, stomp.canRead)

    def test_send_raises_exception_before_connect(self):
        stomp = Stomp(CONFIG)
        self.assertRaises(StompConnectionError, stomp.send, '/queue/foo', 'test message')

    def test_subscribe_raises_exception_before_connect(self):
        stomp = Stomp(CONFIG)
        self.assertRaises(Exception, stomp.subscribe, '/queue/foo')
    
    def test_disconnect_raises_exception_before_connect(self):
        stomp = Stomp(CONFIG)
        self.assertRaises(Exception, stomp.disconnect)
    
    def test_connect_raises_exception_for_bad_host(self):
        stomp = Stomp(StompConfig('tcp://nosuchhost:2345'))
        self.assertRaises(Exception, stomp.connect)
        
    def test_error_frame_after_connect_raises_StompProtocolError(self):
        stomp = self._get_connect_mock(StompFrame('ERROR', body='fake error'))
        self.assertRaises(StompProtocolError, stomp.connect)
        self.assertEquals(stomp._transport.receive.call_count, 1)
    
    def test_connect_when_connected_raises_StompConnectionError(self):
        stomp = self._get_transport_mock()
        self.assertRaises(StompConnectionError, stomp.connect)
    
    def test_connect_writes_correct_frame(self):
        login = 'curious'
        passcode = 'george'
        stomp = self._get_connect_mock(StompFrame('CONNECTED', {StompSpec.SESSION_HEADER: '4711'}))
        stomp._config.login = login
        stomp._config.passcode = passcode
        stomp.connect()
        args, _ = stomp._transport.send.call_args
        sentFrame = args[0]
        self.assertEquals(StompFrame('CONNECT', {'login': login, 'passcode': passcode}), sentFrame)
    
    def _test_send_writes_correct_frame(self):
        destination = '/queue/foo'
        message = 'test message'
        headers = {'foo': 'bar', 'fuzz': 'ball'}
        stomp = self._get_send_mock()
        stomp.send(destination, message, headers)
        args, _ = stomp._transport.send.call_args
        sentFrame = args[0]
        self.assertEquals(StompFrame('SEND', {StompSpec.DESTINATION_HEADER: destination, 'foo': 'bar', 'fuzz': 'ball'}, message), sentFrame)

    def test_subscribe_writes_correct_frame(self):
        destination = '/queue/foo'
        headers = {'foo': 'bar', 'fuzz': 'ball'}
        stomp = self._get_transport_mock()
        stomp.subscribe(destination, headers)
        args, _ = stomp._transport.send.call_args
        sentFrame = args[0]
        self.assertEquals(StompFrame('SUBSCRIBE', {StompSpec.DESTINATION_HEADER: destination, 'foo': 'bar', 'fuzz': 'ball'}, ''), sentFrame)

    def test_subscribe_matching_and_corner_cases(self):
        destination = '/queue/foo'
        headers = {'foo': 'bar', 'fuzz': 'ball'}
        stomp = self._get_transport_mock()
        token = stomp.subscribe(destination, headers)
        self.assertEquals(token, (StompSpec.DESTINATION_HEADER, destination))
        self.assertEquals(stomp.message(StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: '4711', StompSpec.DESTINATION_HEADER: destination})), token)
        self.assertRaises(StompProtocolError, stomp.message, StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: '4711', StompSpec.DESTINATION_HEADER: 'unknown'}))
        self.assertRaises(StompProtocolError, stomp.message, StompFrame(StompSpec.MESSAGE, {StompSpec.DESTINATION_HEADER: destination}))

    def test_stomp_version_1_1(self):
        destination = '/queue/foo'
        stomp = self._get_transport_mock(config=StompConfig('tcp://%s:%s' % (HOST, PORT), version='1.1', check=False))
        stomp._transport = Mock()
        frame = StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: '4711', StompSpec.DESTINATION_HEADER: destination})
        self.assertRaises(StompProtocolError, stomp.nack, frame)
        frame = StompFrame(StompSpec.MESSAGE, {StompSpec.MESSAGE_ID_HEADER: '4711', StompSpec.DESTINATION_HEADER: destination, StompSpec.SUBSCRIPTION_HEADER: '0815'})
        stomp.nack(frame, receipt='123')
        args, _ = stomp._transport.send.call_args
        sentFrame = args[0]
        self.assertEquals(commands.nack(frame, '123', '1.1'), sentFrame)

    def test_ack_writes_correct_frame(self):
        id_ = '12345'
        stomp = self._get_transport_mock()
        stomp.ack(StompFrame('MESSAGE', {StompSpec.MESSAGE_ID_HEADER: id_}, 'blah'))
        args, _ = stomp._transport.send.call_args
        sentFrame = args[0]
        self.assertEquals(StompFrame('ACK', {StompSpec.MESSAGE_ID_HEADER: id_}), sentFrame)

    def test_transaction_writes_correct_frames(self):
        transaction = '4711'
        stomp = self._get_transport_mock()
        for (method, command) in [
            (stomp.begin, 'BEGIN'), (stomp.commit, 'COMMIT'),
            (stomp.begin, 'BEGIN'), (stomp.abort, 'ABORT')
        ]:
            method(transaction)
            args, _ = stomp._transport.send.call_args
            sentFrame = args[0]
            self.assertEquals(StompFrame(command, {'transaction': transaction}), sentFrame)
            
        with stomp.transaction(transaction):
            args, _ = stomp._transport.send.call_args
            sentFrame = args[0]
            self.assertEquals(StompFrame('BEGIN', {'transaction': transaction}), sentFrame)
            
        args, _ = stomp._transport.send.call_args
        sentFrame = args[0]
        self.assertEquals(StompFrame('COMMIT', {'transaction': transaction}), sentFrame)
            
        try:
            with stomp.transaction(transaction):
                raise
        except:
            args, _ = stomp._transport.send.call_args
            sentFrame = args[0]
            self.assertEquals(StompFrame('ABORT', {'transaction': transaction}), sentFrame)

if __name__ == '__main__':
    unittest.main()