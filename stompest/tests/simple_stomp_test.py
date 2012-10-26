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
import unittest

from mock import Mock

from stompest.error import StompProtocolError
from stompest.protocol.frame import StompFrame
from stompest.protocol.parser import StompParser
from stompest.protocol.spec import StompSpec
from stompest.simple import Stomp

HOST = 'fakeHost'
PORT = 61613

class SimpleStompTest(unittest.TestCase):
    def _gen_bytes(self, bytes_):
        for byte in bytes_:
            yield byte
        while True:
            yield ''
        
    def _recv(self, iterator, size):
        result = []
        for _ in range(size):
            result.append(iterator.next())
        return ''.join(result)
            
    def _get_receive_mock(self, bytes_):
        stomp = Stomp(HOST, PORT)
        stomp._checkConnected = Mock()
        stomp.socket = Mock()
        bIter = self._gen_bytes(bytes_).__iter__()
        stomp.socket.recv = Mock(wraps=lambda size, *args: self._recv(bIter, size))
        return stomp
        
    def test_receiveFrame(self):
        hdrs = {'x': 'y'}
        body = 'testing 1 2 3'
        frameBytes = self.getFrame('MESSAGE', hdrs, body)
        self.assertTrue(frameBytes.endswith('\x00'))
        
        stomp = self._get_receive_mock(frameBytes)
        frame = stomp.receiveFrame()
        self.assertEquals('MESSAGE', frame.cmd)
        self.assertEquals(hdrs, frame.headers)
        self.assertEquals(body, frame.body)
        
        self.assertEquals(1, stomp.socket.recv.call_count)

    def test_receiveFrame_no_newline(self):
        hdrs = {'x': 'y'}
        body = 'testing 1 2 3'
        frameBytes = self.getFrame('MESSAGE', hdrs, body)
        self.assertTrue(frameBytes.endswith('\x00'))
        
        stomp = self._get_receive_mock(frameBytes)
        frame = stomp.receiveFrame()
        self.assertEquals('MESSAGE', frame.cmd)
        self.assertEquals(hdrs, frame.headers)
        self.assertEquals(body, frame.body)
        
        self.assertEquals(1, stomp.socket.recv.call_count)

    def test_receiveFrame_binary(self):
        body = binascii.a2b_hex('f0000a09')
        hdrs = {'content-length': str(len(body))}
        frameBytes = self.getFrame('MESSAGE', hdrs, body)
        
        stomp = self._get_receive_mock(frameBytes)
        frame = stomp.receiveFrame()
        self.assertEquals('MESSAGE', frame.cmd)
        self.assertEquals(hdrs, frame.headers)
        self.assertEquals(body, frame.body)
        
        self.assertEquals(1, stomp.socket.recv.call_count)
        
    def test_receiveFrame_multiple_frames_per_read(self):
        body1 = 'boo'
        body2 = 'hoo'
        hdrs = {'x': 'y'}
        frameBytes = self.getFrame('MESSAGE', hdrs, body1) + self.getFrame('MESSAGE', hdrs, body2)

        stomp = self._get_receive_mock(frameBytes)
        
        #Read first frame
        frame = stomp.receiveFrame()
        self.assertEquals('MESSAGE', frame.cmd)
        self.assertEquals(hdrs, frame.headers)
        self.assertEquals(body1, frame.body)
        self.assertEquals(1, stomp.socket.recv.call_count)

        #Read next frame
        frame = stomp.receiveFrame()
        self.assertEquals('MESSAGE', frame.cmd)
        self.assertEquals(hdrs, frame.headers)
        self.assertEquals(body2, frame.body)
        self.assertEquals(1, stomp.socket.recv.call_count)
    
    def test_canRead_raises_exception_before_connect(self):
        stomp = Stomp(HOST, PORT)
        self.assertRaises(Exception, lambda: stomp.canRead())

    def test_send_raises_exception_before_connect(self):
        stomp = Stomp(HOST, PORT)
        self.assertRaises(Exception, lambda: stomp.send('/queue/foo', 'test message'))

    def test_subscribe_raises_exception_before_connect(self):
        stomp = Stomp(HOST, PORT)
        self.assertRaises(Exception, lambda: stomp.subscribe('/queue/foo'))
    
    def test_disconnect_raises_exception_before_connect(self):
        stomp = Stomp(HOST, PORT)
        self.assertRaises(Exception, lambda: stomp.disconnect())
    
    def test_connect_raises_exception_for_bad_host(self):
        stomp = Stomp('nosuchhost', 2345)
        self.assertRaises(Exception, lambda: stomp.connect())

    def test_error_frame_after_connect_raises_StompProtocolError(self):
        stomp = Stomp(HOST, PORT)
        stomp._socketConnect = Mock()
        stomp.receiveFrame = Mock()
        stomp.receiveFrame.return_value = StompFrame('ERROR', body='fake error')
        stomp.socket = Mock()
        self.assertRaises(StompProtocolError, lambda: stomp.connect())
        self.assertEquals(stomp.receiveFrame.call_count, 1, "receiveFrame not called")
    
    def test_connect_writes_correct_frame(self):
        login = 'curious'
        passcode = 'george'
        stomp = Stomp(HOST, PORT)
        stomp._socketConnect = Mock()
        stomp.receiveFrame = Mock()
        stomp.receiveFrame.return_value = StompFrame('CONNECTED', {StompSpec.SESSION_HEADER: '4711'})
        stomp.socket = Mock()
        stomp.connect(login=login, passcode=passcode)
        args, _ = stomp.socket.sendall.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals(StompFrame('CONNECT', {'login': login, 'passcode': passcode}), sentFrame)
    
    def test_send_writes_correct_frame(self):
        dest = '/queue/foo'
        msg = 'test message'
        headers = {'foo': 'bar', 'fuzz': 'ball'}
        stomp = Stomp(HOST, PORT)
        stomp._checkConnected = Mock()
        stomp._write = Mock()
        stomp.send(dest, msg, headers)
        args, _ = stomp._write.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals(StompFrame('SEND', {StompSpec.DESTINATION_HEADER: dest, 'foo': 'bar', 'fuzz': 'ball'}, msg), sentFrame)

    def test_subscribe_writes_correct_frame(self):
        dest = '/queue/foo'
        headers = {'foo': 'bar', 'fuzz': 'ball'}
        stomp = Stomp(HOST, PORT)
        stomp._checkConnected = Mock()
        stomp._write = Mock()
        stomp.subscribe(dest, headers)
        args, _ = stomp._write.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals(StompFrame('SUBSCRIBE', {StompSpec.DESTINATION_HEADER: dest, StompSpec.ACK_HEADER: 'auto', 'activemq.prefetchSize': '1', 'foo': 'bar', 'fuzz': 'ball'}, ''), sentFrame)

    def test_ack_writes_correct_frame(self):
        id_ = '12345'
        stomp = Stomp(HOST, PORT)
        stomp._checkConnected = Mock()
        stomp._write = Mock()
        stomp.ack(StompFrame('MESSAGE', {StompSpec.MESSAGE_ID_HEADER: id_}, 'blah'))
        args, _ = stomp._write.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals(StompFrame('ACK', {StompSpec.MESSAGE_ID_HEADER: id_}), sentFrame)

    def test_transaction_writes_correct_frames(self):
        transactionId = '4711'
        stomp = Stomp(HOST, PORT)
        stomp._checkConnected = Mock()
        stomp._write = Mock()
        for (method, cmd) in [(stomp.begin, 'BEGIN'), (stomp.commit, 'COMMIT'), (stomp.abort, 'ABORT')]:
            method(transactionId)
            args, _ = stomp._write.call_args
            sentFrame = self.parseFrame(args[0])
            self.assertEquals(StompFrame(cmd, {'transaction': transactionId},), sentFrame)
            
        with stomp.transaction(transactionId):
            args, _ = stomp._write.call_args
            sentFrame = self.parseFrame(args[0])
            self.assertEquals(StompFrame('BEGIN', {'transaction': transactionId},), sentFrame)
            
        args, _ = stomp._write.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals(StompFrame('COMMIT', {'transaction': transactionId}), sentFrame)
            
        try:
            with stomp.transaction(transactionId):
                raise
        except:
            args, _ = stomp._write.call_args
            sentFrame = self.parseFrame(args[0])
            self.assertEquals(StompFrame('ABORT', {'transaction': transactionId}), sentFrame)

    def parseFrame(self, message):
        parser = StompParser()
        parser.add(message)
        return parser.get()
    
    def getFrame(self, cmd, headers, body):
        return str(StompFrame(cmd, headers, body))
        
if __name__ == '__main__':
    unittest.main()