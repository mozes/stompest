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
import itertools
import logging
import unittest

from mock import Mock

from stompest.sync.transport import StompFrameTransport
from stompest.protocol.frame import StompFrame
from stompest.error import StompConnectionError

logging.basicConfig(level=logging.DEBUG)

HOST = 'fakeHost'
PORT = 61613

class StompFrameTransportTest(unittest.TestCase):
    def _generate_bytes(self, stream):
        for byte in stream:
            yield byte
        while True:
            yield ''

    def _recv(self, iterable, size):
        return ''.join(itertools.islice(iterable, size))
    
    def _get_receive_mock(self, stream):
        transport = StompFrameTransport(HOST, PORT)
        socket = transport._socket = Mock()
        stream = self._generate_bytes(stream)
        socket.recv = Mock(wraps=lambda size, *_: self._recv(stream, size))
        return transport
    
    def test_receive(self):
        headers = {'x': 'y'}
        body = 'testing 1 2 3'
        frame = StompFrame('MESSAGE', headers, body)
                
        transport = self._get_receive_mock(str(frame))
        frame_ = transport.receive()
        self.assertEquals(frame, frame_)        
        self.assertEquals(1, transport._socket.recv.call_count)
        
        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(transport._socket, None)

    def test_receive_multiple_frames_extra_newlines(self):
        headers = {'x': 'y'}
        body = 'testing 1 2 3'
        frame = StompFrame('MESSAGE', headers, body)
        
        transport = self._get_receive_mock('\n\n%s\n%s\n' % (frame, frame))
        frame_ = transport.receive()
        self.assertEquals(frame, frame_)
        frame_ = transport.receive()
        self.assertEquals(frame, frame_)
        self.assertEquals(1, transport._socket.recv.call_count)
        
        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(transport._socket, None)        

    def test_receive_binary(self):
        body = binascii.a2b_hex('f0000a09')
        headers = {'content-length': str(len(body))}
        frame = StompFrame('MESSAGE', headers, body)
                
        transport = self._get_receive_mock(str(frame))
        frame_ = transport.receive()
        self.assertEquals(frame, frame_)
        self.assertEquals(1, transport._socket.recv.call_count)
        
        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(transport._socket, None)
        
    def test_receive_multiple_frames_per_read(self):
        body1 = 'boo'
        body2 = 'hoo'
        headers = {'x': 'y'}
        frameBytes = str(StompFrame('MESSAGE', headers, body1)) + str(StompFrame('MESSAGE', headers, body2))

        transport = self._get_receive_mock(frameBytes)
        
        frame = transport.receive()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body1, frame.body)
        self.assertEquals(1, transport._socket.recv.call_count)

        frame = transport.receive()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body2, frame.body)
        self.assertEquals(1, transport._socket.recv.call_count)
        
        self.assertRaises(StompConnectionError, transport.receive)
        self.assertEquals(transport._socket, None)
        
if __name__ == '__main__':
    unittest.main()