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
        socket.recv = Mock(wraps=lambda size, *args: self._recv(stream, size))
        return transport
    
    def test_receive(self):
        headers = {'x': 'y'}
        body = 'testing 1 2 3'
        frameBytes = str(StompFrame('MESSAGE', headers, body))
        self.assertTrue(frameBytes.endswith('\x00'))
        
        transport = self._get_receive_mock(frameBytes)
        frame = transport.receive()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body, frame.body)
        
        self.assertEquals(1, transport._socket.recv.call_count)

    def test_receive_no_newline(self):
        headers = {'x': 'y'}
        body = 'testing 1 2 3'
        frameBytes = str(StompFrame('MESSAGE', headers, body))
        self.assertTrue(frameBytes.endswith('\x00'))
        
        transport = self._get_receive_mock(frameBytes)
        frame = transport.receive()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body, frame.body)
        
        self.assertEquals(1, transport._socket.recv.call_count)

    def _test_receive_binary(self):
        body = binascii.a2b_hex('f0000a09')
        headers = {'content-length': str(len(body))}
        frameBytes = self.getFrame('MESSAGE', headers, body)
        
        transport = self._get_receive_mock(frameBytes)
        frame = transport.receive()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body, frame.body)
        
        self.assertEquals(1, transport._socket.recv.call_count)
        
    def test_receive_multiple_frames_per_read(self):
        body1 = 'boo'
        body2 = 'hoo'
        headers = {'x': 'y'}
        frameBytes = str(StompFrame('MESSAGE', headers, body1)) + str(StompFrame('MESSAGE', headers, body2))

        transport = self._get_receive_mock(frameBytes)
        
        #Read first frame
        frame = transport.receive()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body1, frame.body)
        self.assertEquals(1, transport._socket.recv.call_count)

        #Read next frame
        frame = transport.receive()
        self.assertEquals('MESSAGE', frame.command)
        self.assertEquals(headers, frame.headers)
        self.assertEquals(body2, frame.body)
        self.assertEquals(1, transport._socket.recv.call_count)

if __name__ == '__main__':
    unittest.main()