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

from stompest.protocol.frame import StompFrame
from stompest.protocol.spec import StompSpec

class StompFrameTest(unittest.TestCase):
    def test_frame(self):
        message = {'command': 'SEND', 'headers': {StompSpec.DESTINATION_HEADER: '/queue/world'}, 'body': 'two\nlines'}
        frame = StompFrame(**message)
        self.assertEquals(message['headers'], frame.headers)
        self.assertEquals(dict(frame), message)
        self.assertEquals(str(frame), """\
SEND
destination:/queue/world

two
lines\x00""")
        self.assertEquals(eval(repr(frame)), frame)

    def test_frame_without_headers_and_body(self):
        message = {'command': 'DISCONNECT', 'headers': {}, 'body': ''}
        frame = StompFrame(**message)
        self.assertEquals(message['headers'], frame.headers)
        self.assertEquals(dict(frame), message)
        self.assertEquals(str(frame), """\
DISCONNECT

\x00""")
        self.assertEquals(eval(repr(frame)), frame)
    
    def test_binary_body(self):
        body = binascii.a2b_hex('f0000a09')
        headers = {'content-length': str(len(body))}
        frame = StompFrame('MESSAGE', headers, body)
        self.assertEquals(frame.body, body)
        self.assertEquals(str(frame), 'MESSAGE\ncontent-length:4\n\n\xf0\x00\n\t\x00')

if __name__ == '__main__':
    unittest.main()