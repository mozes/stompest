"""
Copyright 2011 Mozes, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import unittest                                 

import stomper
from stompest.parser import StompFrameLineParser
from stompest.error import StompFrameError

class StompFrameLineParserTest(unittest.TestCase):
    
    def test_frameParse_succeeds(self):
        frame = stomper.Frame()
        frame.cmd = 'SEND'
        frame.headers = {'foo': 'bar', 'hello ': 'there-world with space ', 'empty-value':'', '':'empty-header'}
        frame.headers['destination'] = '/queue/blah'
        frame.body = 'some stuff\nand more'
        cmd = frame.pack()
        lines = cmd.split('\n')[:-1]

        parser = StompFrameLineParser()
        self.assertFalse(parser.isDone())
        for line in lines:
            self.assertFalse(parser.isDone())
            parser.processLine(line)
        self.assertTrue(parser.isDone())
                
        msg = parser.getMessage()
        self.assertEqual(msg, {'cmd': 'SEND', 
                               'headers': {'foo': 'bar', 'hello ': 'there-world with space ', 'empty-value':'', '':'empty-header', 'destination': '/queue/blah'},
                               'body': 'some stuff\nand more'})
        
    def test_frame_without_header_or_body_succeeds(self):
        cmd = stomper.disconnect()
        lines = cmd.split('\n')[:-1]
        parser = StompFrameLineParser()
        for line in lines:
            parser.processLine(line)
        msg = parser.getMessage()
        self.assertEqual(msg, {'cmd': 'DISCONNECT', 'headers': {}, 'body': ''})

    def test_getMessage_returns_None_if_not_done(self):
        parser = StompFrameLineParser()
        self.assertEqual(None, parser.getMessage())
        parser.processLine('CONNECT')
        self.assertEqual(None, parser.getMessage())
        
    def test_processLine_throws_FrameError_after_done(self):
        cmd = stomper.connect('foo', 'bar')
        lines = cmd.split('\n')[:-1]

        parser = StompFrameLineParser()
        for line in lines:
            parser.processLine(line)
            
        self.assertTrue(parser.isDone())
        self.assertRaises(StompFrameError, lambda: parser.processLine('SEND'))

    def test_processLine_throws_FrameError_on_empty_command(self):
        parser = StompFrameLineParser()
        self.assertRaises(StompFrameError, lambda: parser.processLine(''))

    def test_processLine_throws_FrameError_on_header_line_missing_separator(self):
        parser = StompFrameLineParser()
        parser.processLine('SEND')
        self.assertRaises(StompFrameError, lambda: parser.processLine('no separator'))

                
if __name__ == '__main__':
    unittest.main()