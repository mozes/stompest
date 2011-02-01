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
from mock import Mock

import stomper
from stompest.simple import Stomp
from stompest.error import StompProtocolError

class SimpleStompTest(unittest.TestCase):
    
    def test_canRead_raises_exception_before_connect(self):
        stomp = Stomp('localhost', 61613)
        self.assertRaises(Exception, lambda: stomp.canRead())

    def test_send_raises_exception_before_connect(self):
        stomp = Stomp('localhost', 61613)
        self.assertRaises(Exception, lambda: stomp.send('/queue/foo', 'test message'))

    def test_subscribe_raises_exception_before_connect(self):
        stomp = Stomp('localhost', 61613)
        self.assertRaises(Exception, lambda: stomp.subscribe('/queue/foo'))
    
    def test_disconnect_raises_exception_before_connect(self):
        stomp = Stomp('localhost', 61613)
        self.assertRaises(Exception, lambda: stomp.disconnect())
    
    def test_connect_raises_exception_for_bad_host(self):
        stomp = Stomp('nosuchhost', 2345)
        self.assertRaises(Exception, lambda: stomp.connect())

    def test_error_frame_after_connect_raises_StompProtocolError(self):
        stomp = Stomp('localhost', 61613)
        stomp._socketConnect = Mock()
        stomp.receiveFrame = Mock()
        stomp.receiveFrame.return_value = {'cmd': 'ERROR', 'headers': {}, 'body': 'fake error'}
        stomp.socket = Mock()
        self.assertRaises(StompProtocolError, lambda: stomp.connect())
        self.assertEquals(stomp.receiveFrame.call_count, 1, "receiveFrame not called")
    
    def test_connect_writes_correct_frame(self):
        login = 'curious'
        passcode = 'george'
        stomp = Stomp('localhost', 61613)
        stomp._socketConnect = Mock()
        stomp.receiveFrame = Mock()
        stomp.receiveFrame.return_value = {'cmd': 'CONNECTED', 'headers': {}, 'body': ''}
        stomp.socket = Mock()
        stomp.connect(login=login,passcode=passcode)
        args,kargs = stomp.socket.sendall.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals({'cmd': 'CONNECT',
                           'headers': {'login': login,
                                       'passcode': passcode,
                                      },
                           'body': ''}, sentFrame)
    
    def test_send_writes_correct_frame(self):
        dest = '/queue/foo'
        msg = 'test message'
        headers = {'foo': 'bar', 'fuzz': 'ball'}
        stomp = Stomp('localhost', 61613)
        stomp._checkConnected = Mock()
        stomp._write = Mock()
        stomp.send(dest, msg, headers)
        args,kargs = stomp._write.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals({'cmd': 'SEND',
                           'headers': {'destination': dest,
                                       'foo': 'bar',
                                       'fuzz': 'ball',
                                      },
                           'body': msg}, sentFrame)

    def test_subscribe_writes_correct_frame(self):
        dest = '/queue/foo'
        headers = {'foo': 'bar', 'fuzz': 'ball'}
        stomp = Stomp('localhost', 61613)
        stomp._checkConnected = Mock()
        stomp._write = Mock()
        stomp.subscribe(dest, headers)
        args,kargs = stomp._write.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals({'cmd': 'SUBSCRIBE',
                           'headers': {'destination': dest,
                                       'ack': 'auto',
                                       'activemq.prefetchSize': '1',
                                       'foo': 'bar',
                                       'fuzz': 'ball',
                                      },
                           'body': ''}, sentFrame)

    def test_ack_writes_correct_frame(self):
        id = '12345'
        stomp = Stomp('localhost', 61613)
        stomp._checkConnected = Mock()
        stomp._write = Mock()
        stomp.ack({'cmd': 'MESSAGE', 'headers': {'message-id': id}, 'body': 'blah'})
        args,kargs = stomp._write.call_args
        sentFrame = self.parseFrame(args[0])
        self.assertEquals({'cmd': 'ACK',
                           'headers': {'message-id': id,
                                      },
                           'body': ''}, sentFrame)

    def parseFrame(self, message):
        from stompest.parser import StompFrameLineParser
        parser = StompFrameLineParser()        
        for line in message.split('\n')[:-1]:
            parser.processLine(line)
        self.assertTrue(parser.isDone())
        return parser.getMessage()

    def getFrame(self, cmd, headers, body):
        sFrame = stomper.Frame()
        sFrame.cmd = cmd
        sFrame.headers = headers
        sFrame.body = body
        return sFrame.pack()
        
if __name__ == '__main__':
    unittest.main()