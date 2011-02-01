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

from stompest.simple import Stomp

class SimpleStompIntegrationTest(unittest.TestCase):
    
    def test_integration(self):
        dest = '/queue/stompUnitTest'
        stomp = Stomp('localhost', 61613)
        stomp.connect()
        stomp.send(dest, 'test message1')
        stomp.send(dest, 'test message2')
        self.assertFalse(stomp.canRead(1))
        stomp.subscribe(dest, {'ack': 'client'})
        self.assertTrue(stomp.canRead(1))
        frame = stomp.receiveFrame()
        stomp.ack(frame)
        stomp.canRead(1)
        self.assertTrue(stomp.canRead(1))
        frame = stomp.receiveFrame()
        stomp.ack(frame)
        self.assertFalse(stomp.canRead(1))
        stomp.disconnect()

if __name__ == '__main__':
    unittest.main()