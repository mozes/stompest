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