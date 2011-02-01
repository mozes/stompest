from stompest.simple import Stomp

QUEUE = '/queue/simpleTest'

stomp = Stomp('localhost', 61613)
stomp.connect()
stomp.send(QUEUE, 'test message1')
stomp.send(QUEUE, 'test message2')
stomp.disconnect()