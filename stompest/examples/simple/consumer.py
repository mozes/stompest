from stompest.simple import Stomp

QUEUE = '/queue/simpleTest'

stomp = Stomp('localhost', 61613)
stomp.connect()
stomp.subscribe(QUEUE, {'ack': 'client'})

while(True):
    frame = stomp.receiveFrame()
    print "Got message frame: %s" % frame
    stomp.ack(frame)
    
stomp.disconnect()