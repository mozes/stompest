from stompest.config import StompConfig
from stompest.sync import Stomp

CONFIG = StompConfig('tcp://localhost:61613')
QUEUE = '/queue/test'

if __name__ == '__main__':
    client = Stomp(CONFIG)
    client.connect()
    client.subscribe(QUEUE, {'ack': 'client'})
    while True:
        frame = client.receiveFrame()
        print 'Got %s' % frame.info()
        client.ack(frame)
    client.disconnect()