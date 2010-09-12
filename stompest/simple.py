import socket
import select
import stomper
import logging

from stompest.parser import StompFrameLineParser
from stompest.error import StompProtocolError

LOG_CATEGORY="stompest.simple"

class Stomp(object):
    """A simple implementation of a STOMP client"""
    
    def __init__(self, host, port):
        self.log = logging.getLogger(LOG_CATEGORY)
        self.host = host
        self.port = port
        self.socket = None
        self.sfile = None
    
    def connect(self, login='', passcode=''):
        self._socketConnect()
        self._write(stomper.connect(login, passcode))
        frame = self.receiveFrame()
        if frame['cmd'] == 'CONNECTED':
            return frame
        raise StompProtocolError('Unexpected frame received: %s' % frame)
        
    def disconnect(self):
        self._write(stomper.disconnect())
        self._socketDisconnect()

    def canRead(self, timeout=None):
        self._checkConnected()
        if timeout is None:
            readList, junk, junk = select.select([self.socket], [], [])
        else:
            readList, junk, junk = select.select([self.socket], [], [], timeout)
        return len(readList) > 0
        
    def send(self, dest, msg, headers={}):
        frame = {'cmd': 'SEND', 'headers': headers, 'body': msg}
        frame['headers']['destination'] = dest
        self.sendFrame(frame)
        
    def subscribe(self, dest, headers={}):
        if not 'ack' in headers:
            headers['ack'] = 'auto'
        if not 'activemq.prefetchSize' in headers:
            headers['activemq.prefetchSize'] = 1
        headers['destination'] = dest
        self.sendFrame({'cmd': 'SUBSCRIBE', 'headers': headers, 'body': ''})
        
    def ack(self, frame):
        messageId = frame['headers']['message-id']
        self.sendFrame({'cmd': 'ACK', 'headers': {'message-id': messageId}, 'body': ''})
    
    def sendFrame(self, frame):
        self._write(self.packFrame(frame))
    
    def receiveFrame(self):
        self._checkConnected()
        parser = StompFrameLineParser()
        while (not parser.isDone()):
            parser.processLine(self.sfile.readline()[:-1])

        return parser.getMessage()

    def packFrame(self, frame):
        sFrame = stomper.Frame()
        sFrame.cmd = frame['cmd']
        sFrame.headers = frame['headers']
        sFrame.body = frame['body']
        return sFrame.pack()
        
    def _socketConnect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sfile = self.socket.makefile()
        self.socket.connect((self.host, self.port))
        
    def _socketDisconnect(self):
        self.sfile.close()
        self.sfile = None
        self.socket.close()
        self.socket = None
    
    def _connected(self):
        return self.socket is not None
        
    def _checkConnected(self):
        if not self._connected():
            raise Exception('Not connected')
       
    def _write(self, data):
        self._checkConnected()
        self.socket.sendall(data)
