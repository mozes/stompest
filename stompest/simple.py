"""
Copyright 2011 Mozes, Inc.

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
import logging
import select
import socket

import stomper

from stompest.error import StompProtocolError
from stompest.parser import StompParser
from stompest.util import createFrame as _createFrame

LOG_CATEGORY = 'stompest.simple'

class Stomp(object):
    """A simple implementation of a STOMP client"""
    
    READ_SIZE = 4096
    
    def __init__(self, host, port):
        self.log = logging.getLogger(LOG_CATEGORY)
        self.host = host
        self.port = port
        self.socket = None
        self._setParser()
    
    def connect(self, login='', passcode=''):
        self._socketConnect()
        self._setParser()
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
            readList, _, _ = select.select([self.socket], [], [])
        else:
            readList, _, _ = select.select([self.socket], [], [], timeout)
        return bool(readList)
        
    def send(self, dest, msg, headers=None):
        headers = headers or {}
        headers['destination'] = dest
        frame = {'cmd': 'SEND', 'headers': headers, 'body': msg}
        self.sendFrame(frame)
        
    def subscribe(self, dest, headers=None):
        headers = headers or {}
        headers.setdefault('ack', 'auto')
        headers.setdefault('activemq.prefetchSize', 1)
        headers['destination'] = dest
        self.sendFrame({'cmd': 'SUBSCRIBE', 'headers': headers, 'body': ''})
        
    def ack(self, frame):
        messageId = frame['headers']['message-id']
        self.sendFrame({'cmd': 'ACK', 'headers': {'message-id': messageId}, 'body': ''})
    
    def sendFrame(self, frame):
        self._write(self.packFrame(frame))
    
    def receiveFrame(self):
        while True:
            try:
                message = self.parser.getMessage()
            except Exception, e:
                self.log.exception(e)
                self.log.error('Parser state: %s' % self.parser.__dict__)
                raise
            if message:
                return message
            data = self.socket.recv(self.READ_SIZE)
            if not data:
                raise Exception('Connection closed')
            self.parser.add(data)
    
    def packFrame(self, message):
        return _createFrame(message).pack()
        
    def _setParser(self):
        self.parser = StompParser()
        
    def _socketConnect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        
    def _socketDisconnect(self):
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
