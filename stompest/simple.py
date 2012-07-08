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
import contextlib
import select
import socket

import stomper

from stompest.error import StompProtocolError, StompConnectionError
from stompest.parser import StompParser
from stompest.util import createFrame

class Stomp(object):
    """A simple implementation of a STOMP client"""
    READ_SIZE = 4096
    
    @classmethod
    def packFrame(cls, message):
        return createFrame(message).pack()
        
    def __init__(self, host, port):
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
        headers['destination'] = dest
        headers.setdefault('ack', 'auto')
        headers.setdefault('activemq.prefetchSize', 1)
        self.sendFrame({'cmd': 'SUBSCRIBE', 'headers': headers, 'body': ''})
        
    def unsubscribe(self, dest, headers=None):
        headers = headers or {}
        headers['destination'] = dest
        self.sendFrame({'cmd': 'UNSUBSCRIBE', 'headers': headers, 'body': ''})
    
    def begin(self, transactionId):
        self.sendFrame({'cmd': 'BEGIN', 'headers': {'transaction': transactionId}, 'body': ''})
        
    def commit(self, transactionId):
        self.sendFrame({'cmd': 'COMMIT', 'headers': {'transaction': transactionId}, 'body': ''})
        
    def abort(self, transactionId):
        self.sendFrame({'cmd': 'ABORT', 'headers': {'transaction': transactionId}, 'body': ''})
    
    @contextlib.contextmanager
    def transaction(self, transactionId):
        self.begin(transactionId)
        try:
            yield
            self.commit(transactionId)
        except:
            self.abort(transactionId)
        
    def ack(self, frame):
        messageId = frame['headers']['message-id']
        self.sendFrame({'cmd': 'ACK', 'headers': {'message-id': messageId}, 'body': ''})
    
    def sendFrame(self, frame):
        self._write(self.packFrame(frame))
    
    def receiveFrame(self):
        while True:
            message = self.parser.getMessage()
            if message:
                return message
            try:
                data = self.socket.recv(self.READ_SIZE)
                if not data:
                    raise StompConnectionError('No more data')
            except (IOError, StompConnectionError) as e:
                self._socketDisconnect()
                raise StompConnectionError('Connection closed [%s]' % e)
            self.parser.add(data)
    
    def _setParser(self):
        self.parser = StompParser()
        
    def _socketConnect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket.connect((self.host, self.port))
        except IOError as e:
            raise StompConnectionError('Could not establish connection [%s]' % e)
        
    def _socketDisconnect(self):
        try:
            self.socket.close()
        except IOError as e:
            raise StompConnectionError('Could not close connection cleanly [%s]' % e)
        finally:
            self.socket = None
    
    def _connected(self):
        return self.socket is not None
        
    def _checkConnected(self):
        if not self._connected():
            raise StompConnectionError('Not connected')
       
    def _write(self, data):
        self._checkConnected()
        try:
            self.socket.sendall(data)
        except IOError as e:
            raise StompConnectionError('Could not send to connection [%s]' % e)
