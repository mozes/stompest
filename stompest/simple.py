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

from stompest.error import StompConnectionError
from stompest.protocol import commands, StompParser, StompSpec

class Stomp(object):
    """A simple implementation of a STOMP client"""
    READ_SIZE = 4096
    
    def __init__(self, host, port, version=None):
        self.host = host
        self.port = port
        self.socket = None
        self.version = commands.version(version)
        self._setParser()
    
    def connect(self, login='', passcode='', headers=None, versions=None, host=None):
        self._socketConnect()
        self.sendFrame(commands.connect(login, passcode, headers, list(versions or [self.version]), host))
        self._setParser()
        return commands.connected(self.receiveFrame(), self.version)
    
    def disconnect(self):
        try:
            self.sendFrame(commands.disconnect())
        finally:
            self._socketDisconnect()
        
    def canRead(self, timeout=None):
        self._checkConnected()
        if self.parser.canRead():
            return True
        if timeout is None:
            readList, _, _ = select.select([self.socket], [], [])
        else:
            readList, _, _ = select.select([self.socket], [], [], timeout)
        return bool(readList)
        
    def send(self, destination, body='', headers=None, receipt=None):
        self.sendFrame(commands.send(destination, body, headers, receipt))
        
    def subscribe(self, destination=None, headers=None, receipt=None):
        headers = dict(headers or {})
        headers.setdefault(StompSpec.ACK_HEADER, 'auto')
        headers.setdefault('activemq.prefetchSize', 1)
        frame, token = commands.subscribe(destination, headers, receipt, self.version)
        self.sendFrame(frame)
        return token
        
    def unsubscribe(self, token, receipt=None):
        self.sendFrame(commands.unsubscribe(token, receipt, self.version))
    
    def transactionId(self, transactionId=None):
        return commands.transactionId(transactionId)
    
    def begin(self, transactionId):
        self.sendFrame(commands.begin(transactionId))
        
    def commit(self, transactionId):
        self.sendFrame(commands.commit(transactionId))        

    def abort(self, transactionId):
        self.sendFrame(commands.abort(transactionId))
    
    @contextlib.contextmanager
    def transaction(self, transactionId=None):
        transactionId = self.transactionId(transactionId)
        self.begin(transactionId)
        try:
            yield
            self.commit(transactionId)
        except:
            self.abort(transactionId)
        
    def ack(self, frame, receipt=None):
        self.sendFrame(commands.ack(frame, receipt, self.version))
    
    def nack(self, frame, receipt=None):
        self.sendFrame(commands.nack(frame, receipt, self.version))
    
    def receiveFrame(self):
        while True:
            frame = self.parser.get()
            if frame:
                return frame
            try:
                data = self.socket.recv(self.READ_SIZE)
                if not data:
                    raise StompConnectionError('No more data')
            except (IOError, StompConnectionError) as e:
                self._socketDisconnect()
                raise StompConnectionError('Connection closed [%s]' % e)
            self.parser.add(data)
    
    def sendFrame(self, frame):
        self._write(str(frame))
        
    def _checkConnected(self):
        if not self._connected():
            raise StompConnectionError('Not connected')
       
    def _connected(self):
        return self.socket is not None
        
    def _setParser(self):
        self.parser = StompParser(self.version)
        
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
    
    def _write(self, data):
        self._checkConnected()
        try:
            self.socket.sendall(data)
        except IOError as e:
            raise StompConnectionError('Could not send to connection [%s]' % e)
