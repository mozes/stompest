# -*- coding: iso-8859-1 -*-
"""
Copyright 2012 Mozes, Inc.

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
import select
import socket

from stompest.error import StompConnectionError
from stompest.protocol import StompParser

LOG_CATEGORY = 'stompest.sync'

class StompFrameTransport(object):
    READ_SIZE = 4096
    
    def __init__(self, host, port, version=None):
        self.host = host
        self.port = port
        self.version = version
        
        self._socket = None
    
    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._socket.connect((self.host, self.port))
        except IOError as e:
            raise StompConnectionError('Could not establish connection [%s]' % e)
        self._parser = StompParser(self.version)
    
    def canRead(self, timeout=None):
        self._check()
        if self._parser.canRead():
            return True
        if timeout is None:
            files, _, _ = select.select([self._socket], [], [])
        else:
            files, _, _ = select.select([self._socket], [], [], timeout)
        return bool(files)
        
    def disconnect(self):
        try:
            self._socket.close()
        except IOError as e:
            raise StompConnectionError('Could not close connection cleanly [%s]' % e)
        finally:
            self._socket = None
    
    def send(self, frame):
        self._write(str(frame))
        
    def receive(self):
        while True:
            frame = self._parser.get()
            if frame:
                return frame
            try:
                data = self._socket.recv(self.READ_SIZE)
                if not data:
                    raise StompConnectionError('No more data')
            except (IOError, StompConnectionError) as e:
                self.disconnect()
                raise StompConnectionError('Connection closed [%s]' % e)
            self._parser.add(data)
    
    def _check(self):
        if not self._connected():
            raise StompConnectionError('Not connected')
       
    def _connected(self):
        return self._socket is not None
        
    def _write(self, data):
        self._check()
        try:
            self._socket.sendall(data)
        except IOError as e:
            raise StompConnectionError('Could not send to connection [%s]' % e)
