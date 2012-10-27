# -*- coding: iso-8859-1 -*-
"""
Copyright 2011, 2012 Mozes, Inc.

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
import collections
import cStringIO

from stompest.error import StompFrameError

from .frame import StompFrame
from .spec import StompSpec

class StompParser(object):
    def __init__(self, version=None):
        self.version = version or StompSpec.DEFAULT_VERSION
        self._parsers = {
            'command': self._parseCommand,
            'headers': self._parseHeader,
            'body': self._parseBody,
        }
        self.reset()
    
    def canRead(self):
        return bool(self._messages)
    
    def get(self):
        if self.canRead():
            return self._messages.popleft()
    
    def add(self, data):
        for character in data:
            if not character:
                return
            self.parse(character)
    
    def reset(self):
        self._messages = collections.deque()
        self._next()
        
    def _flush(self):
        self._buffer = cStringIO.StringIO()

    def _next(self):
        self._message = StompFrame()
        self._length = -1
        self._read = 0
        self._transition('command')
        self._flush()
    
    def _transition(self, state):
        self._flush()
        self.parse = self._parsers[state]
        
    def _parseCommand(self, character):
        if character != StompSpec.LINE_DELIMITER:
            self._buffer.write(character)
            return
        command = self._buffer.getvalue()
        if not command:
            return
        if command not in StompSpec.COMMANDS[self.version]:
            raise StompFrameError('Invalid command: %s' % repr(command))
        self._message.command = command
        self._transition('headers')
        
    def _parseHeader(self, character):
        if character != StompSpec.LINE_DELIMITER:
            self._buffer.write(character)
            return
        header = self._buffer.getvalue()
        if header:
            try:
                name, value = header.split(StompSpec.HEADER_SEPARATOR, 1)
            except ValueError:
                raise StompFrameError('No separator in header line: %s' % header)
            self._message.headers[name] = value
            self._transition('headers')
        else:
            self._length = int(self._message.headers.get(StompSpec.CONTENT_LENGTH_HEADER, -1))
            self._transition('body')
        
    def _parseBody(self, character):
        self._read += 1
        if (self._read <= self._length) or (character != StompSpec.FRAME_DELIMITER):
            self._buffer.write(character)
            return
        self._message.body = self._buffer.getvalue()
        self._messages.append(self._message)
        self._next()
