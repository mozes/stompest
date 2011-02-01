"""
Copyright 2011 Mozes, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import stomper
from stompest.error import StompFrameError

class StompFrameLineParser(object):
    """State machine for line-based parsing of STOMP frames.
    
    http://stomp.codehaus.org/Protocol
    
    Note: Although the protocol allows for null bytes in the body
          in conjunction with the content-length header, it is not
          implemented
    
    parser = StompFrameLineParser()
    frame = None
    while 1:
        line = getNextLine()
        parser.processLine(line)
          if parser.isDone():
              frame = parser.getMessage()
              break
    """
    lineDelimiter = '\n'
    frameDelimiter = '\x00'
    headerDelimiter = ':'

    def __init__(self):
        self.message = dict(cmd='', headers={}, body='')
        self.state = 'cmd'
        self.done = False
        self.states = {
            'cmd': self.parseCommandLine,
            'headers': self.parseHeaderLine,
            'body': self.parseBodyLine,
        }
    
    def isDone(self):
        """Call this method after each line is processed to see if the frame is complete
        """
        return self.done
        
    def getMessage(self):
        """When a complete frame has been parsed, call this method to get whole thing
        """
        if (self.done):
            return self.message
        return None
    
    def processLine(self, line):
        """Call this method for each line receive for the stomp frame
        """
        if (self.done):
            raise StompFrameError('processLine() called after frame end')
        self.states[self.state](line)

    #
    # Internal methods
    #
    def transition(self, newState):
        self.state = newState
    
    def parseCommandLine(self, line):
        if (len(line) == 0):
            raise StompFrameError("Empty stomp command line: %s" % line)
        self.message['cmd'] = line
        self.transition('headers')
        
    def parseHeaderLine(self, line):
        if (len(line) == 0):
            self.transition('body')
            return
        try:
            name, value = line.split(self.headerDelimiter, 1)
        except ValueError:
            raise StompFrameError('Invalid stomp header line: [%s], len [%d]' % (line, len(line)))
        self.message['headers'][name] = value
        
    def parseBodyLine(self, line):
        if line.endswith(self.frameDelimiter):
            self.message['body'] += line[:-1]
            self.endFrame()
            return
        self.message['body'] += line + self.lineDelimiter
        
    def endFrame(self):
        self.done = True