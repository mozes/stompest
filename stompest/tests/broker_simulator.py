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
import logging
import stomper
from twisted.internet.protocol import Protocol, Factory 
from twisted.internet import reactor
from twisted.protocols.basic import LineOnlyReceiver
from stompest.parser import StompFrameLineParser

LOG_CATEGORY="stompest.tests.broker_simulator"

class BlackHoleStompServer(LineOnlyReceiver):

    def __init__(self):
        self.log = logging.getLogger(LOG_CATEGORY)
        self.resetParser()
        self.cmdMap = {
            'CONNECT': self.handleConnect,
            'DISCONNECT': self.handleDisconnect,
            'SEND': self.handleSend,
            'SUBCRIBE': self.handleSubscribe,
            'ACK': self.handleAck,
        }

    def connectionMade(self):
        self.log.debug('Connection made')

    def connectionLost(self, reason): 
        self.log.debug('Connection lost')
        if 'disconnectDeferred' in self.factory.__dict__:
            self.factory.disconnectDeferred.callback('Disconnected')

    def lineReceived(self, line):
        self.parser.processLine(line)
        if (self.parser.isDone()):
            frame = self.parser.getMessage()
            self.resetParser()
            try:
                self.log.debug("Received frame: %s" % frame)
                self.cmdMap[frame['cmd']](frame)
            except KeyError:
                raise stomper.FrameError("Unknown STOMP command: %s" % str(frame))

    def resetParser(self):
        self.parser = StompFrameLineParser()
        self.delimiter = self.parser.lineDelimiter

    def getFrame(self, cmd, headers, body):
        sFrame = stomper.Frame()
        sFrame.cmd = cmd
        sFrame.headers = headers
        sFrame.body = body
        return sFrame.pack()
        
    def handleConnect(self, msg):
        pass

    def handleDisconnect(self, msg):
        pass

    def handleSend(self, msg):
        pass

    def handleSubscribe(self, msg):
        pass

    def handleAck(self, msg):
        pass

class ErrorOnConnectStompServer(BlackHoleStompServer):
    
    def handleConnect(self, msg):
        self.transport.write(self.getFrame('ERROR', {}, 'Fake error message'))

class ErrorOnSendStompServer(BlackHoleStompServer):

    def handleConnect(self, msg):
        self.transport.write(self.getFrame('CONNECTED', {}, ''))

    def handleDisconnect(self, msg):
        self.transport.loseConnection()
        
    def handleSend(self, msg):
        self.transport.write(self.getFrame('ERROR', {}, 'Fake error message'))
    

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    factory          = Factory()
    factory.protocol = ErrorOnConnectStompServer
    reactor.listenTCP(8007, factory) 
    reactor.run() 
