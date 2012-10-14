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
import logging

from twisted.internet import reactor
from twisted.internet.protocol import Factory, Protocol 

from stompest.error import StompFrameError
from stompest.protocol.frame import StompFrame
from stompest.protocol.parser import StompParser
from stompest.protocol.spec import StompSpec

LOG_CATEGORY = 'stompest.tests.broker_simulator'

class BlackHoleStompServer(Protocol):
    delimiter = StompSpec.FRAME_DELIMITER
    
    def __init__(self):
        self.log = logging.getLogger(LOG_CATEGORY)
        self.resetParser()
        self.cmdMap = {
            StompSpec.CONNECT: self.handleConnect,
            StompSpec.DISCONNECT: self.handleDisconnect,
            StompSpec.SEND: self.handleSend,
            StompSpec.SUBSCRIBE: self.handleSubscribe,
            StompSpec.ACK: self.handleAck,
        }

    def connectionMade(self):
        self.log.debug('Connection made')

    def connectionLost(self, reason): 
        self.log.debug('Connection lost: %s' % reason)
        if hasattr(self.factory, 'disconnectDeferred'):
            self.factory.disconnectDeferred.callback('Disconnected')

    def dataReceived(self, data):
        self._parser.add(data)
                
        while True:
            message = self._parser.getMessage()
            if not message:
                break
            try:
                self.log.debug('Received frame: %s' % message)
            except KeyError:
                raise StompFrameError('Unknown STOMP command: %s' % message)
            self.cmdMap[message['cmd']](message)

    def resetParser(self):
        self._parser = StompParser()

    def getFrame(self, cmd, headers, body):
        return str(StompFrame(cmd, headers, body))
        
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
        self.transport.write(self.getFrame(StompSpec.ERROR, {}, 'Fake error message'))

class ErrorOnSendStompServer(BlackHoleStompServer):
    def handleConnect(self, msg):
        self.transport.write(self.getFrame(StompSpec.CONNECTED, {}, ''))

    def handleDisconnect(self, msg):
        self.transport.loseConnection()
        
    def handleSend(self, msg):
        self.transport.write(self.getFrame(StompSpec.ERROR, {}, 'Fake error message'))

class RemoteControlViaFrameStompServer(BlackHoleStompServer):
    def handleConnect(self, msg):
        self.transport.write(self.getFrame(StompSpec.CONNECTED, {}, ''))

    def handleSend(self, msg):
        if msg['body'] == 'disconnect':
            self.transport.loseConnection()
        
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    factory = Factory()
    factory.protocol = ErrorOnConnectStompServer
    reactor.listenTCP(8007, factory) 
    reactor.run() 
