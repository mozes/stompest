"""
Twisted STOMP client

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

import stomper

from twisted.internet import reactor, defer
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet.error import ConnectionLost

from stompest.parser import StompParser
from stompest.error import StompError, StompProtocolError, StompConnectTimeout, StompFrameError
from stompest.util import cloneStompMessageForErrorDest as _cloneStompMessageForErrorDest
from stompest.util import createFrame as _createFrame

LOG_CATEGORY = 'stompest.async'

class StompClient(Protocol):
    """A Twisted implementation of a STOMP client"""
    delimiter = StompParser.FRAME_DELIMITER
    MESSAGE_INFO_LENGTH = 20

    def __init__(self):
        self.log = logging.getLogger(LOG_CATEGORY)
        self.cmdMap = {
            'MESSAGE': self.handleMessage,
            'CONNECTED': self.handleConnected,
            'ERROR': self.handleError,
            'RECEIPT': self.handleReceipt,
        }
        self.clientAckModes = {'client': 1, 'client-individual': 1}
        self.destMap = {}
        self.connectedDeferred = None
        self.connectTimeoutDelayedCall = None
        self.connectError = None
        self.disconnectedDeferred = None
        self.finishedHandlersDeferred = None
        self.disconnecting = False
        self.disconnectError = None
        self.activeHandlers = {}
        self.resetParser()

    #
    # Overriden methods from parent protocol class
    #
    def connectionMade(self):
        """When TCP connection is made, register shutdown handler
        """
        Protocol.connectionMade(self)
        
    def connectionLost(self, reason):
        """When TCP connection is lost, remove shutdown handler
        """
        if reason.type == ConnectionLost:
            msg = 'Disconnected'
        else:
            msg = 'Disconnected: %s' % reason.getErrorMessage()
        self.log.debug(msg)
            
        #Remove connect timeout if set
        if self.connectTimeoutDelayedCall is not None:
            self.log.debug('Cancelling connect timeout after TCP connection was lost')
            self.connectTimeoutDelayedCall.cancel()
            self.connectTimeoutDelayedCall = None
        
        #Callback for failed connect
        if self.connectedDeferred:
            if self.connectError:
                self.log.debug('Calling connectedDeferred errback: %s' % self.connectError)
                self.connectedDeferred.errback(self.connectError)
                self.connectError = None
            else:
                self.log.error('Connection lost with outstanding connectedDeferred')
                error = StompError('Unexpected connection loss')
                self.log.debug('Calling connectedDeferred errback: %s' % error)
                self.connectedDeferred.errback(error)                
            self.connectedDeferred = None
        
        #Callback for disconnect
        if self.disconnectedDeferred:
            if self.disconnectError:
                #self.log.debug('Calling disconnectedDeferred errback: %s' % self.disconnectError)
                self.disconnectedDeferred.errback(self.disconnectError)
                self.disconnectError = None
            else:
                #self.log.debug('Calling disconnectedDeferred callback')
                self.disconnectedDeferred.callback(self)
            self.disconnectedDeferred = None
            
        Protocol.connectionLost(self, reason)

    def dataReceived(self, data):
        self.parser.add(data)
                
        while True:
            message = self.parser.getMessage()
            if not message:
                break
            try:
                command = self.cmdMap[message['cmd']]
            except KeyError:
                raise StompFrameError('Unknown STOMP command: %s' % message)
            command(message)

    #
    # Methods for sending raw STOMP commands
    #
    def _connect(self):
        """Send connect command
        """
        self.log.debug('Sending connect command')
        cmd = stomper.connect(self.factory.login, self.factory.passcode)
        self._write(cmd)

    def _disconnect(self):
        """Send disconnect command
        """
        self.log.debug('Sending disconnect command')
        cmd = stomper.disconnect()
        self._write(cmd)

    def _subscribe(self, dest, headers):
        """Send subscribe command
        """
        ack = headers.get('ack')
        self.log.debug('Sending subscribe command for destination %s with ack mode %s' % (dest, ack))
        headers['destination'] = dest
        # TODO: Roger, please add a comment why you used self.packFrame instead of stomper
        cmd = self.packFrame({'cmd': 'SUBSCRIBE', 'headers': headers})
        self._write(cmd)

    def _ack(self, messageId):
        """Send ack command
        """
        self.log.debug('Sending ack command for message: %s' % messageId)
        #NOTE: cannot use stomper.ack(messageId) with ActiveMQ 5.6.0
        #       b/c stomper adds a space to the header and AMQ no longer accepts it
        cmd = self.packFrame({'cmd': 'ACK', 'headers': {'message-id': messageId}})
        self._write(cmd)
    
    def _write(self, frame):
        #self.log.debug('sending frame:\n%s' % frame)
        self.transport.write(frame)

    #
    # Private helper methods
    #
    @classmethod
    def packFrame(cls, message):
        return _createFrame(message).pack()
        
    def resetParser(self):
        """Stomp parser must be reset after each frame is received
        """
        self.parser = StompParser()
    
    def finishHandlers(self):
        """Return a Deferred to signal when all requests in process are complete
        """
        if self.handlersInProgress():
            self.finishedHandlersDeferred = defer.Deferred()
            return self.finishedHandlersDeferred
    
    def handlersInProgress(self):
        if self.activeHandlers:
            return True
        return False
    
    def handlerFinished(self, messageId):
        del self.activeHandlers[messageId]
        self.log.debug('Handler complete for message: %s' % messageId)

    def handlerStarted(self, messageId):
        if messageId in self.activeHandlers:
            raise StompProtocolError('Duplicate message received. Message id %s is already in progress' % messageId)
        self.activeHandlers[messageId] = None
        self.log.debug('Handler started for message: %s' % messageId)
    
    def messageHandlerFailed(self, failure, messageId, msg, errDest):
        self.log.error('Error in message handler: %s' % str(failure))
        disconnect = False
        #Forward message to error queue if configured
        if errDest is not None:
            errMsg = _cloneStompMessageForErrorDest(msg)
            self.send(errDest, errMsg['body'], errMsg['headers'])
            self._ack(messageId)
            if self.factory.alwaysDisconnectOnUnhandledMsg:
                disconnect = True
        else:
            disconnect = True
        if disconnect:
            #Set disconnect error
            self.disconnectError = failure
            #Disconnect
            self.disconnect()
            return failure
        return None
        
    def connectTimeout(self, timeout):
        self.log.error('Connect command timed out after %s seconds' % timeout)
        self.connectTimeoutDelayedCall = None
        self.connectError = StompConnectTimeout('Connect command timed out after %s seconds' % timeout)
        self.transport.loseConnection()
        
    def handleConnected(self, msg):
        """Handle STOMP CONNECTED commands
        """
        sessionId = msg['headers'].get('session')
        self.log.debug('Connected to stomp broker with session: %s' % sessionId)
        #Remove connect timeout if set
        if self.connectTimeoutDelayedCall is not None:
            self.log.debug('Cancelling connect timeout after sucessfully connecting')
            self.connectTimeoutDelayedCall.cancel()
            self.connectTimeoutDelayedCall = None
        self.disconnectedDeferred = defer.Deferred()
        self.connectedDeferred.callback(self)
        self.connectedDeferred = None
    
    def handleMessage(self, msg):
        """Handle STOMP MESSAGE commands
        """
        dest = msg['headers']['destination']
        messageId = msg['headers']['message-id']
        errDest = self.destMap[dest]['errorDestination']
        clientAck = self.destMap[dest]['ack'] in self.clientAckModes

        #Do not process any more messages if we're disconnecting
        if self.disconnecting:
            self.log.debug('Disconnecting...ignoring stomp message: %s at destination: %s' % (messageId, dest))
            return

        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Received stomp message %s from destination %s: [%s...].  Headers: %s' % (messageId, dest, msg['body'][:self.MESSAGE_INFO_LENGTH], msg['headers']))
        
        #Call message handler (can return deferred to be async)
        self.handlerStarted(messageId)        
        handlerCall = defer.maybeDeferred(self.destMap[dest]['handler'], self, msg)
        #Ack the message if necessary
        if clientAck:
            handlerCall.addCallback(lambda result: self._ack(messageId))
        handlerCall.addErrback(self.messageHandlerFailed, messageId, msg, errDest)
        handlerCall.addBoth(self.postProcessMessage, messageId)

    def postProcessMessage(self, result, messageId):
        self.handlerFinished(messageId)
        #If someone's waiting to know that all handlers are done, call them back
        if self.finishedHandlersDeferred and not self.handlersInProgress():
            self.finishedHandlersDeferred.callback(self)
            self.finishedHandlersDeferred = None

    def handleError(self, msg):
        """Handle STOMP ERROR commands
        """
        self.log.info('Received stomp error: %s' % msg)
        if self.connectedDeferred is not None:
            self.transport.loseConnection()
            self.connectError = StompProtocolError('STOMP error message received while trying to connect: %s' % msg)
        else:
            #Work around for AMQ < 5.2
            if 'message' in msg['headers'] and msg['headers']['message'].find('Unexpected ACK received for message-id') >= 0:
                self.log.debug('AMQ brokers < 5.2 do not support client-individual mode.')
            else:
                #Set disconnect error
                self.disconnectError = StompProtocolError('STOMP error message received: %s' % msg)
                #Disconnect
                self.disconnect()
        
    def handleReceipt(self, msg):
        """Handle STOMP RECEIPT commands
        """
        self.log.info('Received stomp receipt: %s' % msg)
    
    #
    # Public functions
    #
    def connect(self, timeout=None):
        """Send connect command and return Deferred for caller that will get trigger when connect is complete
        """
        if timeout is not None:
            self.connectTimeoutDelayedCall = reactor.callLater(timeout, self.connectTimeout, timeout)
        self._connect()
        self.connectedDeferred = defer.Deferred()
        return self.connectedDeferred

    def disconnect(self):
        """After finishing outstanding requests, send disconnect command and return Deferred for caller that will get trigger when disconnect is complete
        """
        if not self.disconnecting:
            self.disconnecting = True
            #Send disconnect command after outstanding messages are ack'ed
            defer.maybeDeferred(self.finishHandlers).addBoth(lambda result: self._disconnect())

        return self.disconnectedDeferred
    
    def subscribe(self, dest, handler, headers=None, **kwargs):
        """Subscribe to a destination and register a function handler to receive messages for that destination
        """
        errorDestination = kwargs.get('errorDestination')
        # client-individual mode is only supported in AMQ >= 5.2
        # headers['ack'] = headers.get('ack', 'client-individual')
        headers = dict(headers or {})
        headers['ack'] = headers.get('ack', 'client')
        self.destMap[dest] = {'handler': handler, 'ack': headers['ack'], 'errorDestination': errorDestination}
        self._subscribe(dest, headers)
    
    def send(self, dest, msg, headers=None):
        """Do the send command to enqueue a message to a destination
        """
        headers = dict(headers or {})
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Sending message to %s: [%s...]' % (dest, msg[:self.MESSAGE_INFO_LENGTH]))
        headers['destination'] = dest
        cmd = self.packFrame({'cmd': 'SEND', 'headers': headers, 'body': msg})
        self._write(cmd)
        
    def getDisconnectedDeferred(self):
        return self.disconnectedDeferred
    
class StompClientFactory(ClientFactory):
    protocol = StompClient

    def __init__(self, **kwargs):
        self.login = kwargs.get('login', '')
        self.passcode = kwargs.get('passcode', '')
        self.alwaysDisconnectOnUnhandledMsg = kwargs.get('alwaysDisconnectOnUnhandledMsg', True)
        self.buildProtocolDeferred = defer.Deferred()
        self.log = logging.getLogger(LOG_CATEGORY)
    
    def buildProtocol(self, addr):
        protocol = ClientFactory.buildProtocol(self, addr)
        #This is a sneaky way of passing the protocol instance back to the caller
        reactor.callLater(0, self.buildProtocolDeferred.callback, protocol)
        return protocol
    
    def clientConnectionFailed(self, connector, reason):
        """Connection failed
        """
        self.log.error('Connection failed. Reason: %s' % str(reason))
        self.buildProtocolDeferred.errback(reason)

class StompConfig(object):
    def __init__(self, host, port, **kwargs):
        self.host = host
        self.port = port
        self.login = kwargs.get('login', '')
        self.passcode = kwargs.get('passcode', '')
        self.log = logging.getLogger(LOG_CATEGORY)

class StompCreator(object):
    def __init__(self, config, **kwargs):
        self.config = config
        self.connectTimeout = kwargs.get('connectTimeout')
        self.alwaysDisconnectOnUnhandledMsg = kwargs.get('alwaysDisconnectOnUnhandledMsg', False)
        self.log = logging.getLogger(LOG_CATEGORY)
        self.stompConnectedDeferred = None

    def getConnection(self):
        self.stompConnectedDeferred = defer.Deferred()
        self.connect().addCallback(self.connected).addErrback(self.stompConnectedDeferred.errback)
        return self.stompConnectedDeferred

    def connect(self):
        factory = StompClientFactory(login=self.config.login, passcode=self.config.passcode, alwaysDisconnectOnUnhandledMsg=self.alwaysDisconnectOnUnhandledMsg)
        reactor.connectTCP(self.config.host, self.config.port, factory) 
        return factory.buildProtocolDeferred
        
    def connected(self, stomp):
        stomp.connect(self.connectTimeout).addCallback(self.stompConnected).addErrback(self.stompConnectedDeferred.errback)

    def stompConnected(self, stomp):
        self.stompConnectedDeferred.callback(stomp)
        self.stompConnectedDeferred = None
