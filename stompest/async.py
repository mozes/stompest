"""
Twisted STOMP client

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
import sys
import logging
import stomper

from twisted.python import usage, log
from twisted.internet import reactor, defer
from twisted.protocols.basic import LineOnlyReceiver
from twisted.internet.protocol import ClientFactory
from twisted.internet.error import ConnectionLost

from stompest.parser import StompFrameLineParser
from stompest.error import StompError, StompProtocolError, StompConnectTimeout, StompFrameError
from stompest.util import cloneStompMessageForErrorDest

LOG_CATEGORY="stompest.async"

class StompClient(LineOnlyReceiver):
    """A Twisted implementation of a STOMP client"""
    MAX_LENGTH = sys.maxint

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
        LineOnlyReceiver.connectionMade(self)
        
    def connectionLost(self, reason):
        """When TCP connection is lost, remove shutdown handler
        """
        self.log.debug("Disconnected: %s" % reason)
            
        #Remove connect timeout if set
        if self.connectTimeoutDelayedCall is not None:
            self.log.debug("Cancelling connect timeout after TCP connection was lost")
            self.connectTimeoutDelayedCall.cancel()
            self.connectTimeoutDelayedCall = None
        
        #Callback for failed connect
        if self.connectedDeferred:
            if self.connectError:
                self.log.debug("Calling connectedDeferred errback: %s" % self.connectError)
                self.connectedDeferred.errback(self.connectError)
                self.connectError = None
            else:
                self.log.error("Connection lost with outstanding connectedDeferred")
                error = StompError("Unexpected connection loss")
                self.log.debug("Calling connectedDeferred errback: %s" % error)
                self.connectedDeferred.errback(error)                
            self.connectedDeferred = None
        
        #Callback for disconnect
        if self.disconnectedDeferred:
            if self.disconnectError:
                self.log.debug("Calling disconnectedDeferred errback: %s" % self.disconnectError)
                self.disconnectedDeferred.errback(self.disconnectError)
                self.disconnectError = None
            else:
                self.log.debug("Calling disconnectedDeferred callback")
                self.disconnectedDeferred.callback(self)
            self.disconnectedDeferred = None
            
        LineOnlyReceiver.connectionLost(self, reason)

    def lineReceived(self, line):
        """When a line is received, process it and dispatch when we've
           got a whole frame
        """
        # self.log.debug("Received line [%s]" % line)
        
        self.parser.processLine(line)
        if self.parser.isDone():
            frame = self.parser.getMessage()
            self.resetParser()
            if frame['cmd'] in self.cmdMap:
                self.cmdMap[frame['cmd']](frame)
            else:
                raise StompFrameError("Unknown STOMP command: %s" % str(frame))

    def lineLengthExceeded(self, line):
        errorMsg = "Stomp protocol implementation line length maximum (%s) was exceeded" % self.MAX_LENGTH
        self.log.critical(errorMsg)
        raise Exception(errorMsg)

    #
    # Methods for sending raw STOMP commands
    #
    def _connect(self):
        """Send connect command
        """
        self.log.debug("Sending connect command")
        cmd = stomper.connect(self.factory.getLogin(), self.factory.getPasscode())
        # self.log.debug("Writing cmd: %s" % cmd)
        self.transport.write(cmd)

    def _disconnect(self):
        """Send disconnect command
        """
        self.log.debug("Sending disconnect command")
        cmd = stomper.disconnect()
        # self.log.debug("Writing cmd: %s" % cmd)
        self.transport.write(cmd)

    def _subscribe(self, dest, headers):
        """Send subscribe command
        """
        ack = headers.get('ack', None)
        self.log.debug("Sending subscribe command for destination %s with ack mode %s" % (dest, ack))

        headers['destination'] = dest
                
        frame = stomper.Frame()
        frame.cmd = 'SUBSCRIBE'
        frame.headers = headers
        cmd = frame.pack()
        # self.log.debug("Writing cmd: %s" % cmd)
        self.transport.write(cmd)

    def _ack(self, messageId):
        """Send ack command
        """
        self.log.debug("Sending ack command for message: %s" % messageId)
        cmd = stomper.ack(messageId)
        # self.log.debug("Writing cmd: %s" % cmd)
        self.transport.write(cmd)

    #
    # Private helper methods
    #
    def resetParser(self):
        """Stomp parser must be reset after each frame is received
        """
        self.parser = StompFrameLineParser()
        #We need to override the default delimiter for LineOnlyReceiver
        self.delimiter = self.parser.lineDelimiter
    
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
        self.log.debug("Handler complete for message: %s" % messageId)

    def handlerStarted(self, messageId):
        if messageId in self.activeHandlers:
            raise StompProtocolError('Duplicate message received. Message id %s is already in progress' % messageId)
        self.activeHandlers[messageId] = None
        self.log.debug("Handler started for message: %s" % messageId)
    
    def messageHandlerFailed(self, failure, messageId, msg, errDest):
        self.log.error("Error in message handler: %s" % str(failure))
        #Forward message to error queue if configured
        if errDest is not None:
            errMsg = cloneStompMessageForErrorDest(msg)
            self.send(errDest, errMsg['body'], errMsg['headers'])
            self._ack(messageId)
        #Set disconnect error
        self.disconnectError = failure
        #Disconnect
        self.disconnect()
        return failure
        
    def connectTimeout(self, timeout):
        self.log.error("Connect command timed out after %s seconds" % timeout)
        self.connectTimeoutDelayedCall = None
        self.connectError = StompConnectTimeout("Connect command timed out after %s seconds" % timeout)
        self.transport.loseConnection()
        
    def handleConnected(self, msg):
        """Handle STOMP CONNECTED commands
        """
        sessionId = msg['headers'].get('session', None)
        self.log.debug("Connected to stomp broker with session: %s" % sessionId)
        #Remove connect timeout if set
        if self.connectTimeoutDelayedCall is not None:
            self.log.debug("Cancelling connect timeout after sucessfully connecting")
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
            self.log.debug("Disconnecting...ignoring stomp message: %s at destination: %s" % (messageId, dest))
            return

        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("Received stomp message %s from destination %s: [%s...].  Headers: %s" % (messageId, dest, msg['body'][:20], msg['headers']))
        
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
        self.log.info("Received stomp error: %s" % msg)
        if self.connectedDeferred is not None:
            self.transport.loseConnection()
            self.connectError = StompProtocolError("STOMP error message received while trying to connect: %s" % msg)
        else:
            #Work around for AMQ < 5.2
            if 'message' in msg['headers'] and msg['headers']['message'].find('Unexpected ACK received for message-id') >= 0:
                self.log.debug('AMQ brokers < 5.2 do not support client-individual mode.')
            else:
                #Set disconnect error
                self.disconnectError = StompProtocolError("STOMP error message received: %s" % msg)
                #Disconnect
                self.disconnect()
        
    def handleReceipt(self, msg):
        """Handle STOMP RECEIPT commands
        """
        self.log.info("Received stomp receipt: %s" % msg)
    
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
            self.log.debug("Preparing to disconnect")
            self.disconnecting = True
            #Send disconnect command after outstanding messages are ack'ed
            defer.maybeDeferred(self.finishHandlers).addBoth(lambda result: self._disconnect())

        return self.disconnectedDeferred
    
    def subscribe(self, dest, handler, headers={}, **kwargs):
        """Subscribe to a destination and register a function handler to receive messages for that destination
        """
        errorDestination = kwargs.get('errorDestination', None);
        # client-individual mode is only supported in AMQ >= 5.2
        # headers['ack'] = headers.get('ack', 'client-individual')
        headers['ack'] = headers.get('ack', 'client')
        self.destMap[dest] = {'handler': handler, 'ack': headers['ack'], 'errorDestination': errorDestination}
        self._subscribe(dest, headers)
    
    def send(self, dest, msg, headers={}):
        """Do the send command to enqueue a message to a destination
        """
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("Sending message to %s: [%s...]" % (dest, msg[:20]))
        frame = stomper.Frame()
        frame.cmd = 'SEND'
        frame.headers = headers
        frame.headers['destination'] = dest
        frame.body = msg
        cmd = frame.pack()
        # self.log.debug("Writing cmd: %s" % cmd)
        self.transport.write(cmd)
        
    def getDisconnectedDeferred(self):
        return self.disconnectedDeferred
    
class StompClientFactory(ClientFactory):

    protocol = StompClient

    def __init__(self, **kwargs):
        self.login = kwargs.get('login', '');
        self.passcode = kwargs.get('passcode', '');
        self.buildProtocolDeferred = defer.Deferred()
        self.log = logging.getLogger(LOG_CATEGORY)
    
    def getLogin(self):
        return self.login

    def getPasscode(self):
        return self.passcode

    def buildProtocol(self, addr):
        p = ClientFactory.buildProtocol(self, addr)
        #This is a sneaky way of passing the protocol instance back to the caller
        reactor.callLater(0, self.buildProtocolDeferred.callback, p)
        return p
    
    def clientConnectionFailed(self, connector, reason):
        """Connection failed
        """
        self.log.error("Connection failed. Reason: %s" % str(reason))
        self.buildProtocolDeferred.errback(reason)

class StompConfig(object):

    def __init__(self, host, port, **kwargs):
        self.host = host
        self.port = port
        self.login = kwargs.get('login', '');
        self.passcode = kwargs.get('passcode', '');
        self.log = logging.getLogger(LOG_CATEGORY)

class StompCreator(object):

    def __init__(self, config, **kwargs):
        self.config = config
        self.connectTimeout = kwargs.get('connectTimeout', None);
        self.log = logging.getLogger(LOG_CATEGORY)
        self.stompConnectedDeferred = None

    def getConnection(self):
        self.stompConnectedDeferred = defer.Deferred()
        self.connect().addCallback(self.connected).addErrback(self.stompConnectedDeferred.errback)
        return self.stompConnectedDeferred

    def connect(self):
        factory = StompClientFactory(login=self.config.login, passcode=self.config.passcode)
        reactor.connectTCP(self.config.host, self.config.port, factory) 
        return factory.buildProtocolDeferred
        
    def connected(self, stomp):
        stomp.connect(self.connectTimeout).addCallback(self.stompConnected).addErrback(self.stompConnectedDeferred.errback)

    def stompConnected(self, stomp):
        self.stompConnectedDeferred.callback(stomp)
        self.stompConnectedDeferred = None
