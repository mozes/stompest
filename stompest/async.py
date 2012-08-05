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
import warnings

from twisted.internet import defer, reactor
from twisted.internet.endpoints import clientFromString
from twisted.internet.error import ConnectionLost
from twisted.internet.protocol import Factory, Protocol

from stompest.error import StompConnectTimeout, StompError, StompFrameError, StompProtocolError
from stompest.protocol import commands
from stompest.protocol.frame import StompFrame
from stompest.protocol.parser import StompParser
from stompest.protocol.session import StompSession
from stompest.protocol.spec import StompSpec
from stompest.util import cloneStompMessage as _cloneStompMessage

LOG_CATEGORY = 'stompest.async'

class StompClient(Protocol):
    """A Twisted implementation of a STOMP client"""
    MESSAGE_INFO_LENGTH = 20
    CLIENT_ACK_MODES = set(['client', 'client-individual'])

    def __init__(self, alwaysDisconnectOnUnhandledMsg=False):
        self.alwaysDisconnectOnUnhandledMsg = alwaysDisconnectOnUnhandledMsg
        
        self.log = logging.getLogger(LOG_CATEGORY)
        self.cmdMap = {
            'MESSAGE': self.handleMessage,
            'CONNECTED': self.handleConnected,
            'ERROR': self.handleError,
            'RECEIPT': self.handleReceipt,
        }
        self.destMap = {}
        self.connectedDeferred = None
        self.connectTimeoutDelayedCall = None
        self.connectError = None
        self.disconnectedDeferred = None
        self.finishedHandlersDeferred = None
        self.disconnecting = False
        self.disconnectError = None
        self.activeHandlers = set()
        self.resetParser()

    #
    # Overriden methods from parent protocol class
    #
        
    def connectionLost(self, reason):
        """When TCP connection is lost, remove shutdown handler
        """
        if reason.type == ConnectionLost:
            msg = 'Disconnected'
        else:
            msg = 'Disconnected: %s' % reason.getErrorMessage()
        self.log.debug(msg)
            
        self.cancelConnectTimeout('network connection was lost')
        self.handleConnectionLostConnect()
        self.handleConnectionLostDisconnect()
            
        Protocol.connectionLost(self, reason)
    
    def cancelConnectTimeout(self, reason):
        if not self.connectTimeoutDelayedCall:
            return
        self.log.debug('Cancelling connect timeout [%s]' % reason)
        self.connectTimeoutDelayedCall.cancel()
        self.connectTimeoutDelayedCall = None
    
    def handleConnectionLostDisconnect(self):
        if not self.disconnectedDeferred:
            return
        if self.disconnectError:
            #self.log.debug('Calling disconnectedDeferred errback: %s' % self.disconnectError)
            self.disconnectedDeferred.errback(self.disconnectError)
            self.disconnectError = None
        else:
            #self.log.debug('Calling disconnectedDeferred callback')
            self.disconnectedDeferred.callback(self)
        self.disconnectedDeferred = None
            
    def handleConnectionLostConnect(self):
        if not self.connectedDeferred:
            return
        if self.connectError:
            error, self.connectError = self.connectError, None
        else:
            self.log.error('Connection lost with outstanding connectedDeferred')
            error = StompError('Unexpected connection loss')
        self.log.debug('Calling connectedDeferred errback: %s' % error)
        self.connectedDeferred.errback(error)                
        self.connectedDeferred = None
    
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
    def _connect(self, login, passcode):
        """Send connect command
        """
        self.log.debug('Sending connect command')
        self.sendFrame(commands.connect(login, passcode))

    def _disconnect(self):
        """Send disconnect command
        """
        self.log.debug('Sending disconnect command')
        self.sendFrame(commands.disconnect())

    def _subscribe(self, dest, headers):
        """Send subscribe command
        """
        ack = headers.get(StompSpec.ACK_HEADER)
        self.log.debug('Sending subscribe command for destination %s with ack mode %s' % (dest, ack))
        headers[StompSpec.DESTINATION_HEADER] = dest
        self.sendFrame(commands.subscribe(headers))

    def _ack(self, messageId):
        """Send ack command
        """
        self.log.debug('Sending ack command for message: %s' % messageId)
        self.sendFrame(commands.ack({StompSpec.MESSAGE_ID_HEADER: messageId}))
    
    def _write(self, data):
        #self.log.debug('sending data:\n%s' % data)
        self.transport.write(data)

    def _toFrame(self, message):
        if not isinstance(message, StompFrame):
            message = StompFrame(**message)
        return message
            
    #
    # Private helper methods
    #
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
        return bool(self.activeHandlers)
    
    def handlerFinished(self, messageId):
        self.activeHandlers.remove(messageId)
        self.log.debug('Handler complete for message: %s' % messageId)

    def handlerStarted(self, messageId):
        if messageId in self.activeHandlers:
            raise StompProtocolError('Duplicate message received. Message id %s is already in progress' % messageId)
        self.activeHandlers.add(messageId)
        self.log.debug('Handler started for message: %s' % messageId)
    
    def messageHandlerFailed(self, failure, messageId, msg, errDest):
        self.log.error('Error in message handler: %s' % failure)
        if errDest: #Forward message to error queue if configured
            errorMessage = _cloneStompMessage(msg, persistent=True)
            self.send(errDest, errorMessage['body'], errorMessage['headers'])
            self._ack(messageId)
            if not self.alwaysDisconnectOnUnhandledMsg:
                return
        self.disconnectError = failure
        self.disconnect()

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
        self.cancelConnectTimeout('successfully connected')
        self.disconnectedDeferred = defer.Deferred()
        self.connectedDeferred.callback(self)
        self.connectedDeferred = None
    
    @defer.inlineCallbacks
    def handleMessage(self, msg):
        """Handle STOMP MESSAGE commands
        """
        dest = msg['headers'][StompSpec.DESTINATION_HEADER]
        messageId = msg['headers'][StompSpec.MESSAGE_ID_HEADER]
        errDest = self.destMap[dest]['errorDestination']

        #Do not process any more messages if we're disconnecting
        if self.disconnecting:
            self.log.debug('Disconnecting...ignoring stomp message: %s at destination: %s' % (messageId, dest))
            return

        self.log.debug('Received stomp message %s from destination %s: [%s...].  Headers: %s' % (messageId, dest, msg['body'][:self.MESSAGE_INFO_LENGTH], msg['headers']))
        
        #Call message handler (can return deferred to be async)
        self.handlerStarted(messageId)
        try:
            yield defer.maybeDeferred(self.destMap[dest]['handler'], self, msg)
        except Exception as e:
            self.messageHandlerFailed(e, messageId, msg, errDest)
        else:
            if self.clientAck(dest):
                self._ack(messageId)
        finally:
            self.postProcessMessage(messageId)
        
    def clientAck(self, dest):
        return self.destMap[dest][StompSpec.ACK_HEADER] in self.CLIENT_ACK_MODES

    def postProcessMessage(self, messageId):
        self.handlerFinished(messageId)
        #If someone's waiting to know that all handlers are done, call them back
        if self.finishedHandlersDeferred and not self.handlersInProgress():
            self.finishedHandlersDeferred.callback(self)
            self.finishedHandlersDeferred = None

    def handleError(self, msg):
        """Handle STOMP ERROR commands
        """
        self.log.info('Received stomp error: %s' % msg)
        if self.connectedDeferred:
            self.transport.loseConnection()
            self.connectError = StompProtocolError('STOMP error message received while trying to connect: %s' % msg)
        else:
            #Workaround for AMQ < 5.2
            if 'Unexpected ACK received for message-id' in msg['headers'].get('message', ''):
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
    def connect(self, login, passcode, timeout):
        """Send connect command and return Deferred for caller that will get trigger when connect is complete
        """
        if timeout is not None:
            self.connectTimeoutDelayedCall = reactor.callLater(timeout, self.connectTimeout, timeout)
        self._connect(login, passcode)
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
        # headers[StompSpec.ACK_HEADER] = headers.get(StompSpec.ACK_HEADER, 'client-individual')
        headers = dict(headers or {})
        headers[StompSpec.ACK_HEADER] = headers.get(StompSpec.ACK_HEADER, 'client')
        self.destMap[dest] = {'handler': handler, StompSpec.ACK_HEADER: headers[StompSpec.ACK_HEADER], 'errorDestination': errorDestination}
        self._subscribe(dest, headers)
    
    def send(self, dest, msg='', headers=None):
        """Do the send command to enqueue a message to a destination
        """
        headers = dict(headers or {})
        self.log.debug('Sending message to %s: [%s...]' % (dest, msg[:self.MESSAGE_INFO_LENGTH]))
        self.sendFrame(commands.send(dest, msg, headers))
    
    def sendFrame(self, message):
        self._write(str(self._toFrame(message)))
            
    def getDisconnectedDeferred(self):
        return self.disconnectedDeferred
    
class StompConfig(object):
    def __init__(self, host=None, port=None, uri=None, login='', passcode=''):
        if not uri:
            if not (host and port):
                raise ValueError('host and port missing')
            uri = 'tcp://%s:%d' % (host, port)
            warnings.warn('host and port arguments are deprecated. use uri=%s instead!' % uri)
        self.uri = uri
        self.login = login
        self.passcode = passcode

class StompCreator(object):
    def __init__(self, config, connectTimeout=None, **kwargs):
        self.config = config
        self.connectTimeout = connectTimeout
        self.kwargs = kwargs
        self.log = logging.getLogger(LOG_CATEGORY)
        
    @defer.inlineCallbacks  
    def getConnection(self):
        session = StompSession(self.config.uri, self._getEndpoint)
        try:
            for (endpoint, connectDelay) in session.connections():
                if connectDelay:
                    self.log.debug('delaying connect attempt for %d ms' % int(connectDelay * 1000))
                    yield self._sleep(connectDelay)
                    
                try:
                    stomp = yield self._getConnection(endpoint)
                    yield stomp.connect(self.config.login, self.config.passcode, timeout=self.connectTimeout)
                    defer.returnValue(stomp)
                    
                except Exception as e:
                    self.log.warning('%s [%s]' % ('could not connect to %(host)s:%(port)d' % endpoint.broker, e))
                
        except Exception as e:
            self.log.error('reconnect failed [%s]' % e)
            raise
        
    def _getConnection(self, endpoint):
        factory = StompFactory(**self.kwargs)
        return endpoint.connect(factory)
    
    def _getEndpoint(self, broker):
        endpoint = clientFromString(reactor, 'tcp:host=%(host)s:port=%(port)d' % broker)
        endpoint.broker = broker
        return endpoint
    
    def _sleep(self, delay):
        sleep = defer.Deferred()
        reactor.callLater(delay, sleep.callback, None)
        return sleep

class StompFactory(Factory):
    protocol = StompClient
    
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        
    def buildProtocol(self, addr):
        protocol = self.protocol(**self.kwargs)
        protocol.factory = self
        return protocol
