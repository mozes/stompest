"""
Twisted STOMP client

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
import functools
import logging

from twisted.internet import defer, reactor, task
from twisted.internet.error import ConnectionLost
from twisted.internet.protocol import Factory, Protocol

from stompest.error import StompConnectionError, StompConnectTimeout, StompFrameError, StompProtocolError
from stompest.protocol import commands, StompFailoverProtocol, StompParser, StompSession, StompSpec
from stompest.util import cloneFrame

from .util import endpointFactory, exclusive

LOG_CATEGORY = 'stompest.async.client'

class Stomp(object):
    @classmethod
    def endpointFactory(cls, broker):
        return endpointFactory(broker)
    
    def __init__(self, config, connectTimeout=None, version=None, **kwargs):
        self._config = config
        self._connectTimeout = connectTimeout
        self._kwargs = kwargs
        
        self.session = StompSession(version)
        self._failover = StompFailoverProtocol(config.uri)

        self.log = logging.getLogger(LOG_CATEGORY)
        
    @exclusive
    @defer.inlineCallbacks
    def connect(self, headers=None, versions=None, host=None):
        try:
            try:
                self._protocol
            except StompConnectionError:
                frame = self.session.connect(self._config.login, self._config.passcode, headers, versions, host)
                yield self._connect(frame)
            defer.returnValue(self)
        except Exception as e:
            self.log.error('Connect failed [%s]' % e)
            raise
    
    @exclusive
    @defer.inlineCallbacks
    def disconnect(self, failure=None):
        yield self._protocol.disconnect(failure)
        defer.returnValue(None)
    
    @property
    def disconnected(self):
        return self._protocol and self._protocol.disconnected
    
    # STOMP commands
    
    def send(self, destination, body='', headers=None):
        self._protocol.send(destination, body, headers)
        
    def ack(self, frame):
        self._protocol.sendFrame(self.session.ack(frame))
    
    def nack(self, frame):
        self._protocol.sendFrame(self.session.nack(frame))
    
    def begin(self):
        protocol = self._protocol
        frame, token = self.session.begin()
        protocol.sendFrame(frame)
        return token
        
    def abort(self, transaction):
        protocol = self._protocol
        frame, token = self.session.abort(transaction)
        protocol.sendFrame(frame)
        return token
        
    def commit(self, transaction):
        protocol = self._protocol
        frame, token = self.session.commit(transaction)
        protocol.sendFrame(frame)
        return token
    
    def subscribe(self, destination, handler, headers=None, receipt=None, errorDestination=None):
        protocol = self._protocol
        if not callable(handler):
            raise ValueError('Cannot subscribe (handler is missing): %s' % handler)
        frame, token = self.session.subscribe(destination, headers, context={'handler': handler, 'receipt': receipt, 'errorDestination': errorDestination})
        protocol.subscribe(token, frame, self._createHandler(handler), errorDestination)
        return token
    
    def unsubscribe(self, token, receipt=None):
        frame = self.session.unsubscribe(token, receipt)
        return self._protocol.unsubscribe(token, frame)
    
    # private methods
    
    @defer.inlineCallbacks
    def _connect(self, frame):
        for (broker, delay) in self._failover:
            yield self._sleep(delay)
            endpoint = self.endpointFactory(broker)
            self.log.debug('Connecting to %(host)s:%(port)s ...' % broker)
            try:
                protocol = yield endpoint.connect(StompFactory(self, **self._kwargs))
            except Exception as e:
                self.log.warning('%s [%s]' % ('Could not connect to %(host)s:%(port)d' % broker, e))
                continue
            frame = yield protocol.connect(frame, self._connectTimeout)
            self.session.connected(frame)
            self._protocol = protocol
            self._replay()

            self.disconnected.addBoth(self._handleDisconnected)
            defer.returnValue(None)
    
    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler
    
    def _handleDisconnected(self, result):
        self._protocol = None
        return result
    
    def _replay(self):
        for (destination, headers, context) in self.session.replay():
            self.log.debug('Replaying subscription: %s' % headers)
            self.subscribe(destination, headers=headers, **context)
    
    def _sleep(self, delay):
        if not delay:
            return
        self.log.debug('Delaying connect attempt for %d ms' % int(delay * 1000))
        return task.deferLater(reactor, delay, lambda: None)
    
    @property
    def _protocol(self):
        try:
            protocol = self.__protocol
        except AttributeError:
            protocol = self.__protocol = None
        if not protocol:
            raise StompConnectionError('Not connected')
        return protocol
        
    @_protocol.setter
    def _protocol(self, protocol):
        self.__protocol = protocol

class StompProtocol(Protocol):
    #
    # Overriden methods from parent protocol class
    #
        
    def connectionLost(self, reason):
        """When TCP connection is lost, remove shutdown handler
        """
        message = 'Disconnected'
        if reason.type is not ConnectionLost:
            message = '%s: %s' % (message, reason.getErrorMessage())
        self.log.debug(message)
        
        self._cancelConnectTimeout('Network connection was lost')
        self._handleConnectionLostConnect()
        self._handleConnectionLostDisconnect()
        
        Protocol.connectionLost(self, reason)
    
    def dataReceived(self, data):
        self._parser.add(data)
                
        while True:
            frame = self._parser.get()
            if not frame:
                break
            self.log.debug('Received %s' % frame.info())
            try:
                handler = self._handlers[frame.command]
            except KeyError:
                raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
            handler(frame)
    
    CLIENT_ACK_MODES = set(['client', 'client-individual'])
    DEFAULT_ACK_MODE = 'client'
    
    def __init__(self, stomp, **kwargs):
        self.stomp = stomp
        self._alwaysDisconnectOnUnhandledMsg = kwargs.get('alwaysDisconnectOnUnhandledMsg', False)
        
        # leave the used logger public in case the user wants to override it
        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._handlers = {
            'MESSAGE': self._handleMessage,
            'CONNECTED': self._handleConnected,
            'ERROR': self._handleError,
            'RECEIPT': self._handleReceipt,
        }
        self._subscriptions = {}
        self._connectedSignal = None
        self._scheduledConnectTimeout = None
        self._connectError = None
        self._disconnectedSignal = None
        self._finishedHandlersDeferred = None
        self._disconnecting = False
        self._disconnectError = None
        self._activeHandlers = set()
        self._parser = StompParser()
    
    #
    # user interface
    #
    @exclusive
    @defer.inlineCallbacks
    def connect(self, frame, timeout=None):
        """Send connect command and return Deferred for caller that will get trigger when connect is complete
        """
        if timeout is not None:
            self._scheduledConnectTimeout = reactor.callLater(timeout, self._connectTimeout, timeout) #@UndefinedVariable
        self.sendFrame(frame)
        self._connectedSignal = defer.Deferred()
        result = yield self._connectedSignal
        defer.returnValue(result)
    
    def disconnect(self, receipt=None, failure=None):
        """After finishing outstanding requests, send disconnect command and return Deferred for caller that will get trigger when disconnect is complete
        """
        if failure:
            self._disconnectError = failure
        if not self._disconnecting:
            self._disconnecting = True
            #Send disconnect command after outstanding messages are ack'ed
            defer.maybeDeferred(self._finishHandlers).addBoth(lambda _: self._disconnect(receipt))
            
        return self._disconnectedSignal
    
    def subscribe(self, token, frame, handler, errorDestination):
        """Subscribe to a destination and register a function handler to receive messages for that destination
        """
        headers = frame.headers
        ack = headers.setdefault(StompSpec.ACK_HEADER, self.DEFAULT_ACK_MODE)
        self._subscriptions[token] = {'destination': headers[StompSpec.DESTINATION_HEADER], 'handler': self._createHandler(handler), 'ack': ack, 'errorDestination': errorDestination}
        self.sendFrame(frame)
    
    def unsubscribe(self, token, frame):
        try:
            self._subscriptions.pop(token)
        except:
            self.log.warning('Cannot unsubscribe (subscription id unknown): %s=%s' % token)
            raise
        self.sendFrame(frame)
    
    def send(self, destination, body='', headers=None, receipt=None):
        self.sendFrame(commands.send(destination, body, headers, receipt))
    
    def sendFrame(self, frame):
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Sending %s' % frame.info())
        self._write(str(frame))
    
    @property
    def disconnected(self):
        return self._disconnectedSignal
    
    #
    # Methods for sending raw STOMP commands
    #
    def _disconnect(self, receipt=None):
        self.sendFrame(self.stomp.session.disconnect(receipt))
        self.transport.loseConnection()
        if not self._disconnectError:
            self.stomp.session.flush()
            
    def ack(self, frame):
        self.sendFrame(self.stomp.session.ack(frame))
    
    def _write(self, data):
        #self.log.debug('sending data:\n%s' % repr(data))
        self.transport.write(data)

    #
    # Private helper methods
    #
    def _cancelConnectTimeout(self, reason):
        if not self._scheduledConnectTimeout:
            return
        self.log.debug('Cancelling connect timeout [%s]' % reason)
        self._scheduledConnectTimeout.cancel()
        self._scheduledConnectTimeout = None
    
    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler

    def _handleConnectionLostDisconnect(self):
        if not self._disconnectedSignal:
            return
        if not self._disconnecting:
            self._disconnectError = StompConnectionError('Unexpected connection loss')
        if self._disconnectError:
            #self.log.debug('Calling disconnected deferred errback: %s' % self._disconnectError)
            self._disconnectedSignal.errback(self._disconnectError)
            self._disconnectError = None
        else:
            #self.log.debug('Calling disconnected deferred callback')
            self._disconnectedSignal.callback(self)
        self._disconnectedSignal = None
            
    def _handleConnectionLostConnect(self):
        if not self._connectedSignal:
            return
        if self._connectError:
            error, self._connectError = self._connectError, None
        else:
            self.log.error('Connection lost before connection was established')
            error = StompConnectionError('Unexpected connection loss')
        self.log.debug('Calling connected deferred errback: %s' % error)
        self._connectedSignal.errback(error)                
        self._connectedSignal = None
    
    def _finishHandlers(self):
        """Return a Deferred to signal when all requests in process are complete
        """
        if self._handlersInProgress():
            self._finishedHandlersDeferred = defer.Deferred()
            return self._finishedHandlersDeferred
    
    def _handlersInProgress(self):
        return bool(self._activeHandlers)
    
    def _handlerFinished(self, messageId):
        self._activeHandlers.remove(messageId)
        self.log.debug('Handler complete for message: %s' % messageId)

    def _handlerStarted(self, messageId):
        if messageId in self._activeHandlers:
            raise StompProtocolError('Duplicate message received. Message id %s is already in progress' % messageId)
        self._activeHandlers.add(messageId)
        self.log.debug('Handler started for message: %s' % messageId)
    
    def _messageHandlerFailed(self, failure, frame, errorDestination):
        self.log.error('Error in message handler: %s' % failure)
        if errorDestination: #Forward message to error queue if configured
            errorMessage = cloneFrame(frame, persistent=True)
            self.send(errorDestination, errorMessage.body, errorMessage.headers)
            self.ack(frame)
            if not self._alwaysDisconnectOnUnhandledMsg:
                return
        self.disconnect(failure=failure)

    def _connectTimeout(self, timeout):
        self.log.error('Connect command timed out after %s seconds' % timeout)
        self._scheduledConnectTimeout = None
        self._connectError = StompConnectTimeout('Connect command timed out after %s seconds' % timeout)
        self.transport.loseConnection()
    
    def _handleConnected(self, frame):
        """Handle STOMP CONNECTED commands
        """
        self.log.debug('Connected to stomp broker with session: %s' % self.stomp.session.id)
        self._cancelConnectTimeout('Successfully connected')
        self._disconnectedSignal = defer.Deferred()
        self._connectedSignal.callback(frame)
        self._connectedSignal = None
    
    @defer.inlineCallbacks
    def _handleMessage(self, frame):
        """Handle STOMP MESSAGE commands
        """
        headers = frame.headers
        messageId = headers[StompSpec.MESSAGE_ID_HEADER]
        try:
            token = self.stomp.session.message(frame)
            subscription = self._subscriptions[token]
        except:
            self.log.warning('[%s] No handler found: ignoring %s' % (messageId, frame.info()))
            return
        
        #Do not process any more messages if we're disconnecting
        if self._disconnecting:
            self.log.debug('[%s] Disconnect: ignoring %s' % (messageId, frame.info()))
            return
        
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('[%s] Received %s' % (messageId, frame.info()))
        
        #Call message handler (can return deferred to be async)
        self._handlerStarted(messageId)
        try:
            yield defer.maybeDeferred(subscription['handler'], self, frame)
        except Exception as e:
            self._messageHandlerFailed(e, frame, subscription['errorDestination'])
        else:
            if self._isClientAck(subscription):
                self.ack(frame)
        finally:
            self._postProcessMessage(messageId)
        
    def _isClientAck(self, subscription):
        return subscription['ack'] in self.CLIENT_ACK_MODES

    def _postProcessMessage(self, messageId):
        self._handlerFinished(messageId)
        self._finish()
        
    def _finish(self):
        #If someone's waiting to know that all handlers are done, call them back
        if (not self._finishedHandlersDeferred) or self._handlersInProgress():
            return
        self._finishedHandlersDeferred.callback(self)
        self._finishedHandlersDeferred = None
        
    def _handleError(self, frame):
        """Handle STOMP ERROR commands
        """
        self.log.info('Received %s' % frame.info())
        if self._connectedSignal:
            self.transport.loseConnection()
            self._connectError = StompProtocolError('While trying to connect, received %s' % frame.info())
        else:
            #Workaround for AMQ < 5.2
            if 'Unexpected ACK received for message-id' in frame.headers.get('message', ''):
                self.log.debug('AMQ brokers < 5.2 do not support client-individual mode.')
            else:
                self._disconnectError = StompProtocolError('Received %s' % frame.info())
                self.disconnect()
        
    def _handleReceipt(self, frame):
        """Handle STOMP RECEIPT commands
        """
        self.log.info('Received %s' % frame.info())

class StompFactory(Factory):
    protocol = StompProtocol
    
    def __init__(self, stomp, **kwargs):
        self.stomp = stomp
        self.kwargs = kwargs
        
    def buildProtocol(self, _):
        protocol = self.protocol(self.stomp, **self.kwargs)
        protocol.factory = self
        return protocol
