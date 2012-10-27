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
    CLIENT_ACK_MODES = set(['client', 'client-individual'])
    DEFAULT_ACK_MODE = 'client'
    
    @classmethod
    def endpointFactory(cls, broker):
        return endpointFactory(broker)
    
    def __init__(self, config, connectTimeout=None, version=None, **kwargs):
        self._alwaysDisconnectOnUnhandledMsg = kwargs.get('alwaysDisconnectOnUnhandledMsg', False)
        
        self._config = config
        self._connectTimeout = connectTimeout
        
        self.session = StompSession(version)
        self._failover = StompFailoverProtocol(config.uri)

        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._handlers = {
            'MESSAGE': self._onMessage,
            'CONNECTED': self._onConnected,
            'ERROR': self._onError,
            'RECEIPT': self._onReceipt,
        }
        self._subscriptions = {}

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
        return self._protocol and self._protocol.disconnectedSignal
    
    # STOMP commands
    
    def sendFrame(self, frame):
        self._protocol.send(frame)
    
    def send(self, destination, body='', headers=None, receipt=None):
        self._protocol.send(commands.send(destination, body, headers, receipt))
        
    def ack(self, frame):
        self._protocol.send(self.session.ack(frame))
    
    def nack(self, frame):
        self._protocol.send(self.session.nack(frame))
    
    def begin(self):
        protocol = self._protocol
        frame, token = self.session.begin()
        protocol.send(frame)
        return token
        
    def abort(self, transaction):
        protocol = self._protocol
        frame, token = self.session.abort(transaction)
        protocol.send(frame)
        return token
        
    def commit(self, transaction):
        protocol = self._protocol
        frame, token = self.session.commit(transaction)
        protocol.send(frame)
        return token
    
    def subscribe(self, destination, handler, headers=None, receipt=None, errorDestination=None):
        protocol = self._protocol
        if not callable(handler):
            raise ValueError('Cannot subscribe (handler is missing): %s' % handler)
        frame, token = self.session.subscribe(destination, headers, context={'handler': handler, 'receipt': receipt, 'errorDestination': errorDestination})
        ack = frame.headers.setdefault(StompSpec.ACK_HEADER, self.DEFAULT_ACK_MODE)
        self._subscriptions[token] = {'destination': destination, 'handler': self._createHandler(handler), 'ack': ack, 'errorDestination': errorDestination}
        protocol.send(frame)
        return token
    
    def unsubscribe(self, token, receipt=None):
        protocol = self._protocol
        frame = self.session.unsubscribe(token, receipt)
        try:
            self._subscriptions.pop(token)
        except:
            self.log.warning('Cannot unsubscribe (subscription id unknown): %s=%s' % token)
            raise
        protocol.send(frame)
    
    # private methods
    
    @defer.inlineCallbacks
    def _connect(self, frame):
        for (broker, delay) in self._failover:
            yield self._sleep(delay)
            endpoint = self.endpointFactory(broker)
            self.log.debug('Connecting to %(host)s:%(port)s ...' % broker)
            try:
                protocol = yield endpoint.connect(StompFactory(self._onFrame, self._onDisconnect))
            except Exception as e:
                self.log.warning('%s [%s]' % ('Could not connect to %(host)s:%(port)d' % broker, e))
                continue
            protocol.send(frame)
            self._protocol = yield protocol.connect(self._connectTimeout)
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
    
    # callbacks for received STOMP frames
    
    def _onFrame(self, protocol, frame):
        self.log.info('Received %s' % frame.info())
        try:
            handler = self._handlers[frame.command]
        except KeyError:
            raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
        return handler(protocol, frame)
    
    def _onConnected(self, protocol, frame):
        self.log.debug('Connected to stomp broker with session: %s' % self.session.id)
        self.session.connected(frame)
        protocol.onConnected()

    def _onError(self, protocol, frame):
        if protocol.connectedSignal:
            protocol.abortConnect(StompProtocolError('While trying to connect, received %s' % frame.info()))
        else:
            #Workaround for AMQ < 5.2
            if 'Unexpected ACK received for message-id' in frame.headers.get('message', ''):
                self.log.debug('AMQ brokers < 5.2 do not support client-individual mode.')
            else:
                protocol.disconnect(failure=StompProtocolError('Received %s' % frame.info()))
        
    @defer.inlineCallbacks
    def _onMessage(self, protocol, frame):
        headers = frame.headers
        messageId = headers[StompSpec.MESSAGE_ID_HEADER]
        try:
            token = self.session.message(frame)
            subscription = self._subscriptions[token]
        except:
            self.log.warning('[%s] No handler found: ignoring %s' % (messageId, frame.info()))
            return
        
        try:
            protocol.handlerStarted(messageId)
        except StompConnectionError: # disconnecting
            return
        
        # call message handler (can return deferred to be async)
        try:
            yield defer.maybeDeferred(subscription['handler'], self, frame)
        except Exception as e:
            self._onMessageFailed(e, frame, subscription['errorDestination'])
            if not self._alwaysDisconnectOnUnhandledMsg: # TODO: introduce a callback for this
                return
            protocol.disconnect(failure=e)
        else:
            if subscription['ack'] in StompSpec.CLIENT_ACK_MODES:
                self.ack(frame)
        finally:
            protocol.handlerFinished(messageId)
        
    def _onReceipt(self, protocol, frame):
        return frame
    
    # other callbacks for protocol

    def _onDisconnect(self, protocol, error):
        frame = self.session.disconnect()
        if not error:
            self.session.flush()
        protocol.send(frame)
        protocol.loseConnection()
    
    # more stuff
    
    def _onMessageFailed(self, failure, frame, errorDestination):
        self.log.error('Error in message handler: %s' % failure)
        if not errorDestination: # forward message to error queue if configured
            return
        errorMessage = cloneFrame(frame, persistent=True)
        self.send(errorDestination, errorMessage.body, errorMessage.headers)
        self.ack(frame)
 
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
            
            self._onFrame(self, frame)
    
    def __init__(self, onFrame, onDisconnect):
        self._onFrame = onFrame
        self._onDisconnect = onDisconnect
        
        # leave the used logger public in case the user wants to override it
        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._parser = StompParser()
        
        self._connectedSignal = None
        self._scheduledConnectTimeout = None
        self._connectError = None
        
        self._disconnectedSignal = None
        self._disconnecting = False
        self._disconnectError = None
        
        # keep track of active handlers for graceful disconnect
        self._activeHandlers = set()
        self._allHandlersFinishedSignal = None
        
    #
    # user interface
    #
    def send(self, frame):
        #self.log.debug('sending data:\n%s' % repr(data))
        self.transport.write(str(frame))

    @exclusive
    @defer.inlineCallbacks
    def connect(self, timeout=None):
        """Send connect command and return Deferred for caller that will be triggered when connect is complete
        """
        if timeout is not None:
            self._scheduledConnectTimeout = reactor.callLater(timeout, self._connectTimeout, timeout) #@UndefinedVariable
        self._connectedSignal = defer.Deferred()
        result = yield self._connectedSignal
        defer.returnValue(result)
    
    def disconnect(self, failure=None):
        """After finishing outstanding requests, send disconnect command and return Deferred for caller that will be triggered when disconnect is complete
        """
        if failure:
            self._disconnectError = failure
        if not self._disconnecting:
            self._disconnecting = True
            #Send disconnect command after outstanding messages are ack'ed
            defer.maybeDeferred(self._finishHandlers).addBoth(lambda _: self._onDisconnect(self, self._disconnectError))
            
        return self._disconnectedSignal
    
    @property
    def connectedSignal(self):
        return self._connectedSignal
    
    @property
    def disconnectedSignal(self):
        return self._disconnectedSignal
    
    def loseConnection(self):
        self.transport.loseConnection()
        
    def abortConnect(self, error):
        self._connectError = error
        self.transport.loseConnection()

    def onConnected(self):
        self._cancelConnectTimeout('Successfully connected')
        self._disconnectedSignal = defer.Deferred()
        self._connectedSignal.callback(self)
        self._connectedSignal = None
    
    def handlerStarted(self, messageId):
        # do not process any more messages if we're disconnecting
        if self._disconnecting:
            message = 'Ignoring message %s (disconnecting)' % messageId
            self.log.debug(message)
            raise StompConnectionError(message)
        
        if messageId in self._activeHandlers:
            raise StompProtocolError('Duplicate message id %s received. Message is already in progress.' % messageId)
        self._activeHandlers.add(messageId)
        self.log.debug('Handler started for message: %s' % messageId)
    
    def handlerFinished(self, messageId):
        self._activeHandlers.remove(messageId)
        self.log.debug('Handler complete for message: %s' % messageId)
        self._finish()

    #
    # Private helper methods
    #
    def _cancelConnectTimeout(self, reason):
        if not self._scheduledConnectTimeout:
            return
        self.log.debug('Cancelling connect timeout [%s]' % reason)
        self._scheduledConnectTimeout.cancel()
        self._scheduledConnectTimeout = None
    
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
            
    def _connectTimeout(self, timeout):
        self.log.error('Connect command timed out after %s seconds' % timeout)
        self._scheduledConnectTimeout = None
        self._connectError = StompConnectTimeout('Connect command timed out after %s seconds' % timeout)
        self.transport.loseConnection()
    
    def _finish(self):
        # if someone's waiting to know that all handlers are done, call them back
        if self._activeHandlers or (not self._allHandlersFinishedSignal):
            return
        self._allHandlersFinishedSignal.callback(self)
        self._allHandlersFinishedSignal = None
    
    def _finishHandlers(self):
        """Return a Deferred to signal when all requests in process are complete
        """
        if self._activeHandlers:
            self._allHandlersFinishedSignal = defer.Deferred()
            return self._allHandlersFinishedSignal
    
    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler

class StompFactory(Factory):
    protocol = StompProtocol
    
    def __init__(self, onFrame, onDisconnect):
        self.onFrame = onFrame
        self.onDisconnect = onDisconnect
        
    def buildProtocol(self, _):
        protocol = self.protocol(self.onFrame, self.onDisconnect)
        protocol.factory = self
        return protocol
