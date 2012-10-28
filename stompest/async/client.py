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
from twisted.internet.protocol import Factory, Protocol

from stompest.error import StompConnectionError, StompFrameError, StompProtocolError
from stompest.protocol import commands, StompFailoverProtocol, StompParser, StompSession, StompSpec
from stompest.util import cloneFrame

from .util import endpointFactory, exclusive

LOG_CATEGORY = 'stompest.async.client'

class Stomp(object):
    CLIENT_ACK_MODES = set(['client', 'client-individual'])
    DEFAULT_ACK_MODE = 'client'
    
    @classmethod
    def endpointFactory(cls, broker, timeout=None):
        return endpointFactory(broker, timeout)
    
    def __init__(self, config, connectTimeout=None, connectedTimeout=None, version=None, onMessageFailed=None, **kwargs):
        self.__onMessageFailed = onMessageFailed
                
        self._config = config
        self._connectTimeout = connectTimeout
        self._connectedTimeout = connectedTimeout
        
        self.session = StompSession(version)
        self._failover = StompFailoverProtocol(config.uri)
        self._protocol = None
        
        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._handlers = {
            'MESSAGE': self._onMessage,
            'CONNECTED': self._onConnected,
            'ERROR': self._onError,
            'RECEIPT': self._onReceipt,
        }
        self._subscriptions = {}
        
        # signals
        self._connectedSignal = None
     
    #   
    # STOMP commands
    #
    @exclusive
    @defer.inlineCallbacks
    def connect(self, headers=None, versions=None, host=None):
        frame = self.session.connect(self._config.login, self._config.passcode, headers, versions, host)
        
        try:
            self._protocol
        except:
            pass
        else:
            raise StompConnectionError('Already connected')
        
        try:
            protocol = yield self._connectEndpoint()
        except Exception as e:
            self.log.error('Endpoint connect failed')
            raise

        try:
            protocol.send(frame)
            yield self._waitConnected()
        except Exception as e:
            self.log.error('STOMP session connect failed [%s]' % e)
            protocol.loseConnection()
            raise
        
        self._protocol = protocol
        self._replay()
        defer.returnValue(self)
    
    @exclusive
    @defer.inlineCallbacks
    def disconnect(self, failure=None):
        try:
            yield self._protocol.disconnect(failure)
        finally:
            self._protocol = None
        defer.returnValue(None)
    
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
            
    #
    # callbacks for received STOMP frames
    #
    def _onFrame(self, protocol, frame):
        try:
            handler = self._handlers[frame.command]
        except KeyError:
            raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
        return handler(protocol, frame)
    
    def _onConnected(self, protocol, frame):
        self.session.connected(frame)
        self.log.debug('Connected to stomp broker with session: %s' % self.session.id)
        protocol.onConnected()
        self._connectedSignal.callback(None)

    def _onError(self, protocol, frame):
        if self._connectedSignal:
            self._connectedSignal.errback(StompProtocolError('While trying to connect, received %s' % frame.info()))
            return

        #Workaround for AMQ < 5.2
        if 'Unexpected ACK received for message-id' in frame.headers.get('message', ''):
            self.log.debug('AMQ brokers < 5.2 do not support client-individual mode.')
        else:
            self.disconnect(failure=StompProtocolError('Received %s' % frame.info()))
        
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
            yield subscription['handler'](self, frame)
        except Exception as e:
            self.log.exception('')
            self.log.error('Error in message handler: %s' % e)
            try:
                self._onMessageFailed(e, frame, subscription['errorDestination'])
            except Exception as e:
                self.disconnect(failure=e)
        else:
            if subscription['ack'] in StompSpec.CLIENT_ACK_MODES:
                self.ack(frame)
        finally:
            protocol.handlerFinished(messageId)
    
    def _onReceipt(self, protocol, frame):
        pass
    
    #
    # hook for MESSAGE frame error handling
    #
    def _onMessageFailed(self, failure, frame, errorDestination):
        if self.__onMessageFailed:
            self.__onMessageFailed(self, failure, frame, errorDestination)
        else:
            self.sendToErrorDestination(frame, errorDestination)
    
    def sendToErrorDestination(self, frame, errorDestination):
        if not errorDestination: # forward message to error queue if configured
            return
        errorMessage = cloneFrame(frame, persistent=True)
        self.send(errorDestination, errorMessage.body, errorMessage.headers)
        self.ack(frame)
        
    #
    # protocol
    #
    @property
    def _protocol(self):
        protocol = self.__protocol
        if not protocol:
            raise StompConnectionError('Not connected')
        return protocol
        
    @_protocol.setter
    def _protocol(self, protocol):
        self.__protocol = protocol
        if protocol:
            protocol.disconnected.addBoth(self._handleDisconnected)
    
    @property
    def disconnected(self):
        return self._protocol and self._protocol.disconnected
    
    def sendFrame(self, frame):
        self._protocol.send(frame)
    
    def _handleDisconnected(self, result):
        self._protocol = None
        return result
    
    def _onDisconnect(self, protocol, error):
        frame = self.session.disconnect()
        if not error:
            self.session.flush()
        protocol.send(frame)
        protocol.loseConnection()
    
    #
    # private helpers
    #
    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler
    
    @defer.inlineCallbacks
    def _connectEndpoint(self):
        for (broker, delay) in self._failover:
            yield self._sleep(delay)
            endpoint = self.endpointFactory(broker, self._connectTimeout)
            self.log.debug('Connecting to %(host)s:%(port)s ...' % broker)
            try:
                protocol = yield endpoint.connect(StompFactory(self._onFrame, self._onDisconnect))
            except Exception as e:
                self.log.warning('%s [%s]' % ('Could not connect to %(host)s:%(port)d' % broker, e))
            else:
                defer.returnValue(protocol)
    
    def _replay(self):
        for (destination, headers, context) in self.session.replay():
            self.log.debug('Replaying subscription: %s' % headers)
            self.subscribe(destination, headers=headers, **context)
    
    def _sleep(self, delay):
        if not delay:
            return
        self.log.debug('Delaying connect attempt for %d ms' % int(delay * 1000))
        return task.deferLater(reactor, delay, lambda: None)
    
    @defer.inlineCallbacks
    def _waitConnected(self):
        try:
            self._connectedSignal = defer.Deferred()
            timeout = self._connectedTimeout and task.deferLater(reactor, self._connectedTimeout, self._connectedSignal.cancel)
            yield self._connectedSignal
            timeout and timeout.cancel()
        finally:
            self._connectedSignal = None

class StompProtocol(Protocol):
    #
    # twisted.internet.Protocol interface overrides
    #
    def connectionLost(self, reason):
        self._connectionLost(reason)
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
        
        self._disconnecting = False
        self._disconnectedSignal = None
        self._disconnectReason = None
        
        # keep track of active handlers for graceful disconnect
        self._activeHandlers = set()
        self._allHandlersFinishedSignal = None
        
    #
    # user interface
    #
    def send(self, frame):
        self.log.debug('Sending %s' % frame.info())
        self.transport.write(str(frame))
    
    def onConnected(self):
        self._disconnectedSignal = defer.Deferred()
    
    def disconnect(self, failure=None):
        """After finishing outstanding requests, notify that we may be disconnected
        and return Deferred for caller that will be triggered when disconnect is complete
        """
        if failure:
            self._disconnectReason = failure
        if not self._disconnecting:
            self._disconnecting = True
            # notify that we are ready to disconnect after outstanding messages are ack'ed
            defer.maybeDeferred(self._finishHandlers).addBoth(lambda _: self._onDisconnect(self, self._disconnectReason))
        return self._disconnectedSignal
    
    @property
    def disconnected(self):
        return self._disconnectedSignal
    
    def loseConnection(self):
        self.transport.loseConnection()
        
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
    def _connectionLost(self, reason):
        self.log.debug('Disconnected: %s' % reason.getErrorMessage())
        
        if not self._disconnectedSignal:
            return
        if not self._disconnecting:
            self._disconnectReason = StompConnectionError('Unexpected connection loss')
        if self._disconnectReason:
            #self.log.debug('Calling disconnected deferred errback: %s' % self._disconnectReason)
            self._disconnectedSignal.errback(self._disconnectReason)
            self._disconnectReason = None
        else:
            #self.log.debug('Calling disconnected deferred callback')
            self._disconnectedSignal.callback(None)
        self._disconnectedSignal = None
            
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
