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

from twisted.internet import defer

from stompest.error import StompConnectionError, StompFrameError, StompProtocolError
from stompest.protocol import commands, StompSession, StompSpec
from stompest.util import cloneFrame

from .protocol import StompProtocolCreator
from .util import exclusive
from twisted.internet.defer import CancelledError

LOG_CATEGORY = 'stompest.async.client'

class Stomp(object):
    DEFAULT_ACK_MODE = 'auto'
    
    def __init__(self, config, onMessageFailed=None):
        self._config = config
        self.session = StompSession(self._config.version)
        self._protocol = None
        self._protocolCreator = StompProtocolCreator(self._config.uri)
        
        self.__onMessageFailed = onMessageFailed
        
        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._handlers = {
            'MESSAGE': self._onMessage,
            'CONNECTED': self._onConnected,
            'ERROR': self._onError,
            'RECEIPT': self._onReceipt,
        }
        self._subscriptions = {}

        # keep track of active handlers for graceful disconnect
        self._activeHandlers = set()
                        
    #   
    # STOMP commands
    #
    @exclusive
    @defer.inlineCallbacks
    def connect(self, headers=None, versions=None, host=None, connectTimeout=None, connectedTimeout=None):
        frame = self.session.connect(self._config.login, self._config.passcode, headers, versions, host)
        
        try:
            self._protocol
        except:
            pass
        else:
            raise StompConnectionError('Already connected')
        
        try:
            self._protocol = yield self._protocolCreator.connect(connectTimeout, self._onFrame, self._onConnectionLost)
        except Exception as e:
            self.log.error('Endpoint connect failed')
            raise
        
        self._disconnectedSignal = defer.Deferred()
        self._disconnectReason = None
        
        try:
            self.sendFrame(frame)
            yield self.connect.wait(connectedTimeout)
        except Exception as e:
            self.log.error('STOMP session connect failed [%s]' % e)
            yield self.disconnect(e)
        
        self._replay()
        
        defer.returnValue(self)
    
    @exclusive
    @defer.inlineCallbacks
    def disconnect(self, failure=None, timeout=None):
        if failure:
            self._disconnectReason = failure
            
        try:
            # notify that we are ready to disconnect after outstanding messages are ack'ed
            if self._activeHandlers:
                self.log.info('Waiting for outstanding message handlers to finish ... [timeout=%s]' % timeout)
                try:
                    yield self.disconnect.wait(timeout)
                except CancelledError:
                    self.log.warning('Handlers did not finish in time. Force disconnect ...')
            
            frame = self.session.disconnect()
            
            if not self._disconnectReason:
                self.session.flush()
            
            try:
                self.sendFrame(frame)
                self._protocol.loseConnection()
            except:
                self.log.warning('Connection lost unexpectedly. Could not send %s.' % frame.info())
                
            result = yield self._disconnectedSignal
            
        except Exception as e:
            self.log.error(e)
            raise
        finally:
            self._protocol = None
        
        defer.returnValue(result)
    
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
    
    def subscribe(self, destination, handler, headers=None, receipt=None, ack=True, errorDestination=None):
        protocol = self._protocol
        if not callable(handler):
            raise ValueError('Cannot subscribe (handler is missing): %s' % handler)
        frame, token = self.session.subscribe(destination, headers, context={'handler': handler, 'receipt': receipt, 'errorDestination': errorDestination})
        ack = ack and (frame.headers.setdefault(StompSpec.ACK_HEADER, self.DEFAULT_ACK_MODE) in StompSpec.CLIENT_ACK_MODES)
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
    def _onFrame(self, frame):
        try:
            handler = self._handlers[frame.command]
        except KeyError:
            raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
        return handler(frame)
    
    def _onConnected(self, frame):
        self.session.connected(frame)
        self.log.debug('Connected to stomp broker with session: %s' % self.session.id)
        self.connect.waiting.callback(None)

    def _onError(self, frame):
        if self.connect.waiting:
            self.connect.waiting.errback(StompProtocolError('While trying to connect, received %s' % frame.info()))
            return

        #Workaround for AMQ < 5.2
        if 'Unexpected ACK received for message-id' in frame.headers.get('message', ''):
            self.log.debug('AMQ brokers < 5.2 do not support client-individual mode.')
        else:
            self.disconnect(failure=StompProtocolError('Received %s' % frame.info()))
        
    @defer.inlineCallbacks
    def _onMessage(self, frame):
        headers = frame.headers
        messageId = headers[StompSpec.MESSAGE_ID_HEADER]
        try:
            token = self.session.message(frame)
            subscription = self._subscriptions[token]
        except:
            self.log.warning('[%s] Ignoring message (no handler found): %s' % (messageId, frame.info()))
            return
        
        try:
            self._handlerStarted(messageId)
        except StompConnectionError as disconnecting:
            self.log.info(disconnecting)
            try:
                self.nack(frame)
            except:
                pass
            return
        
        # call message handler (can return deferred to be async)
        try:
            yield subscription['handler'](self, frame)
            if subscription['ack']:
                self.ack(frame)
        except Exception as e:
            self.log.error('[%s] Error in message handler: %s' % (messageId, e))
            try:
                self._onMessageFailed(e, frame, subscription['errorDestination'])
                if subscription['ack']:
                    self.ack(frame)
            except Exception as e:
                self.disconnect(failure=e)
        finally:
            self._handlerFinished(messageId)
    
    def _onReceipt(self, frame):
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
            
    @property
    def disconnected(self):
        return self._disconnectedSignal
    
    def sendFrame(self, frame):
        self._protocol.send(frame)
    
    #
    # private helpers
    #
    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler
    
    def _replay(self):
        for (destination, headers, context) in self.session.replay():
            self.log.debug('Replaying subscription: %s' % headers)
            self.subscribe(destination, headers=headers, **context)
    
    def _handlerStarted(self, messageId):
        # do not process any more messages if we're disconnecting
        if self.disconnect.running:
            raise StompConnectionError('[%s] Ignoring message (disconnecting)' % messageId)
        
        if messageId in self._activeHandlers:
            raise StompProtocolError('Duplicate message id %s received. Message is already in progress.' % messageId)
        self._activeHandlers.add(messageId)
        self.log.debug('Handler started for message: %s' % messageId)
    
    def _handlerFinished(self, messageId):
        self._activeHandlers.remove(messageId)
        self.log.debug('Handler complete for message: %s' % messageId)
        if self.disconnect.waiting and not self._activeHandlers:
            self.log.debug('All handlers complete. Resuming disconnect ...')
            self.disconnect.waiting.callback(self)
            
    def _onConnectionLost(self, reason):
        self._protocol = None
        self.log.info('Disconnected: %s' % reason.getErrorMessage())
        if (not self.disconnect.running) or self.disconnect.waiting:
            self._disconnectReason = StompConnectionError('Unexpected connection loss [%s]' % reason.getErrorMessage())
        if self.disconnect.waiting:
            self.disconnect.cancel()
        if self._disconnectReason:
            #self.log.debug('Calling disconnected deferred errback: %s' % self._disconnectReason)
            self._disconnectedSignal.errback(self._disconnectReason)
            self._disconnectReason = None
        else:
            #self.log.debug('Calling disconnected deferred callback')
            self._disconnectedSignal.callback(None)
        self._disconnectedSignal = None
