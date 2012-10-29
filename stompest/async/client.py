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

LOG_CATEGORY = 'stompest.async.client'

class Stomp(object):
    DEFAULT_ACK_MODE = 'auto'
    
    def __init__(self, config, connectTimeout=None, connectedTimeout=None, disconnectTimeout=None, onMessageFailed=None):
        self._config = config
        self.session = StompSession(self._config.version)
        self._protocol = None
        self._protocolCreator = StompProtocolCreator(self._config.uri)
        
        self._connectTimeout = connectTimeout
        self._connectedTimeout = connectedTimeout
        self._disconnectTimeout = disconnectTimeout
        
        self.__onMessageFailed = onMessageFailed
        
        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._handlers = {
            'MESSAGE': self._onMessage,
            'CONNECTED': self._onConnected,
            'ERROR': self._onError,
            'RECEIPT': self._onReceipt,
        }
        self._subscriptions = {}
        
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
            self._protocol = yield self._protocolCreator.connect(self._connectTimeout, self._disconnectTimeout, self._onFrame, self._onDisconnect)
        except Exception as e:
            self.log.error('Endpoint connect failed')
            raise

        try:
            self.sendFrame(frame)
            yield self.connect.wait(self._connectedTimeout)
        except Exception as e:
            self.log.error('STOMP session connect failed [%s]' % e)
            yield self.disconnect(e)
        
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
    def _onFrame(self, protocol, frame):
        try:
            handler = self._handlers[frame.command]
        except KeyError:
            raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
        return handler(protocol, frame)
    
    def _onConnected(self, protocol, frame):
        self.session.connected(frame)
        self.log.debug('Connected to stomp broker with session: %s' % self.session.id)
        self.connect.waiting.callback(None)

    def _onError(self, protocol, frame):
        if self.connect.waiting:
            self.connect.waiting.errback(StompProtocolError('While trying to connect, received %s' % frame.info()))
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
            self.log.warning('[%s] Ignoring message (no handler found): %s' % (messageId, frame.info()))
            return
        
        try:
            protocol.handlerStarted(messageId)
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
    
    def _replay(self):
        for (destination, headers, context) in self.session.replay():
            self.log.debug('Replaying subscription: %s' % headers)
            self.subscribe(destination, headers=headers, **context)
    