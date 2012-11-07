"""
"""
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

from twisted.internet import defer, task

from stompest.error import StompCancelledError, StompConnectionError, StompFrameError, StompProtocolError
from stompest.protocol import StompSession, StompSpec
from stompest.util import checkattr, cloneFrame

from .protocol import StompProtocolCreator
from .util import InFlightOperations, exclusive, wait

LOG_CATEGORY = __name__

connected = checkattr('_protocol')

class Stomp(object):
    DEFAULT_ACK_MODE = 'auto'
    MESSAGE_FAILED_HEADER = 'message-failed'
    
    def __init__(self, config, receiptTimeout=None):
        """
        :config: a :StompConfig: object
        """
        self._config = config
        self._receiptTimeout = receiptTimeout
        
        self.session = StompSession(self._config.version, self._config.check)
        self._protocol = None
        self._protocolCreator = StompProtocolCreator(self._config.uri)
        
        self.log = logging.getLogger(LOG_CATEGORY)
        
        # wait for CONNECTED frame
        self._connecting = InFlightOperations('STOMP session negotiation')
        
        # keep track of active handlers for graceful disconnect
        self._messages = InFlightOperations('Handler for message')
        self._receipts = InFlightOperations('Waiting for receipt')
                        
        self._handlers = {
            'MESSAGE': self._onMessage,
            'CONNECTED': self._onConnected,
            'ERROR': self._onError,
            'RECEIPT': self._onReceipt,
        }
        self._subscriptions = {}
        
    @property
    def disconnected(self):
        return self._disconnected
    
    def sendFrame(self, frame):
        self._protocol.send(frame)
    
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
        
        self._disconnected = defer.Deferred()
        self._disconnectReason = None
        
        try:
            with self._connecting(None, self.log) as connected:
                self.sendFrame(frame)
                yield wait(connected, connectedTimeout, StompCancelledError('STOMP broker did not answer on time [timeout=%s]' % connectedTimeout))
        except Exception as e:
            self.log.error('Could not establish STOMP session. Disconnecting ...')
            yield self.disconnect(failure=e)
        
        self._replay()
        
        defer.returnValue(self)
    
    @exclusive
    @connected
    @defer.inlineCallbacks
    def disconnect(self, receipt=None, failure=None, timeout=None):
        if failure:
            self._disconnectReason = failure
        self.log.info('Disconnecting ...%s' % ('' if (not failure) else  ('[reason=%s]' % failure)))
        protocol = self._protocol
        disconnected = self.disconnected
        try:
            # notify that we are ready to disconnect after outstanding messages are ack'ed
            if self._messages:
                self.log.info('Waiting for outstanding message handlers to finish ... [timeout=%s]' % timeout)
                try:
                    yield task.cooperate(iter([wait(handler, timeout, StompCancelledError('Going down to disconnect now')) for handler in self._messages.values()])).whenDone()
                except StompCancelledError as e:
                    self._disconnectReason = StompCancelledError('Handlers did not finish in time.')
                else:
                    self.log.info('All handlers complete. Resuming disconnect ...')
            
            if self.session.state == self.session.CONNECTED:
                frame = self.session.disconnect(receipt)
                try:
                    self.sendFrame(frame)
                except Exception as e:
                    self._disconnectReason = StompConnectionError('Could not send %s. [%s]' % (frame.info(), e))
                
                try:
                    yield self._waitForReceipt(receipt)
                except StompCancelledError:
                    self._disconnectReason = StompCancelledError('Receipt for disconnect command did not arrive on time.')
                    
            protocol.loseConnection()
            result = yield disconnected

        except Exception as e:
            self.log.error(e)
            raise
        
        defer.returnValue(result)
    
    @connected
    @defer.inlineCallbacks
    def send(self, destination, body='', headers=None, receipt=None):
        self.sendFrame(self.session.send(destination, body, headers, receipt))
        yield self._waitForReceipt(receipt)
        
    @connected
    @defer.inlineCallbacks
    def ack(self, frame, receipt=None):
        self.sendFrame(self.session.ack(frame, receipt))
        yield self._waitForReceipt(receipt)
    
    @connected
    @defer.inlineCallbacks
    def nack(self, frame, receipt=None):
        self.sendFrame(self.session.nack(frame, receipt))
        yield self._waitForReceipt(receipt)
    
    @connected
    @defer.inlineCallbacks
    def begin(self, transaction=None, receipt=None):
        frame, token = self.session.begin(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
        defer.returnValue(token)
    
    @connected
    @defer.inlineCallbacks
    def abort(self, transaction=None, receipt=None):
        frame, token = self.session.abort(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
        defer.returnValue(token)
    
    @connected
    @defer.inlineCallbacks
    def commit(self, transaction=None, receipt=None):
        frame, token = self.session.commit(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
        defer.returnValue(token)
    
    @connected
    @defer.inlineCallbacks
    def subscribe(self, destination, handler, headers=None, receipt=None, ack=True, errorDestination=None, onMessageFailed=None):
        if not callable(handler):
            raise ValueError('Cannot subscribe (handler is missing): %s' % handler)
        frame, token = self.session.subscribe(destination, headers, receipt, {'handler': handler, 'errorDestination': errorDestination, 'onMessageFailed': onMessageFailed})
        ack = ack and (frame.headers.setdefault(StompSpec.ACK_HEADER, self.DEFAULT_ACK_MODE) in StompSpec.CLIENT_ACK_MODES)
        self._subscriptions[token] = {'destination': destination, 'handler': self._createHandler(handler), 'ack': ack, 'errorDestination': errorDestination, 'onMessageFailed': onMessageFailed}
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
        defer.returnValue(token)
    
    @connected
    @defer.inlineCallbacks
    def unsubscribe(self, token, receipt=None):
        frame = self.session.unsubscribe(token, receipt)
        try:
            self._subscriptions.pop(token)
        except:
            self.log.warning('Cannot unsubscribe (subscription id unknown): %s=%s' % token)
            raise
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
            
    #
    # callbacks for received STOMP frames
    #
    def _onFrame(self, frame):
        try:
            handler = self._handlers[frame.command]
        except KeyError:
            raise StompFrameError('Unknown STOMP command: %s' % repr(frame))
        handler(frame)
    
    def _onConnected(self, frame):
        self.session.connected(frame)
        self.log.info('Connected to stomp broker [session=%s]' % self.session.id)
        self._connecting[None].callback(None)
    
    def _onError(self, frame):
        if self._connecting:
            self._connecting[None].errback(StompProtocolError('While trying to connect, received %s' % frame.info()))
            return

        #Workaround for AMQ < 5.2
        if 'Unexpected ACK received for message-id' in frame.headers.get('message', ''):
            self.log.debug('AMQ brokers < 5.2 do not support client-individual mode')
        else:
            self.disconnect(failure=StompProtocolError('Received %s' % frame.info()))
        
    @defer.inlineCallbacks
    def _onMessage(self, frame):
        headers = frame.headers
        messageId = headers[StompSpec.MESSAGE_ID_HEADER]
        
        if self.disconnect.running:
            self.log.info('[%s] Ignoring message (disconnecting)' % messageId)
            try:
                self.nack(frame)
            except:
                pass
            return
        
        try:
            token = self.session.message(frame)
            subscription = self._subscriptions[token]
        except:
            self.log.warning('[%s] Ignoring message (no handler found): %s' % (messageId, frame.info()))
            return
        
        try:
            with self._messages(messageId, self.log) as finished:
                finished.addErrback(lambda _: None)
                yield subscription['handler'](self, frame)
                if subscription['ack']:
                    self.ack(frame)
        except Exception as e:
            try:
                self._onMessageFailed(e, frame, subscription)
                if subscription['ack']:
                    self.ack(frame)
            except Exception as e:
                if not self.disconnect.running:
                    self.disconnect(failure=e)

    def _onReceipt(self, frame):
        receipt = self.session.receipt(frame)
        self._receipts[receipt].callback(None)
    
    #
    # hook for MESSAGE frame error handling
    #
    def _onMessageFailed(self, failure, frame, subscription):
        onMessageFailed = subscription['onMessageFailed'] or Stomp.sendToErrorDestination
        onMessageFailed(self, failure, frame, subscription['errorDestination'])
    
    # this one is public on purpose as a building block for custom hooks
    def sendToErrorDestination(self, failure, frame, errorDestination):
        if not errorDestination: # forward message to error queue if configured
            return
        errorFrame = cloneFrame(frame, persistent=True)
        errorFrame.headers.setdefault(self.MESSAGE_FAILED_HEADER, str(failure))
        self.send(errorDestination, errorFrame.body, errorFrame.headers)
        self.ack(frame)
        
    #
    # properties
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
    def _disconnectReason(self):
        return self.__disconnectReason
    
    @_disconnectReason.setter
    def _disconnectReason(self, reason):
        if reason:
            self.log.error(str(reason))
            reason = self._disconnectReason or reason # existing reason wins
        self.__disconnectReason = reason
        
    #
    # private helpers
    #
    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler
    
    def _onConnectionLost(self, reason):
        self._protocol = None
        self.log.info('Disconnected: %s' % reason.getErrorMessage())
        if not self.disconnect.running:
            self._disconnectReason = StompConnectionError('Unexpected connection loss [%s]' % reason.getErrorMessage())
        self.session.close(flush=not self._disconnectReason)
        for operations in (self._connecting, self._messages, self._receipts):
            for waiting in operations.values():
                if not waiting.called:
                    waiting.errback(StompCancelledError('In-flight operation cancelled (connection lost)'))
        if self._disconnectReason:
            #self.log.debug('Calling disconnected deferred errback: %s' % self._disconnectReason)
            self._disconnected.errback(self._disconnectReason)
            self._disconnectReason = None
        else:
            #self.log.debug('Calling disconnected deferred callback')
            self._disconnected.callback(None)
        self._disconnected = None
        
    def _replay(self):
        for (destination, headers, receipt, context) in self.session.replay():
            self.log.info('Replaying subscription: %s' % headers)
            self.subscribe(destination, headers=headers, receipt=receipt, **context)
    
    @defer.inlineCallbacks
    def _waitForReceipt(self, receipt):
        if receipt is None:
            defer.returnValue(None)
        with self._receipts(receipt, self.log) as receiptArrived:
            timeout = self._receiptTimeout
            yield wait(receiptArrived, timeout, StompCancelledError('Receipt did not arrive on time: %s [timeout=%s]' % (receipt, timeout)))
        