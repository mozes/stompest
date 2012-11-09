"""The asynchronous client is based on `Twisted <http://twistedmatrix.com/>`_, a very mature and powerful asynchronous programming framework. It supports destination specific message and error handlers (with default "poison pill" error handling), concurrent message processing, graceful shutdown, and connect and disconnect timeouts.

.. seealso:: `STOMP protocol specification <http://stomp.github.com/>`_, `Twisted API documentation <http://twistedmatrix.com/documents/current/api/>`_, `Apache ActiveMQ - Stomp <http://activemq.apache.org/stomp.html>`_

Examples
--------

Producer
^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/producer.py

Transformer
^^^^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/transformer.py

Consumer
^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/consumer.py

API
---
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

# TODO: is it ensured that the DISCONNECT frame is the last frame we send?

class Stomp(object):
    """An asynchronous STOMP client for the Twisted framework.

    :param config: A :class:`StompConfig` object.
    :param receiptTimeout: When a STOMP frame was sent to the broker and a *RECEIPT* frame was requested, this is the time (in seconds) to wait for the *RECEIPT* frame to arrive. If :obj:`None`, we will wait indefinitely.
    
    .. seealso :: :mod:`protocol.failover.StompConfig`, the modules :mod:`protocol.session` and :mod:`protocol.commands` for all API options which are documented here.
    """
    DEFAULT_ACK_MODE = 'auto'
    MESSAGE_FAILED_HEADER = 'message-failed'
    
    def __init__(self, config, receiptTimeout=None):
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
        """This :class:`twisted.internet.defer.Deferred` calls back when the connection to the broker was lost. It will err back when the connection loss was unexpected or caused by another error.
        """
        return self._disconnected
    
    def sendFrame(self, frame):
        """Send a raw STOMP frame.
        
        .. note :: If we are not connected, this method, and all other API commands for sending STOMP frames except :meth:`connect`, will raise a :class:`StompConnectionError`. Use this command only if you have to bypass the :class:`StompSession` logic and you know what you're doing!
        """
        self._protocol.send(frame)
    
    #   
    # STOMP commands
    #
    @exclusive
    @defer.inlineCallbacks
    def connect(self, headers=None, versions=None, host=None, connectTimeout=None, connectedTimeout=None):
        """connect(headers=None, versions=None, host=None, connectTimeout=None, connectedTimeout=None)
        
        Establish a connection to a STOMP broker. If a network connect fails, attempt a failover according to the settings in the client's :class:`StompConfig` object. If there are active subscriptions in the session, replay them when the STOMP session is established. This method returns a :class:`twisted.internet.defer.Deferred` object which calls back with :obj:`self` when the STOMP connection has been established and all subscriptions (if any) were replayed. In case of an error, it will err back with the reason of the failure.
        
        :param versions: The STOMP versions we wish to support. The default behavior (:obj:`None`) is the same as for :func:`commands.connect`, but the highest supported version will be the one you specified in the :class:`StompConfig` object. The version which is valid for the conenction about to be initiated is stored in the client's :class:`StompSession` object (attribute :attr:`session`).
        :param connectTimeout: This is the time (in seconds) to wait for the wire-level connection to be established. If :obj:`None`, we will wait indefinitely.
        :param connectedTimeout: This is the time (in seconds) to wait for the STOMP connection to be established (that is, the broker's *CONNECTED* frame to arrive). If :obj:`None`, we will wait indefinitely.
        
        .. note :: Only one connect attempt may be pending at a time. Any other attempt will result in a :class:`StompAlreadyRunningError`.

        .. seealso :: :mod:`protocol.failover`, :mod:`protocol.session` for the details of subscription replay and failover transport.
        """
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
        """disconnect(receipt=None, failure=None, timeout=None)
        
        Send a *DISCONNECT* frame and terminate the STOMP connection. This method returns a :class:`twisted.internet.defer.Deferred` object which calls back with :obj:`None` when the STOMP connection has been closed. In case of a failure, it will err back with the failure reason. 
        
        :param failure: A disconnect reason (a :class:`Exception`) to err back. Example: ``versions=['1.0', '1.1']``
        :param timeout: This is the time (in seconds) to wait for a graceful disconnect, thas is, for pending message handlers to complete. If receipt is :obj:`None`, we will wait indefinitely.
        
        .. note :: The session's active subscriptions will be cleared if no failure has been passed to this method. This allows you to replay the subscriptions upon reconnect. If you do not wish to do so, you have to clear the subscriptions yourself by calling ``self.session.close()``. Only one disconnect attempt may be pending at a time. Any other attempt will result in a :class:`StompAlreadyRunningError`. The result of any (user-requested or not) disconnect event is available via the :attr:`disconnected` property.
        """
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
        """send(destination, body='', headers=None, receipt=None)
        
        Send a *SEND* frame. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire when a possibly requested *RECEIPT* frame has arrived.
        """
        self.sendFrame(self.session.send(destination, body, headers, receipt))
        yield self._waitForReceipt(receipt)
        
    @connected
    @defer.inlineCallbacks
    def ack(self, frame, receipt=None):
        """ack(frame, receipt=None)
        
        Send an *ACK* frame for a received *MESSAGE* frame. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire when a possibly requested *RECEIPT* frame has arrived. 
        """
        self.sendFrame(self.session.ack(frame, receipt))
        yield self._waitForReceipt(receipt)
    
    @connected
    @defer.inlineCallbacks
    def nack(self, frame, receipt=None):
        """nack(frame, receipt=None)
        
        Send a *NACK* frame for a received *MESSAGE* frame. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire when a possibly requested *RECEIPT* frame has arrived.
        """
        self.sendFrame(self.session.nack(frame, receipt))
        yield self._waitForReceipt(receipt)
    
    @connected
    @defer.inlineCallbacks
    def begin(self, transaction=None, receipt=None):
        """begin(transaction=None, receipt=None)
        
        Send a *BEGIN* frame to begin a STOMP transaction. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire when a possibly requested *RECEIPT* frame has arrived.
        
        :param receipt: See :meth:`disconnect`.
        
        .. note :: If you try and begin a pending transaction twice, this will result in a :class:`StompProtocolError`.
        """
        frame, token = self.session.begin(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
        defer.returnValue(token)
    
    @connected
    @defer.inlineCallbacks
    def abort(self, transaction=None, receipt=None):
        """abort(transaction=None, receipt=None)
        
        Send an *ABORT* frame to abort a STOMP transaction. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire when a possibly requested *RECEIPT* frame has arrived.
        
        :param receipt: See :meth:`disconnect`.
        
        .. note :: If you try and abort a transaction which is not pending, this will result in a :class:`StompProtocolError`.
        """
        frame, token = self.session.abort(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
        defer.returnValue(token)
    
    @connected
    @defer.inlineCallbacks
    def commit(self, transaction=None, receipt=None):
        """commit(transaction=None, receipt=None)
        
        Send a *COMMIT* frame to commit a STOMP transaction. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire when a possibly requested *RECEIPT* frame has arrived.
        
        :param receipt: See :meth:`disconnect`.
        
        .. note :: If you try and commit a transaction which is not pending, this will result in a :class:`StompProtocolError`.
        """
        frame, token = self.session.commit(transaction, receipt)
        self.sendFrame(frame)
        yield self._waitForReceipt(receipt)
        defer.returnValue(token)
    
    @connected
    @defer.inlineCallbacks
    def subscribe(self, destination, handler, headers=None, receipt=None, ack=True, errorDestination=None, onMessageFailed=None):
        """subscribe(destination, handler, headers=None, receipt=None, ack=True, errorDestination=None, onMessageFailed=None)
        
        Send a *SUBSCRIBE* frame to subscribe to a STOMP destination. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire with a token when a possibly requested *RECEIPT* frame has arrived. This token is used internally to match incoming *MESSAGE* frames and must be kept if you wish to :meth:`unsubscribe` later.
        
        :param handler: A callable :obj:`f(client, frame)` which accepts this client and the received :class:`StompFrame`.
        :param ack: Check this option if you wish the client to automatically ack MESSAGE frames when the were handled (successfully or not).
        :param errorDestination: If a frame was not handled successfully, forward a copy of the offending frame to this destination. Example: ``errorDestination='/queue/back-to-square-one'``
        :param onMessageFailed: You can specify a custom error handler which must be a callable with signature :obj:`f(self, failure, frame, errorDestination)`. Note that a non-trivial choice of this error handler overrides the default behavior (forward frame to error destination and ack it).
        
        .. note :: As opposed to the behavior of stompest 1.x, the client will not disconnect when a message could not be handled. Rather, a disconnect will only be triggered in a "panic" situation when also the error handler failed. The automatic disconnect was partly a substitute for the missing NACK command in STOMP 1.0. If you wish to automatically disconnect, you have to implement the **onMessageFailed** hook.
        """
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
        """unsubscribe(token, receipt=None)
        
        Send an *UNSUBSCRIBE* frame to terminate an existing subscription. This method returns a :class:`twisted.internet.defer.Deferred` object which will fire when a possibly requested *RECEIPT* frame has arrived.
        
        :param token: The result of the :meth:`subscribe` command which initiated the subscription in question.
        :param receipt: See :meth:`disconnect`.
        """
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
    
    def sendToErrorDestination(self, failure, frame, errorDestination):
        """sendToErrorDestination(failure, frame, errorDestination)
        
        This is the default error handler for failed *MESSAGE* handlers: forward the offending frame to the error destination (if given) and ack the frame. As opposed to earlier versions, It may be used as a building block for custom error handlers.
        
        :param failure: see the onMessageFailed argument of :meth:`subscribe`.
        :param frame: see the onMessageFailed argument of :meth:`subscribe`.
        :param errorDestination: see the onMessageFailed argument of :meth:`subscribe`.
        """
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
        