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
import functools
import logging
import warnings

from twisted.internet import defer, reactor, task
from twisted.internet.endpoints import clientFromString
from twisted.internet.error import ConnectionLost
from twisted.internet.protocol import Factory, Protocol

from stompest.error import StompConnectTimeout, StompError, StompFrameError, StompProtocolError,\
    StompConnectionError
from stompest.protocol import commands
from stompest.protocol.frame import StompFrame
from stompest.protocol.parser import StompParser
from stompest.protocol.session import StompSession
from stompest.protocol.spec import StompSpec
from stompest.util import cloneStompMessage as _cloneStompMessage
from stompest.protocol.failover import StompFailoverUri

LOG_CATEGORY = 'stompest.async'

def _endpointFactory(broker):
    return clientFromString(reactor, '%(protocol)s:host=%(host)s:port=%(port)d' % broker)

def exclusive(f):
    @functools.wraps(f)
    def _exclusive(*args, **kwargs):
        if not _exclusive.running.called:
            raise RuntimeError('%s still running' % f.__name__)
        _exclusive.running = task.deferLater(reactor, 0, f, *args, **kwargs)
        return _exclusive.running
    
    _exclusive.running = defer.Deferred()
    _exclusive.running.callback(None)
    return _exclusive

class StompClient(Protocol):
    """A Twisted implementation of a STOMP client"""
    MESSAGE_INFO_LENGTH = 20
    CLIENT_ACK_MODES = set(['client', 'client-individual'])

    def __init__(self, alwaysDisconnectOnUnhandledMsg=False):
        self._alwaysDisconnectOnUnhandledMsg = alwaysDisconnectOnUnhandledMsg
        
        # leave the used logger public in case the user wants to override it
        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._handlers = {
            'MESSAGE': self._handleMessage,
            'CONNECTED': self._handleConnected,
            'ERROR': self._handleError,
            'RECEIPT': self._handleReceipt,
        }
        self._destinations = {}
        self._connectedDeferred = None
        self._connectTimeoutDelayedCall = None
        self._connectError = None
        self._disconnectedDeferred = None
        self._finishedHandlersDeferred = None
        self._disconnecting = False
        self._disconnectError = None
        self._activeHandlers = set()
        self._parser = StompParser()

    #
    # user interface
    #
    def connect(self, login, passcode, timeout):
        """Send connect command and return Deferred for caller that will get trigger when connect is complete
        """
        if timeout is not None:
            self._connectTimeoutDelayedCall = reactor.callLater(timeout, self._connectTimeout, timeout)
        self._connect(login, passcode)
        self._connectedDeferred = defer.Deferred()
        return self._connectedDeferred
    
    def disconnect(self, failure=None):
        """After finishing outstanding requests, send disconnect command and return Deferred for caller that will get trigger when disconnect is complete
        """
        if failure:
            self._disconnectError = failure
        if not self._disconnecting:
            self._disconnecting = True
            #Send disconnect command after outstanding messages are ack'ed
            defer.maybeDeferred(self._finishHandlers).addBoth(lambda _: self._disconnect())
            
        return self._disconnectedDeferred
    
    def subscribe(self, dest, handler, headers=None, **kwargs):
        """Subscribe to a destination and register a function handler to receive messages for that destination
        """
        errorDestination = kwargs.get('errorDestination')
        # client-individual mode is only supported in AMQ >= 5.2
        # headers[StompSpec.ACK_HEADER] = headers.get(StompSpec.ACK_HEADER, 'client-individual')
        headers = dict(headers or {})
        headers[StompSpec.ACK_HEADER] = headers.get(StompSpec.ACK_HEADER, 'client')
        self._destinations[dest] = {'handler': handler, StompSpec.ACK_HEADER: headers[StompSpec.ACK_HEADER], 'errorDestination': errorDestination}
        self._subscribe(dest, headers)
        
    def send(self, dest, msg='', headers=None):
        """Do the send command to enqueue a message to a destination
        """
        headers = dict(headers or {})
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Sending message to %s: [%s...]' % (dest, msg[:self.MESSAGE_INFO_LENGTH]))
        self.sendFrame(commands.send(dest, msg, headers))
    
    def sendFrame(self, message):
        self._write(str(self._toFrame(message)))
    
    def getDisconnectedDeferred(self):
        return self._disconnectedDeferred
    
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
            message = self._parser.getMessage()
            if not message:
                break
            try:
                handler = self._handlers[message['cmd']]
            except KeyError:
                raise StompFrameError('Unknown STOMP command: %s' % message)
            handler(message)
    
    #
    # Methods for sending raw STOMP commands
    #
    def _connect(self, login, passcode):
        self.log.debug('Sending CONNECT command')
        self.sendFrame(commands.connect(login, passcode))

    def _disconnect(self):
        self.log.debug('Sending DISCONNECT command')
        self.sendFrame(commands.disconnect())

    def _subscribe(self, dest, headers):
        ack = headers.get(StompSpec.ACK_HEADER)
        self.log.debug('Sending SUBSCRIBE command for destination %s with ack mode %s' % (dest, ack))
        headers[StompSpec.DESTINATION_HEADER] = dest
        self.sendFrame(commands.subscribe(headers))

    def _ack(self, messageId):
        self.log.debug('Sending ACK command for message: %s' % messageId)
        self.sendFrame(commands.ack({StompSpec.MESSAGE_ID_HEADER: messageId}))
    
    def _toFrame(self, message):
        if not isinstance(message, StompFrame):
            message = StompFrame(**message)
        return message
    
    def _write(self, data):
        #self.log.debug('sending data:\n%s' % data)
        self.transport.write(data)

    #
    # Private helper methods
    #
    def _cancelConnectTimeout(self, reason):
        if not self._connectTimeoutDelayedCall:
            return
        self.log.debug('Cancelling connect timeout [%s]' % reason)
        self._connectTimeoutDelayedCall.cancel()
        self._connectTimeoutDelayedCall = None
    
    def _handleConnectionLostDisconnect(self):
        if not self._disconnectedDeferred:
            return
        if not self._disconnecting:
            self._disconnectError = StompConnectionError('Unexpected connection loss')
        if self._disconnectError:
            #self.log.debug('Calling disconnected deferred errback: %s' % self._disconnectError)
            self._disconnectedDeferred.errback(self._disconnectError)
            self._disconnectError = None
        else:
            #self.log.debug('Calling disconnected deferred callback')
            self._disconnectedDeferred.callback(self)
        self._disconnectedDeferred = None
            
    def _handleConnectionLostConnect(self):
        if not self._connectedDeferred:
            return
        if self._connectError:
            error, self._connectError = self._connectError, None
        else:
            self.log.error('Connection lost before connection was established')
            error = StompConnectionError('Unexpected connection loss')
        self.log.debug('Calling connected deferred errback: %s' % error)
        self._connectedDeferred.errback(error)                
        self._connectedDeferred = None
    
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
    
    def _messageHandlerFailed(self, failure, messageId, msg, errDest):
        self.log.error('Error in message handler: %s' % failure)
        if errDest: #Forward message to error queue if configured
            errorMessage = _cloneStompMessage(msg, persistent=True)
            self.send(errDest, errorMessage['body'], errorMessage['headers'])
            self._ack(messageId)
            if not self._alwaysDisconnectOnUnhandledMsg:
                return
        self.disconnect(failure)

    def _connectTimeout(self, timeout):
        self.log.error('Connect command timed out after %s seconds' % timeout)
        self._connectTimeoutDelayedCall = None
        self._connectError = StompConnectTimeout('Connect command timed out after %s seconds' % timeout)
        self.transport.loseConnection()
        
    def _handleConnected(self, msg):
        """Handle STOMP CONNECTED commands
        """
        sessionId = msg['headers'].get('session')
        self.log.debug('Connected to stomp broker with session: %s' % sessionId)
        self._cancelConnectTimeout('successfully connected')
        self._disconnectedDeferred = defer.Deferred()
        self._connectedDeferred.callback(self)
        self._connectedDeferred = None
    
    @defer.inlineCallbacks
    def _handleMessage(self, msg):
        """Handle STOMP MESSAGE commands
        """
        dest = msg['headers'][StompSpec.DESTINATION_HEADER]
        messageId = msg['headers'][StompSpec.MESSAGE_ID_HEADER]
        errDest = self._destinations[dest]['errorDestination']

        #Do not process any more messages if we're disconnecting
        if self._disconnecting:
            self.log.debug('Disconnecting...ignoring stomp message: %s at destination: %s' % (messageId, dest))
            return
        
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Received stomp message %s from destination %s: [%s...].  Headers: %s' % (messageId, dest, msg['body'][:self.MESSAGE_INFO_LENGTH], msg['headers']))
        
        #Call message handler (can return deferred to be async)
        self._handlerStarted(messageId)
        try:
            yield defer.maybeDeferred(self._destinations[dest]['handler'], self, msg)
        except Exception as e:
            self._messageHandlerFailed(e, messageId, msg, errDest)
        else:
            if self._isClientAck(dest):
                self._ack(messageId)
        finally:
            self._postProcessMessage(messageId)
        
    def _isClientAck(self, dest):
        return self._destinations[dest][StompSpec.ACK_HEADER] in self.CLIENT_ACK_MODES

    def _postProcessMessage(self, messageId):
        self._handlerFinished(messageId)
        #If someone's waiting to know that all handlers are done, call them back
        if self._finishedHandlersDeferred and not self._handlersInProgress():
            self._finishedHandlersDeferred.callback(self)
            self._finishedHandlersDeferred = None

    def _handleError(self, msg):
        """Handle STOMP ERROR commands
        """
        self.log.info('Received stomp error: %s' % msg)
        if self._connectedDeferred:
            self.transport.loseConnection()
            self._connectError = StompProtocolError('STOMP error message received while trying to connect: %s' % msg)
        else:
            #Workaround for AMQ < 5.2
            if 'Unexpected ACK received for message-id' in msg['headers'].get('message', ''):
                self.log.debug('AMQ brokers < 5.2 do not support client-individual mode.')
            else:
                self._disconnectError = StompProtocolError('STOMP error message received: %s' % msg)
                self.disconnect()
        
    def _handleReceipt(self, msg):
        """Handle STOMP RECEIPT commands
        """
        self.log.info('Received stomp receipt: %s' % msg)

class StompFactory(Factory):
    protocol = StompClient
    
    def __init__(self, **kwargs):
        self._kwargs = kwargs
        
    def buildProtocol(self, _):
        protocol = self.protocol(**self._kwargs)
        protocol.factory = self
        return protocol

class StompCreator(object):
    def __init__(self, config, connectTimeout=None, **kwargs):
        self.config = config
        self.connectTimeout = connectTimeout
        self.kwargs = kwargs
        self.log = logging.getLogger(LOG_CATEGORY)
    
    @defer.inlineCallbacks  
    def getConnection(self, endpoint=None):
        endpoint = endpoint or self._createEndpoint()
        stomp = yield endpoint.connect(StompFactory(**self.kwargs))
        yield stomp.connect(self.config.login, self.config.passcode, timeout=self.connectTimeout)
        defer.returnValue(stomp)
        
    def _createEndpoint(self):
        brokers = StompFailoverUri(self.config.uri).brokers
        if len(brokers) != 1:
            raise ValueError('failover URI is not supported [%s]' % self.config.failoverUri)
        return _endpointFactory(brokers[0])

class StompFailoverClient(object):
    def __init__(self, config, connectTimeout=None, **kwargs):
        self.log = logging.getLogger(LOG_CATEGORY)
        self._config = config
        self._connectTimeout = connectTimeout
        self._kwargs = kwargs
        self._session = StompSession(self._config.uri)
        self._stomp = None
        
    @exclusive
    @defer.inlineCallbacks
    def connect(self):
        try:
            if not self._stomp:
                yield self._connect()
            defer.returnValue(self)
        except Exception as e:
            self.log.error('Connect failed [%s]' % e)
            raise
    
    @exclusive
    @defer.inlineCallbacks
    def disconnect(self, failure=None):
        if not self._stomp:
            raise StompError('Not connected')
        yield self._stomp.disconnect(failure)
        defer.returnValue(None)
    
    @property
    def disconnected(self):
        return self._stomp and self._stomp.getDisconnectedDeferred()
    
    # STOMP commands
    
    def send(self, dest, msg='', headers=None):
        self._stomp.send(dest=dest, msg=msg, headers=headers)
        
    def sendFrame(self, message):
        self._stomp.sendFrame(message)
    
    def subscribe(self, dest, handler, headers=None, **kwargs):
        headers = dict(headers or {})
        self._stomp.subscribe(dest=dest, handler=handler, headers=headers, **kwargs)
        headers['destination'] = dest
        self._session.subscribe(headers, context={'handler': handler, 'kwargs': kwargs})
    
    # TODO: unsubscribe
        
    # private methods
    
    @defer.inlineCallbacks
    def _connect(self):
        for (broker, delay) in self._session:
            yield self._sleep(delay)
            endpoint = _endpointFactory(broker)
            self.log.debug('Connecting to %(host)s:%(port)s ...' % broker)
            try:
                stomp = yield endpoint.connect(StompFactory(**self._kwargs))
            except Exception as e:
                self.log.warning('%s [%s]' % ('Could not connect to %(host)s:%(port)d' % broker, e))
                continue
            self._stomp = yield stomp.connect(self._config.login, self._config.passcode, timeout=self._connectTimeout)
            self._stomp.getDisconnectedDeferred().addBoth(self._handleDisconnected).addErrback(self._handleDisconnectedError)
            yield self._replay()
            defer.returnValue(None)
    
    def _handleDisconnected(self, result):
        self._stomp = None
        return result
    
    def _handleDisconnectedError(self, failure):
        self.log.debug('Connection lost: %s' % failure)
        failure.trap(StompConnectionError)
        self.log.warning('Attempting to reconnect ...')
        return self.connect()
    
    @defer.inlineCallbacks
    def _replay(self):
        for (headers, context) in self._session.replay():
            self.log.debug('Replaying subscription %s' % headers)
            yield self.subscribe(dest=headers['destination'], handler=context['handler'], headers=headers, **context['kwargs'])
    
    def _sleep(self, delay):
        if not delay:
            return
        self.log.debug('Delaying connect attempt for %d ms' % int(delay * 1000))
        return task.deferLater(reactor, delay, lambda: None)

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
