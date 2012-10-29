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
import logging

from twisted.internet import defer, reactor, task
from twisted.internet.protocol import Factory, Protocol

from stompest.protocol import StompFailoverProtocol, StompParser
from stompest.error import StompConnectionError, StompProtocolError

from .util import endpointFactory, exclusive

LOG_CATEGORY = 'stompest.async.protocol'

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
    
    def __init__(self, disconnectTimeout, onFrame, onDisconnect):
        self._onFrame = onFrame
        self._onDisconnect = onDisconnect
        self._disconnectTimeout = disconnectTimeout
        
        # leave the used logger public in case the user wants to override it
        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._parser = StompParser()
        
        self._disconnectedSignal = defer.Deferred()
        self._disconnectReason = None
        
        # keep track of active handlers for graceful disconnect
        self._activeHandlers = set()
        
    #
    # user interface
    #
    def send(self, frame):
        self.log.debug('Sending %s' % frame.info())
        self.transport.write(str(frame))
    
    @exclusive
    @defer.inlineCallbacks
    def disconnect(self, failure=None):
        """After finishing outstanding requests, notify that we may be disconnected
        and return Deferred for caller that will be triggered when disconnect is complete
        """
        if failure:
            self._disconnectReason = failure
            
        # notify that we are ready to disconnect after outstanding messages are ack'ed
        if self._activeHandlers:
            timeout = self._disconnectTimeout
            self.log.info('Waiting for outstanding message handlers to finish ... [timeout=%s]' % timeout)
            yield self.disconnect.wait(timeout)
        
        self._onDisconnect(self, self._disconnectReason)
        result = yield self._disconnectedSignal
        defer.returnValue(result)
    
    @property
    def disconnected(self):
        return self._disconnectedSignal
    
    def loseConnection(self):
        self.transport.loseConnection()
        
    def handlerStarted(self, messageId):
        # do not process any more messages if we're disconnecting
        if self.disconnect.running:
            raise StompConnectionError('[%s] Ignoring message (disconnecting)' % messageId)
        
        if messageId in self._activeHandlers:
            raise StompProtocolError('Duplicate message id %s received. Message is already in progress.' % messageId)
        self._activeHandlers.add(messageId)
        self.log.debug('Handler started for message: %s' % messageId)
    
    def handlerFinished(self, messageId):
        self._activeHandlers.remove(messageId)
        self.log.debug('Handler complete for message: %s' % messageId)
        if self.disconnect.waiting and not self._activeHandlers:
            self.log.debug('All handlers complete. Resuming disconnect ...')
            self.disconnect.waiting.callback(self)

    #
    # Private helper methods
    #
    def _connectionLost(self, reason):
        self.log.info('Disconnected: %s' % reason.getErrorMessage())
        if not self.disconnect.running:
            self._disconnectReason = StompConnectionError('Unexpected connection loss [%s]' % reason)
        if self._disconnectReason:
            #self.log.debug('Calling disconnected deferred errback: %s' % self._disconnectReason)
            self._disconnectedSignal.errback(self._disconnectReason)
            self._disconnectReason = None
        else:
            #self.log.debug('Calling disconnected deferred callback')
            self._disconnectedSignal.callback(None)
        self._disconnectedSignal = None
            
class StompFactory(Factory):
    protocol = StompProtocol
    
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        
    def buildProtocol(self, _):
        protocol = self.protocol(*self.args, **self.kwargs)
        protocol.factory = self
        return protocol

class StompProtocolCreator(object):
    protocolFactory = StompFactory
    
    @classmethod
    def endpointFactory(cls, broker, timeout=None):
        return endpointFactory(broker, timeout)
    
    def __init__(self, uri):
        self._failover = StompFailoverProtocol(uri)
        self.log = logging.getLogger(LOG_CATEGORY)

    @defer.inlineCallbacks
    def connect(self, timeout, *args, **kwargs):
        for (broker, delay) in self._failover:
            yield self._sleep(delay)
            endpoint = self.endpointFactory(broker, timeout)
            self.log.info('Connecting to %(host)s:%(port)s ...' % broker)
            try:
                protocol = yield endpoint.connect(self.protocolFactory(*args, **kwargs))
            except Exception as e:
                self.log.warning('%s [%s]' % ('Could not connect to %(host)s:%(port)d' % broker, e))
            else:
                defer.returnValue(protocol)
    
    def _sleep(self, delay):
        if not delay:
            return
        self.log.info('Delaying connect attempt for %d ms' % int(delay * 1000))
        return task.deferLater(reactor, delay, lambda: None)
