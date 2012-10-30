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

from .util import endpointFactory

LOG_CATEGORY = 'stompest.async.protocol'

class StompProtocol(Protocol):
    #
    # twisted.internet.Protocol interface overrides
    #
    def connectionLost(self, reason):
        try:
            self._onConnectionLost(reason)
        finally:
            Protocol.connectionLost(self, reason)
    
    def dataReceived(self, data):
        self._parser.add(data)
                
        while True:
            frame = self._parser.get()
            if not frame:
                break
            self.log.debug('Received %s' % frame.info())
            
            self._onFrame(frame)
    
    def __init__(self, onFrame, onConnectionLost):
        self._onFrame = onFrame
        self._onConnectionLost = onConnectionLost
        
        # leave the used logger public in case the user wants to override it
        self.log = logging.getLogger(LOG_CATEGORY)
        
        self._parser = StompParser()
        
    #
    # user interface
    #
    def send(self, frame):
        self.log.debug('Sending %s' % frame.info())
        self.transport.write(str(frame))
    
    def loseConnection(self):
        self.transport.loseConnection()
        
    #
    # Private helper methods
    #
            
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
