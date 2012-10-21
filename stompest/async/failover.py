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

from twisted.internet import defer, reactor, task

from stompest.error import StompConnectionError
from stompest.protocol import StompFailoverProtocol, StompSession, StompSpec

from .client import StompFactory
from .util import endpointFactory, exclusive

LOG_CATEGORY = 'stompest.async.failover'

class StompFailoverClient(object):
    def __init__(self, config, connectTimeout=None, version=None, **kwargs):
        self._config = config
        self._protocol = StompFailoverProtocol(config.uri)
        self._session = StompSession(version)
        self._connectTimeout = connectTimeout
        self._kwargs = kwargs

        self.log = logging.getLogger(LOG_CATEGORY)
        
    @exclusive
    @defer.inlineCallbacks
    def connect(self):
        try:
            try:
                self._stomp
            except StompConnectionError:
                yield self._connect()
            defer.returnValue(self)
        except Exception as e:
            self.log.error('Connect failed [%s]' % e)
            raise
    
    @exclusive
    @defer.inlineCallbacks
    def disconnect(self, failure=None):
        self._session.replay() # forget subscriptions upon graceful disconnect
        yield self._stomp.disconnect(failure)
        defer.returnValue(None)
    
    @property
    def disconnected(self):
        return self._stomp and self._stomp.disconnected
    
    # STOMP commands
    
    def send(self, destination, body='', headers=None):
        self.sendFrame(self._session.send(destination, body, headers))
        
    def sendFrame(self, message):
        self._stomp.sendFrame(message)
    
    def subscribe(self, destination, handler, headers=None, **kwargs):
        frame = self._session.subscribe(destination, headers, context={'handler': handler, 'kwargs': kwargs})
        self._stomp.subscribe(frame.headers[StompSpec.DESTINATION_HEADER], self._createHandler(handler), frame.headers, **kwargs)
        return frame
    
    def unsubscribe(self, subscription):
        frame = self._session.unsubscribe(subscription)
        self._stomp.unsubscribe(frame.headers)
    
    # private methods
    
    @defer.inlineCallbacks
    def _connect(self):
        for (broker, delay) in self._protocol:
            yield self._sleep(delay)
            endpoint = endpointFactory(broker)
            self.log.debug('Connecting to %(host)s:%(port)s ...' % broker)
            try:
                stomp = yield endpoint.connect(StompFactory(**self._kwargs))
            except Exception as e:
                self.log.warning('%s [%s]' % ('Could not connect to %(host)s:%(port)d' % broker, e))
                continue
            self._stomp = yield stomp.connect(self._config.login, self._config.passcode, timeout=self._connectTimeout)
            self.disconnected.addBoth(self._handleDisconnected)
            yield self._replay()
            defer.returnValue(None)
    
    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler
    
    def _handleDisconnected(self, result):
        self._stomp = None
        return result
    
    @defer.inlineCallbacks
    def _replay(self):
        for (destination, headers, context) in self._session.replay():
            self.log.debug('Replaying subscription: %s' % headers)
            yield self.subscribe(destination, context['handler'], headers, **context['kwargs'])
    
    def _sleep(self, delay):
        if not delay:
            return
        self.log.debug('Delaying connect attempt for %d ms' % int(delay * 1000))
        return task.deferLater(reactor, delay, lambda: None)
    
    @property
    def _stomp(self):
        try:
            stomp = self.__stomp
        except AttributeError:
            self.__stomp = stomp = None
        if not stomp:
            raise StompConnectionError('Not connected')
        return stomp
        
    @_stomp.setter
    def _stomp(self, stomp):
        self.__stomp = stomp
        

