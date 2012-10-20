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

from stompest.error import StompError, StompConnectionError
from stompest.protocol.session import StompSession

from stompest.async.client import StompFactory
from stompest.async.utils import exclusive, endpointFactory

LOG_CATEGORY = 'stompest.async.failover'

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
        handler = self._createHandler(handler)
        self._stomp.subscribe(dest=dest, handler=handler, headers=headers, **kwargs)
        headers['destination'] = dest
        self._session.subscribe(headers, context={'handler': handler, 'kwargs': kwargs})
    
    # TODO: unsubscribe
        
    # private methods
    
    @defer.inlineCallbacks
    def _connect(self):
        for (broker, delay) in self._session:
            yield self._sleep(delay)
            endpoint = endpointFactory(broker)
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
    
    def _createHandler(self, handler):
        @functools.wraps(handler)
        def _handler(_, result):
            return handler(self, result)
        return _handler
    
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
