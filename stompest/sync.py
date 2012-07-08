"""
Copyright 2012 Mozes, Inc.

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
import time

from itertools import cycle
from random import choice, random

from stompest.error import StompConnectionError, StompConnectTimeout
from stompest.simple import Stomp as _Stomp
from stompest.util import createFrame
from stompest.util import StompConfiguration as _StompConfiguration

LOG_CATEGORY = 'stompest.sync'

class Stomp(object):
    """A less simple implementation of a failover STOMP session with potentially more than one client"""
    @classmethod
    def packFrame(cls, message):
        return createFrame(message).pack()
    
    def __init__(self, uri):
        self.log = logging.getLogger(LOG_CATEGORY)
        self._config = _StompConfiguration(uri) # syntax of uri: cf. stompest.util
        stomps = [_Stomp(broker['host'], broker['port']) for broker in self._config.brokers]
        self._stomps = ((choice(stomps) if self._config.options['randomize'] else stomp) for stomp in cycle(stomps))
        self._stomp = None
        self._subscriptions = []
    
    def connect(self):
        options = self._config.options
        maxReconnectAttempts = options['maxReconnectAttempts'] if self._stomp else options['startupMaxReconnectAttempts']
        if self._stomp:
            try: # preserve existing connection
                self._stomp.canRead(0)
                return
            except:
                if not maxReconnectAttempts:
                    raise
                self.log.warning('connection lost. trying to reconnect to %s:%d ...' % (self._stomp.host, self._stomp.port))
        
        reconnectDelay = options['initialReconnectDelay'] / 1000.0
        cutoff = None
        reconnectAttempts = 0
        while True:
            self._stomp = self._stomps.next()
            try:
                return self._connect()
            except StompConnectionError as connectError:
                if cutoff is None:
                    cutoff = time.time() + (options['maxReconnectDelay'] / 1000.0)
                try:
                    reconnectDelay = self._wait(maxReconnectAttempts, reconnectAttempts, cutoff, reconnectDelay)
                    reconnectAttempts += 1
                except StompConnectionError as reconnectError:
                    self.log.error('%s [%s]' % (reconnectError, connectError))
                    raise reconnectError
                
    def disconnect(self):
        self._subscriptions = []
        return self._stomp.disconnect()

    def canRead(self, timeout=None):
        return self._stomp.canRead(timeout)
        
    def send(self, dest, msg, headers=None):
        return self._stomp.send(dest, msg, headers)
        
    def subscribe(self, dest, headers=None):
        headers = dict(headers or {})
        self._subscribe(dest, headers)
        self._stomp.subscribe(dest, headers)
        
    def unsubscribe(self, dest, headers=None):
        headers = dict(headers or {})
        self._unsubscribe(dest, headers)
        self._stomp.unsubscribe(dest, headers)
    
    def begin(self, transactionId):
        self._stomp.begin(transactionId)
        
    def commit(self, transactionId):
        self._stomp.commit(transactionId)
        
    def abort(self, transactionId):
        self._stomp.abort(transactionId)
    
    def transaction(self, transactionId):
        self._stomp.transaction(transactionId)
        
    def ack(self, frame):
        self._stomp.ack(frame)
    
    def sendFrame(self, frame):
        self._stomp.sendFrame(frame)
    
    def receiveFrame(self):
        return self._stomp.receiveFrame()
    
    def _connect(self):
        self.log.debug('connecting to %s:%d ...' % (self._stomp.host, self._stomp.port))
        result = self._stomp.connect(self._config.login, self._config.passcode)
        subscriptions, self._subscriptions = self._subscriptions, []
        for (dest, headers) in subscriptions:
            self.log.debug('replaying subscription %s to %s' % (dict(headers), dest))
            self.subscribe(dest, headers)
        self.log.info('connection established to %s:%d' % (self._stomp.host, self._stomp.port))
        return result
    
    def _subscribe(self, dest, headers):
        subscription = self._subscription(dest, headers)
        if subscription not in self._subscriptions:
            self._subscriptions.append(subscription)
    
    def _unsubscribe(self, dest, headers):
        try:
            self._subscriptions.remove(self._subscription(dest, headers))
        except ValueError:
            self.log.warning('trying to unsubscribe from an unknown subscription: %s' % headers)
        
    def _subscription(self, dest, headers):
        return (dest, tuple(sorted(headers.items())))
    
    def _wait(self, maxReconnectAttempts, reconnectAttempts, cutoff, reconnectDelay):
        options = self._config.options
        if reconnectAttempts >= maxReconnectAttempts:
            raise StompConnectTimeout('Reconnect timeout: %d attempts'  % maxReconnectAttempts)
        remainingTime = cutoff - time.time()
        if remainingTime <= 0:
            raise StompConnectTimeout('Reconnect timeout: %d ms'  % options['maxReconnectDelay'])
        sleep = min(reconnectDelay + (random() * options['reconnectDelayJitter'] / 1000.0), remainingTime)
        if sleep:
            self.log.debug('delaying reconnect attempt for %d ms' % int(sleep * 1000))
            time.sleep(sleep)
        return reconnectDelay * (options['backOffMultiplier'] if options['useExponentialBackOff'] else 1)
