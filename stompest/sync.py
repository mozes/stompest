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

from stompest.simple import Stomp as _Stomp
from stompest.util import StompConfiguration as _StompConfiguration
from stompest.error import StompConnectionError
from contextlib import contextmanager

LOG_CATEGORY = 'stompest.sync'

class Stomp(object):
    """A less simple implementation of a failover STOMP session with potentially more than one client"""
    def __init__(self, uri):
        self.log = logging.getLogger(LOG_CATEGORY)
        self._config = _StompConfiguration(uri) # syntax of uri: cf. stompest.util
        stomps = [_Stomp(broker['host'], broker['port']) for broker in self._config.brokers]
        self._stomps = ((choice(stomps) if self._config.options['randomize'] else stomp) for stomp in cycle(stomps))
        self._stomp = None
    
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
                self.log.warning('connection lost. reconnecting ... [%s:%d]' % (self._stomp.host, self._stomp.port))
        
        reconnectDelay = options['initialReconnectDelay'] / 1000.0
        reconnectDelayJitter = options['reconnectDelayJitter'] / 1000.0
        backOffMultiplier = options['backOffMultiplier'] if options['useExponentialBackOff'] else 1
        cutoff = time.time() + options['maxReconnectDelay'] / 1000.0
        reconnectAttempts = 0
        while True:
            self._stomp = self._stomps.next()
            try:
                self.log.debug('connecting ... [%s:%d]' % (self._stomp.host, self._stomp.port))
                self._stomp.connect(self._config.login, self._config.passcode)
                self.log.info('connection established [%s:%d]' % (self._stomp.host, self._stomp.port))
                return
            except StompConnectionError as e:
                remainingTime = cutoff - time.time()
                error = None
                if (reconnectAttempts >= maxReconnectAttempts):
                    error = StompConnectionError('Reconnect timeout: %d attempts [%s]'  % (maxReconnectAttempts, e))                    
                if remainingTime <= 0:
                    error = StompConnectionError('Reconnect timeout: %d ms [%s]'  % (options['maxReconnectDelay'], e))
                if error:
                    self.log.error(error)
                    raise error
                reconnectAttempts += 1
                sleep = min(reconnectDelay + random() * reconnectDelayJitter, remainingTime)
                if sleep:
                    self.log.debug('delaying reconnect attempt for %d ms' % int(sleep * 1000))
                    time.sleep(sleep)
                if backOffMultiplier != 1:
                    reconnectDelay *= backOffMultiplier
                    self.log.debug('backed off [multiplier=%d]' % backOffMultiplier)
                
    
    def disconnect(self):
        return self._stomp.disconnect()

    def canRead(self, timeout=None):
        return self._stomp.canRead(timeout)
        
    def send(self, dest, msg, headers=None):
        return self._stomp.send(dest, msg, headers)
        
    def subscribe(self, dest, headers=None):
        return self._stomp.subscribe(dest, headers)
        
    def unsubscribe(self, dest, headers=None):
        return self._stomp.unsubscribe(dest, headers)
    
    def begin(self, transactionId):
        return self._stomp.begin(transactionId)
        
    def commit(self, transactionId):
        return self._stomp.commit(transactionId)
        
    def abort(self, transactionId):
        return self._stomp.abort(transactionId)
    
    def transaction(self, transactionId):
        return self._stomp.transaction(transactionId)
        
    def ack(self, frame):
        return self._stomp.ack(frame)
    
    def sendFrame(self, frame):
        return self._stomp.sendFrame(frame)
    
    def receiveFrame(self):
        return self._stomp.receiveFrame()
    
    def packFrame(self, message):
        return self._stomp.packFrame(message)
