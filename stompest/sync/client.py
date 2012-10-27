# -*- coding: iso-8859-1 -*-
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
import contextlib
import logging
import time

from stompest.error import StompConnectionError
from stompest.protocol import StompFailoverProtocol, StompSession

from .transport import StompFrameTransport
from stompest.protocol import commands

LOG_CATEGORY = 'stompest.sync'

class Stomp(object):
    factory = StompFrameTransport
    
    def __init__(self, config):
        self.log = logging.getLogger(LOG_CATEGORY)
        self._config = config
        self._session = StompSession(self._config.version)
        self._failover = StompFailoverProtocol(config.uri)
        self._transport = None
    
    def connect(self, headers=None, versions=None, host=None):
        if self.__transport:
            try: # preserve existing connection
                self._transport.canRead(0)
                self.log.warning('Already connected to %s' % self._transport)
                return
            except StompConnectionError as e:
                self.log.warning('Lost connection to %s [%s]' % (self._transport, e))
        try:
            for (broker, connectDelay) in self._failover:
                self._transport = self.factory(broker['host'], broker['port'], self._session.version)
                if connectDelay:
                    self.log.debug('Delaying connect attempt for %d ms' % int(connectDelay * 1000))
                    time.sleep(connectDelay)
                self.log.debug('Connecting to %s ...' % self._transport)
                try:
                    self._transport.connect()
                except StompConnectionError as e:
                    self.log.warning('Could not connect to %s [%s]' % (self._transport, e))
                else:
                    self.log.debug('Connection established')
                    self._connect(headers, versions, host)
                    break
        except StompConnectionError as e:
            self.log.error('Reconnect failed [%s]' % e)
            raise
        
    def _connect(self, headers=None, versions=None, host=None):
        frame = self._session.connect(self._config.login, self._config.passcode, headers, versions, host)
        self.sendFrame(frame)
        frame = self.receiveFrame()
        self._session.connected(frame)
        self.log.info('STOMP session established with broker %s' % self._transport)
        for (dest, headers, _) in self._session.replay():
            self.log.debug('Replaying subscription %s' % headers)
            self.subscribe(dest, headers)
        
    def disconnect(self):
        self.sendFrame(self._session.disconnect())
        self._session.flush()
        self._transport.disconnect()

    # STOMP frames

    def send(self, destination, body='', headers=None, receipt=None):
        self.sendFrame(commands.send(destination, body, headers, receipt))
        
    def subscribe(self, destination, headers):
        frame, token = self._session.subscribe(destination, headers)
        self.sendFrame(frame)
        return token
    
    def unsubscribe(self, token):
        self.sendFrame(self._session.unsubscribe(token))
        
    def ack(self, headers):
        self.sendFrame(self._session.ack(headers))
    
    def nack(self, headers):
        self.sendFrame(self._session.nack(headers))
    
    def begin(self, transactionId):
        self.sendFrame(self._session.begin(transactionId))
        
    def abort(self, transactionId):
        self.sendFrame(self._session.abort(transactionId))
        
    def commit(self, transactionId):
        self.sendFrame(self._session.commit(transactionId))
    
    @contextlib.contextmanager
    def transaction(self, transaction=None):
        try:
            transaction = self._session.transaction(transaction)
            self.begin(transaction)
            yield transaction
            self.commit(transaction)
        except:
            self.abort(transaction)
    
    # frame transport
    
    def canRead(self, timeout=None):
        return self._transport.canRead(timeout)
        
    def sendFrame(self, frame):
        self.log.debug('Sending %s' % frame.info())
        self._transport.send(frame)
    
    def receiveFrame(self):
        frame = self._transport.receive()
        if frame:
            self.log.debug('Received %s' % frame.info())
        return frame
    
    @property
    def _transport(self):
        if not self.__transport:
            raise StompConnectionError('Not connected')
        return self.__transport
    
    @_transport.setter
    def _transport(self, transport):
        self.__transport = transport
        