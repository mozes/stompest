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
from stompest.error import StompProtocolError

import commands
import copy
import itertools

class StompSession(object):
    CONNECTED = 'connected'
    CONNECTING = 'connecting'
    DISCONNECTED = 'disconnected'
    
    def __init__(self, version=None, check=False):
        self.version = version
        self._check = check
        self._nextSubscription = itertools.count().next
        self._reset()
        self.flush()
    
    @property
    def version(self):
        return self._version or self.__version
    
    @version.setter
    def version(self, version):
        version = commands.version(version)
        if not hasattr(self, '__version'):
            self.__version = version
            version = None
        self._version = version
    
    # STOMP commands
    
    def connect(self, login=None, passcode=None, headers=None, versions=None, host=None):
        self.__check(self.DISCONNECTED)
        frame = commands.connect(login, passcode, headers, list(commands.versions(versions or self.version)), host)
        self._state = self.CONNECTING
        return frame
    
    def disconnect(self, receipt=None):
        self.__check(self.CONNECTED)
        frame = commands.disconnect(receipt, self.version)
        self._reset()
        return frame
    
    def send(self, destination, body='', headers=None, receipt=None):
        self.__check(self.CONNECTED)
        return commands.send(destination, body, headers, receipt)
    
    def subscribe(self, destination, headers=None, receipt=None, context=None):
        self.__check(self.CONNECTED)
        frame, token = commands.subscribe(destination, headers, receipt, self.version)
        if token in self._subscriptions:
            raise StompProtocolError('Already subscribed [%s=%s]' % token)
        self._subscriptions[token] = (self._nextSubscription(), destination, copy.deepcopy(headers), context)
        return frame, token
    
    def unsubscribe(self, token, receipt=None):
        self.__check(self.CONNECTED)
        frame = commands.unsubscribe(token, receipt, self.version)
        try:
            self._subscriptions.pop(token)
        except KeyError:
            raise StompProtocolError('No such subscription [%s=%s]' % token)
        return frame
    
    def ack(self, frame, receipt=None):
        self.__check(self.CONNECTED)
        return commands.ack(frame, receipt, self.version)
        
    def nack(self, frame, receipt=None):
        self.__check(self.CONNECTED)
        return commands.nack(frame, receipt, self.version)
        
    def transactionId(self, transactionId=None):
        return commands.transactionId(transactionId)
    
    def begin(self, transactionId, receipt=None):
        self.__check(self.CONNECTED)
        frame = commands.begin(transactionId, receipt)
        if transactionId in self._transactions:
            raise StompProtocolError('Transaction already active: %s' % transactionId)
        self._transactions.add(transactionId)
        return frame
        
    def commit(self, transactionId, receipt=None):
        self.__check(self.CONNECTED)
        frame = commands.commit(transactionId, receipt)
        try:
            self._transactions.remove(transactionId)
        except KeyError:
            raise StompProtocolError('Transaction unknown: %s' % transactionId)
        return frame
    
    def abort(self, transactionId, receipt=None):
        self.__check(self.CONNECTED)
        frame = commands.abort(transactionId, receipt)
        try:
            self._transactions.remove(transactionId)
        except KeyError:
            raise StompProtocolError('Transaction unknown: %s' % transactionId)
        return frame
    
    def connected(self, headers):
        self.__check(self.CONNECTING)
        self.version, self._server, self._id = commands.connected(headers, version=self.version)
        self._state = self.CONNECTED
        
    def message(self, frame):
        self.__check(self.CONNECTED)
        token = commands.message(frame, self.version)
        if token not in self._subscriptions:
            raise StompProtocolError('No such subscription [%s=%s]' % token)
        return token
    
    @property
    def id(self):
        return self._id
    
    @property
    def server(self):
        return self._server
    
    @property
    def state(self):
        return self._state
    
    # state management
    
    def flush(self):
        self._subscriptions = {}
        self._transactions = set()
    
    def replay(self):
        subscriptions = self._subscriptions
        self.flush()
        for (_, destination, headers, context) in sorted(subscriptions.itervalues()):
            yield destination, headers, context
    
    # helpers
    
    def _reset(self):
        self._id = None
        self._server = None
        self._state = self.DISCONNECTED
        
    def __check(self, state):
        if self._check and (self.state != state):
            raise StompProtocolError('Cannot handle command in state %s != %s' % (self.state, state))
 