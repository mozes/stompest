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
import uuid

class StompSession(object):
    CONNECTING = 'connecting'
    CONNECTED = 'connected'
    DISCONNECTED = 'disconnected'
    
    def __init__(self, version=None, check=False):
        self.version = version
        self._check = check
        self._nextSubscription = itertools.count().next
        self._reset()
        self._flush()
    
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
    
    @property
    def _versions(self):
        try:
            self.__versions
        except:
            self.__versions = None
        return list(sorted(self.__versions or commands.versions(self.version)))
    
    @_versions.setter
    def _versions(self, versions):
        if versions and (set(versions) - set(commands.versions(self.version))):
            raise StompProtocolError('Invalid versions: %s [version=%s]' % (versions, self.version))
        self.__versions = versions
    
    # STOMP commands
    
    def connect(self, login=None, passcode=None, headers=None, versions=None, host=None):
        self.__check('connect', [self.DISCONNECTED])
        self._versions = versions
        frame = commands.connect(login, passcode, headers, self._versions, host)
        self._state = self.CONNECTING
        return frame
    
    def disconnect(self, receipt=None):
        self.__check('disconnect', [self.CONNECTED])
        frame = commands.disconnect(receipt)
        self._receipt(receipt)
        return frame
    
    def close(self, flush=True):
        self._reset()
        if flush:
            self._flush()
        
    def send(self, destination, body='', headers=None, receipt=None):
        self.__check('send', [self.CONNECTED])
        frame = commands.send(destination, body, headers, receipt)
        self._receipt(receipt)
        return frame
        
    def subscribe(self, destination, headers=None, receipt=None, context=None):
        self.__check('subscribe', [self.CONNECTED])
        frame, token = commands.subscribe(destination, headers, receipt, self.version)
        if token in self._subscriptions:
            raise StompProtocolError('Already subscribed [%s=%s]' % token)
        self._receipt(receipt)
        self._subscriptions[token] = (self._nextSubscription(), destination, copy.deepcopy(headers), receipt, context)
        return frame, token
    
    def unsubscribe(self, token, receipt=None):
        self.__check('unsubscribe', [self.CONNECTED])
        frame = commands.unsubscribe(token, receipt, self.version)
        try:
            self._subscriptions.pop(token)
        except KeyError:
            raise StompProtocolError('No such subscription [%s=%s]' % token)
        self._receipt(receipt)
        return frame
    
    def ack(self, frame, receipt=None):
        self.__check('ack', [self.CONNECTED])
        frame = commands.ack(frame, receipt, self.version)
        self._receipt(receipt)
        return frame
    
    def nack(self, frame, receipt=None):
        self.__check('nack', [self.CONNECTED])
        frame = commands.nack(frame, receipt, self.version)
        self._receipt(receipt)
        return frame
    
    def transaction(self, transaction=None):
        return str(transaction or uuid.uuid4())
    
    def begin(self, transaction=None, receipt=None):
        self.__check('begin', [self.CONNECTED])
        frame = commands.begin(transaction, receipt)
        if transaction in self._transactions:
            raise StompProtocolError('Transaction already active: %s' % transaction)
        self._transactions.add(transaction)
        self._receipt(receipt)
        return frame
        
    def commit(self, transaction, receipt=None):
        self.__check('commit', [self.CONNECTED])
        frame = commands.commit(transaction, receipt)
        try:
            self._transactions.remove(transaction)
        except KeyError:
            raise StompProtocolError('Transaction unknown: %s' % transaction)
        self._receipt(receipt)
        return frame
    
    def abort(self, transaction, receipt=None):
        self.__check('abort', [self.CONNECTED])
        frame = commands.abort(transaction, receipt)
        try:
            self._transactions.remove(transaction)
        except KeyError:
            raise StompProtocolError('Transaction unknown: %s' % transaction)
        self._receipt(receipt)
        return frame
    
    def connected(self, headers):
        self.__check('connected', [self.CONNECTING])
        try:
            self.version, self._server, self._id = commands.connected(headers, versions=self._versions)
        finally:
            self._versions = None
        self._state = self.CONNECTED
        
    def message(self, frame):
        self.__check('message', [self.CONNECTED])
        token = commands.message(frame, self.version)
        if token not in self._subscriptions:
            raise StompProtocolError('No such subscription [%s=%s]' % token)
        return token
    
    def receipt(self, frame):
        self.__check('receipt', [self.CONNECTED])
        receipt = commands.receipt(frame, self.version)
        try:
            self._receipts.remove(receipt)
        except KeyError:
            raise StompProtocolError('Unexpected receipt: %s' % receipt)
        return receipt
    
    # session information
    
    @property
    def id(self):
        return self._id
    
    @property
    def server(self):
        return self._server
    
    @property
    def state(self):
        return self._state
    
    #subscription replay
    
    def replay(self):
        subscriptions = self._subscriptions
        self._flush()
        for (_, destination, headers, receipt, context) in sorted(subscriptions.itervalues()):
            yield destination, headers, receipt, context
    
    # helpers
    
    def _flush(self):
        self._receipts = set()
        self._subscriptions = {}
        self._transactions = set()
        
    def _receipt(self, receipt):
        if not receipt:
            return
        if receipt in self._receipts:
            raise StompProtocolError('Duplicate receipt: %s' % receipt)
        self._receipts.add(receipt)
        
    def _reset(self):
        self._id = None
        self._server = None
        self._state = self.DISCONNECTED
        self._versions = None
        
    def __check(self, command, states):
        if self._check and (self.state not in states):
            raise StompProtocolError('Cannot handle command %s in state %s (only in states %s)' % (repr(command), repr(self.state), ', '.join(map(repr, states))))
