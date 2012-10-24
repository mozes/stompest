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
from stompest.protocol.spec import StompSpec

class StompSession(object):
    CONNECTED = 'connected'
    CONNECTING = 'connecting'
    DISCONNECTED = 'disconnected'
    
    def __init__(self, version=None, check=False):
        self.version = version
        self._check = check
        self._nextSubscription = itertools.count().next
        self._reset()
    
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
    
    def send(self, destination, body, headers):
        self.__check(self.CONNECTED)
        return commands.send(destination, body, headers)
    
    def ack(self, headers):
        self.__check(self.CONNECTED)
        return commands.ack(self._untokenize(headers))
        
    def nack(self, headers):
        self.__check(self.CONNECTED)
        return commands.nack(self._untokenize(headers), self.version)
        
    def subscribe(self, destination, headers=None, context=None):
        self.__check(self.CONNECTED)
        frame = commands.subscribe(destination, headers, self.version)
        token = self._tokenize(commands.unsubscribe(self._untokenize(frame), self.version))
        if token in self._subscriptions:
            raise StompProtocolError('Already subscribed [%s=%s]' % token)
        self._subscriptions[token] = (self._nextSubscription(), destination, copy.copy(headers), context)
        return frame, token
    
    def subscription(self, subscription):
        self.__check(self.CONNECTED)
        subscription = self._untokenize(subscription)
        if StompSpec.SUBSCRIPTION_HEADER in subscription:
            subscription[StompSpec.ID_HEADER] = subscription.pop(StompSpec.SUBSCRIPTION_HEADER)
        try:
            frame = commands.unsubscribe(self._untokenize(subscription), self.version)
            token = self._tokenize(frame)
        except:
            raise StompProtocolError('Invalid subscription [%s]' % subscription)
        if token not in self._subscriptions:
            raise StompProtocolError('No such subscription [%s=%s]' % token)
        return token
    
    def unsubscribe(self, subscription):
        self.__check(self.CONNECTED)
        token = self.subscription(subscription)
        self._subscriptions.pop(token)
        return commands.unsubscribe(self._untokenize(token), self.version), token

    def begin(self):
        self.__check(self.CONNECTED)
        frame = commands.begin(commands.transaction())
        token = self._tokenize(frame)
        self._transactions.add(token)
        return frame, token
        
    def commit(self, transaction):
        self.__check(self.CONNECTED)
        frame = commands.commit(self._untokenize(transaction))
        token = self._tokenize(frame)
        try:
            self._transactions.remove(token)
        except KeyError:
            raise StompProtocolError('Transaction unknown [%s=%s]' % token)
        return frame, token
    
    def abort(self, transaction):
        self.__check(self.CONNECTED)
        frame = commands.abort(self._untokenize(transaction))
        token = self._tokenize(frame)
        try:
            self._transactions.remove(token)
        except KeyError:
            raise StompProtocolError('Transaction unknown [%s=%s]' % token)
        return frame, token
    
    def connect(self, login=None, passcode=None, headers=None, versions=None, host=None):
        self.__check(self.DISCONNECTED)
        frame = commands.connect(login, passcode, headers, versions=list(commands.versions(self.version)))
        self._state = self.CONNECTING
        return frame
    
    def connected(self, headers):
        self.__check(self.CONNECTING)
        self.version, self._server, self._id = commands.connected(headers, version=self.version)
        self._state = self.CONNECTED
        
    def disconnect(self, receipt=None):
        self.__check(self.CONNECTED)
        self._reset()
        return commands.disconnect(receipt, self.version)
    
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
    
    def replay(self):
        subscriptions = self._subscriptions
        self._flush()
        for (_, destination, headers, context) in sorted(subscriptions.itervalues()):
            yield destination, headers, context
    
    # helpers
    
    def _flush(self):
        self._subscriptions = {}
        self._transactions = set()
    
    def _reset(self):
        self._flush()
        self._id = None
        self._server = None
        self._state = self.DISCONNECTED
        
    def _tokenize(self, headers):
        """Accepts a single header (header, value), {header: value}, or a StompFrame with a
        single header. Returns a hashable token (header, value).
        """
        token = headers if isinstance(headers, tuple) else dict(getattr(headers, 'headers', headers)).popitem()
        return tuple(str(e) for e in token)
    
    def _untokenize(self, token):
        """Accepts a token (header, value), or headers, or a StompFrame. Returns the headers
        as a dict.
        """
        return dict([token] if isinstance(token, tuple) else getattr(token, 'headers', token))

    def __check(self, state):
        if self._check and (self.state != state):
            raise StompProtocolError('Cannot handle command in state %s != %s' % (self.state, state))
 