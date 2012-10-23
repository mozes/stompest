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
    SUPPORTED_VERSIONS = ['1.0', '1.1']
    DEFAULT_VERSION = '1.0'
    
    CONNECTED = 'connected'
    DISCONNECTED = 'disconnected'
    
    def __init__(self, version=None):
        self.version = version
        self._nextSubscription = itertools.count().next
        self._reset()
    
    @property
    def version(self):
        return self._version or self.__version
    
    @version.setter
    def version(self, version):
        version = version or self.DEFAULT_VERSION
        if version not in self.SUPPORTED_VERSIONS:
            raise StompProtocolError('Version is not supported [%s]' % version)
        if not hasattr(self, '__version'):
            self.__version = version
            version = None
        self._version = version
    
    # STOMP commands
    
    def send(self, destination, body, headers):
        return commands.send(destination, body, headers)
    
    def ack(self, headers):
        return commands.ack(self._untokenize(headers))
        
    def nack(self, headers):
        return commands.nack(self._untokenize(headers), self.version)
        
    def subscribe(self, destination, headers=None, context=None):
        frame = commands.subscribe(destination, headers, self.version)
        token = self._tokenize(commands.unsubscribe(self._untokenize(frame), self.version))
        if token in self._subscriptions:
            raise StompProtocolError('Already subscribed [%s=%s]' % token)
        self._subscriptions[token] = (self._nextSubscription(), destination, copy.copy(headers), context)
        return frame, token
    
    def subscription(self, subscription):
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
        token = self.subscription(subscription)
        self._subscriptions.pop(token)
        return commands.unsubscribe(self._untokenize(token), self.version), token

    def begin(self):
        frame = commands.begin(commands.transaction())
        token = self._tokenize(frame)
        self._transactions.add(token)
        return frame, token
        
    def commit(self, transaction):
        frame = commands.commit(self._untokenize(transaction))
        token = self._tokenize(frame)
        try:
            self._transactions.remove(token)
        except KeyError:
            raise StompProtocolError('Transaction unknown [%s=%s]' % token)
        return frame, token
    
    def abort(self, transaction):
        frame = commands.abort(self._untokenize(transaction))
        token = self._tokenize(frame)
        try:
            self._transactions.remove(token)
        except KeyError:
            raise StompProtocolError('Transaction unknown [%s=%s]' % token)
        return frame, token
    
    def connect(self, username, password, headers=None):
        headers = dict(headers or {})
        if self.version != '1.0':
            headers[StompSpec.ACCEPT_VERSION_HEADER] = ','.join(self._versions)
        return commands.connect(username, password, headers)
    
    def connected(self, headers):
        self.version = headers.get(StompSpec.VERSION_HEADER, '1.0')
        if self.version != '1.0':
            self._server = headers.get(StompSpec.SERVER_HEADER)
        try:
            self._id = headers[StompSpec.SESSION_HEADER]
        except KeyError:
            if self.version == '1.0':
                raise StompProtocolError('Invalid CONNECTED frame (%s header is missing) [headers=%s]' % (StompSpec.SESSION_HEADER, headers))
        self._state = self.CONNECTED
        
    def disconnect(self):
        self._reset()
        return commands.disconnect()
    
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
    
    @property
    def _versions(self):
        for version in self.SUPPORTED_VERSIONS:
            yield version
            if version == self.version:
                break
        
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
