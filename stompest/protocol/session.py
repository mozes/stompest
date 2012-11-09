"""
"""
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
    """This object implements an abstract representation of a STOMP protocol session. It builds upon the low-level commands API in :mod:`protocol.command`. You can use the stateful API provided by :class:`StompSession` independently of the stompest clients to roll your own STOMP client.
    
    .. note :: Being stateful implies that the session keeps track of subscriptions, receipts, and transactions, so keep track of them yourself, too! -- Unless you like to be surprised by a spurious :class:`StompProtocolError` ...
    
    .. seealso :: The commands API in :mod:`protocol.command` for all API command parameters which are not documented here.
    """
    CONNECTING = 'connecting'
    CONNECTED = 'connected'
    DISCONNECTING = 'disconnecting'
    DISCONNECTED = 'disconnected'
    
    def __init__(self, version=None, check=False):
        self.version = version
        self._check = check
        self._nextSubscription = itertools.count().next
        self._reset()
        self._flush()
    
    @property
    def version(self):
        """The STOMP protocol version of the current client-broker connection (if any), or the version you created this session with (otherwise)."""
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
        """Create a *CONNECT* frame and set the session state to CONNECTING."""
        self.__check('connect', [self.DISCONNECTED])
        self._versions = versions
        frame = commands.connect(login, passcode, headers, self._versions, host)
        self._state = self.CONNECTING
        return frame
    
    def disconnect(self, receipt=None):
        """Create a *DISCONNECT* frame and set the session state to DISCONNECTING."""
        self.__check('disconnect', [self.CONNECTED])
        frame = commands.disconnect(receipt)
        self._receipt(receipt)
        self._state = self.DISCONNECTING
        return frame
    
    def close(self, flush=True):
        """Clean up the session: Set the state to DISCONNECTING, remove all information related to an eventual broker connection, clear all pending transactions and receipts.
        
        :param flush: Clear all active subscriptions. This flag controls whether the next :meth:`connect` will replay the currently active subscriptions or will wipe the slate clean.
        """
        self._reset()
        if flush:
            self._flush()
        
    def send(self, destination, body='', headers=None, receipt=None):
        """Create a *SEND* frame."""
        self.__check('send', [self.CONNECTED])
        frame = commands.send(destination, body, headers, receipt)
        self._receipt(receipt)
        return frame
        
    def subscribe(self, destination, headers=None, receipt=None, context=None):
        """Create a *SUBSCRIBE* frame and keep track of the subscription assiocated to it. This method returns a token which you have to keep if you wish to match incoming *MESSAGE* frames to this subscription with :meth:`message` or to :meth:`unsubscribe` later.
        
        :param context: An arbitrary context object which you can use to store any information related to the subscription at hand.
        """
        self.__check('subscribe', [self.CONNECTED])
        frame, token = commands.subscribe(destination, headers, receipt, self.version)
        if token in self._subscriptions:
            raise StompProtocolError('Already subscribed [%s=%s]' % token)
        self._receipt(receipt)
        self._subscriptions[token] = (self._nextSubscription(), destination, copy.deepcopy(headers), receipt, context)
        return frame, token
    
    def unsubscribe(self, token, receipt=None):
        """Create an *UNSUBSCRIBE* frame and lose track of the subscription assiocated to it."""
        self.__check('unsubscribe', [self.CONNECTED])
        frame = commands.unsubscribe(token, receipt, self.version)
        try:
            self._subscriptions.pop(token)
        except KeyError:
            raise StompProtocolError('No such subscription [%s=%s]' % token)
        self._receipt(receipt)
        return frame
    
    def ack(self, frame, receipt=None):
        """Create an *ACK* frame for a received *MESSAGE* frame."""
        self.__check('ack', [self.CONNECTED])
        frame = commands.ack(frame, receipt, self.version)
        self._receipt(receipt)
        return frame
    
    def nack(self, frame, receipt=None):
        """Create a *NACK* frame for a received *MESSAGE* frame."""
        self.__check('nack', [self.CONNECTED])
        frame = commands.nack(frame, receipt, self.version)
        self._receipt(receipt)
        return frame
    
    def transaction(self, transaction=None):
        """Generate a transaction id which can be used for :meth:`begin`, :meth:`abort`, and :meth:`commit`.
        
        :param transaction: A valid transaction id, or :obj:`None` (automatically generate a unique id).
        """
        return str(transaction or uuid.uuid4())
    
    def begin(self, transaction=None, receipt=None):
        """Create a *BEGIN* frame and begin an abstract STOMP transaction.
        
        :param transaction: See :meth:`transaction`.
        
        .. note :: If you try and begin a pending transaction twice, this will result in a :class:`StompProtocolError`.
        """
        self.__check('begin', [self.CONNECTED])
        frame = commands.begin(transaction, receipt)
        if transaction in self._transactions:
            raise StompProtocolError('Transaction already active: %s' % transaction)
        self._transactions.add(transaction)
        self._receipt(receipt)
        return frame
    
    def abort(self, transaction, receipt=None):
        """Create an *ABORT* frame to abort a STOMP transaction.
        
        :param transaction: See :meth:`transaction`.
        
        .. note :: If you try and abort a transaction which is not pending, this will result in a :class:`StompProtocolError`.
        """
        self.__check('abort', [self.CONNECTED])
        frame = commands.abort(transaction, receipt)
        try:
            self._transactions.remove(transaction)
        except KeyError:
            raise StompProtocolError('Transaction unknown: %s' % transaction)
        self._receipt(receipt)
        return frame
            
    def commit(self, transaction, receipt=None):
        """Send a *COMMIT* command to commit a STOMP transaction.
        
        :param transaction: See :meth:`transaction`.
        
        .. note :: If you try and commit a transaction which is not pending, this will result in a :class:`StompProtocolError`.
        """
        self.__check('commit', [self.CONNECTED])
        frame = commands.commit(transaction, receipt)
        try:
            self._transactions.remove(transaction)
        except KeyError:
            raise StompProtocolError('Transaction unknown: %s' % transaction)
        self._receipt(receipt)
        return frame
    
    def connected(self, frame):
        """Handle a *CONNECTED* frame and set the session state to CONENCTED."""
        self.__check('connected', [self.CONNECTING])
        try:
            self.version, self._server, self._id = commands.connected(frame, versions=self._versions)
        finally:
            self._versions = None
        self._state = self.CONNECTED
        
    def message(self, frame):
        """Handle a *MESSAGE* frame. Returns a token which you can use to match this message to its subscription.
        
        .. seealso :: :meth:`subscribe`.
        """
        self.__check('message', [self.CONNECTED])
        token = commands.message(frame, self.version)
        if token not in self._subscriptions:
            raise StompProtocolError('No such subscription [%s=%s]' % token)
        return token
    
    def receipt(self, frame):
        """Handle a *RECEIPT* frame. Returns the receipt id which you can use to match this receipt to the command that requested it."""
        self.__check('receipt', [self.CONNECTED, self.DISCONNECTING])
        receipt = commands.receipt(frame, self.version)
        try:
            self._receipts.remove(receipt)
        except KeyError:
            raise StompProtocolError('Unexpected receipt: %s' % receipt)
        return receipt
    
    # session information
    
    @property
    def id(self):
        """The session id for the current client-broker connection."""
        return self._id
    
    @property
    def server(self):
        """The server id for the current client-broker connection."""
        return self._server
    
    @property
    def state(self):
        """The current session state."""
        return self._state
    
    #subscription replay
    
    def replay(self):
        """Flush all active subscriptions and return an iterator over the :meth:`subscribe` parameters (*destinations*, *header*, *receipt*, *context*) which you can consume to replay the subscriptions upon the next :meth:`connect`."""
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
