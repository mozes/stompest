"""The synchronous client is dead simple. It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.

Examples
--------

.. automodule:: stompest.examples
    :members:

Producer
^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/producer.py

Transformer
^^^^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/transformer.py

Consumer
^^^^^^^^

.. literalinclude:: ../../stompest/examples/async/consumer.py

API
---
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
import contextlib
import logging
import time

from stompest.error import StompConnectionError, StompProtocolError
from stompest.protocol import StompFailoverTransport, StompSession
from stompest.util import checkattr

from .transport import StompFrameTransport
from stompest.protocol import commands

LOG_CATEGORY = __name__

connected = checkattr('_transport')

class Stomp(object):
    """A synchronous STOMP client. This is the successor of the simple STOMP client in stompest 1.x, but the API is not backward compatible.

    :param config: A :class:`~.StompConfig` object
    
    .. seealso :: :class:`~.StompConfig` for how to set session configuration options, :class:`~.StompSession` for session state, :mod:`~.commands` for all API options which are documented here.
    """
    failoverFactory = StompFailoverTransport
    transportFactory = StompFrameTransport
    
    def __init__(self, config):
        self.log = logging.getLogger(LOG_CATEGORY)
        self._config = config
        self.session = StompSession(self._config.version, self._config.check)
        self._failover = self.failoverFactory(config.uri)
        self._transport = None
    
    def connect(self, headers=None, versions=None, host=None, connectTimeout=None, connectedTimeout=None):
        """connect(headers=None, versions=None, host=None, connectTimeout=None, connectedTimeout=None)
        
        Establish a connection to a STOMP broker. If the wire-level connect fails, attempt a failover according to the settings in the client's :class:`~.StompConfig` object. If there are active subscriptions in the session, replay them when the STOMP session is established. The negotiated version which is applicable to the established STOMP session is stored in the client's :class:`~.StompSession` attribute :attr:`session`.
        
        :param versions: The STOMP protocol versions we wish to support. The default behavior (:obj:`None`) is the same as for the :func:`~.commands.connect` function of the commands API, but the highest supported version will be the one you specified in the :class:`~.StompConfig` object. The version which is valid for the connection about to be initiated will be stored in the client's :class:`~.StompSession` object (attribute :attr:`session`).
        :param connectTimeout: This is the time (in seconds) to wait for the wire-level connection to be established. If :obj:`None`, we will wait indefinitely.
        :param connectedTimeout: This is the time (in seconds) to wait for the STOMP connection to be established (that is, the broker's **CONNECTED** frame to arrive). If :obj:`None`, we will wait indefinitely.
        
        .. seealso :: The :mod:`~.failover` and :mod:`~.session` modules for the details of subscription replay and failover transport.
        """
        try: # preserve existing connection
            self._transport
        except StompConnectionError:
            pass
        else:
            raise StompConnectionError('Already connected to %s' % self._transport)
        
        try:
            for (broker, connectDelay) in self._failover:
                transport = self.transportFactory(broker['host'], broker['port'], self.session.version)
                if connectDelay:
                    self.log.debug('Delaying connect attempt for %d ms' % int(connectDelay * 1000))
                    time.sleep(connectDelay)
                self.log.info('Connecting to %s ...' % transport)
                try:
                    transport.connect(connectTimeout)
                except StompConnectionError as e:
                    self.log.warning('Could not connect to %s [%s]' % (transport, e))
                else:
                    self.log.info('Connection established')
                    self._transport = transport
                    self._connect(headers, versions, host, connectedTimeout)
                    break
        except StompConnectionError as e:
            self.log.error('Reconnect failed [%s]' % e)
            raise
        
    def _connect(self, headers=None, versions=None, host=None, timeout=None):
        frame = self.session.connect(self._config.login, self._config.passcode, headers, versions, host)
        self.sendFrame(frame)
        if not self.canRead(timeout):
            self.session.disconnect()
            raise StompProtocolError('STOMP session connect failed [timeout=%s]' % timeout)
        frame = self.receiveFrame()
        self.session.connected(frame)
        self.log.info('STOMP session established with broker %s' % self._transport)
        for (destination, headers, receipt, _) in self.session.replay():
            self.log.info('Replaying subscription %s' % headers)
            self.subscribe(destination, headers, receipt)
    
    @connected
    def disconnect(self, receipt=None):
        """disconnect(receipt=None)
        
        Send a STOMP **DISCONNECT** command and terminate the STOMP connection.
        
        .. note :: Calling this method will clear the session's active subscriptions unless you request a **RECEIPT** response from the broker. In the latter case, you have to disconnect the wire-level connection and flush the subscriptions yourself by calling ``self.close(flush=True)``.
        """
        self.sendFrame(self.session.disconnect(receipt))
        if not receipt:
            self.close()
    
    # STOMP frames

    @connected
    def send(self, destination, body='', headers=None, receipt=None):
        """send(destination, body='', headers=None, receipt=None)
        
        Send a **SEND** frame.
        """
        self.sendFrame(commands.send(destination, body, headers, receipt))
        
    @connected
    def subscribe(self, destination, headers, receipt=None):
        """subscribe(destination, handler, headers=None, receipt=None)
        
        Send a **SUBSCRIBE** frame to subscribe to a STOMP destination. This method returns a token which you have to keep if you wish to match incoming **MESSAGE** frames to this subscription or to :meth:`~.sync.client.Stomp.client.Stomp.unsubscribe` later.
        """
        frame, token = self.session.subscribe(destination, headers, receipt)
        self.sendFrame(frame)
        return token
    
    @connected
    def unsubscribe(self, token, receipt=None):
        """unsubscribe(token, receipt=None)
        
        Send an **UNSUBSCRIBE** frame to terminate an existing subscription.
        """
        self.sendFrame(self.session.unsubscribe(token, receipt))
        
    @connected
    def ack(self, frame, receipt=None):
        """ack(frame, receipt=None)
        
        Send an **ACK** frame for a received **MESSAGE** frame.
        """
        self.sendFrame(self.session.ack(frame, receipt))
    
    @connected
    def nack(self, headers, receipt=None):
        """nack(frame, receipt=None)
        
        Send a **NACK** frame for a received **MESSAGE** frame.
        """
        self.sendFrame(self.session.nack(headers, receipt))
    
    @connected
    def begin(self, transaction, receipt=None):
        """begin(transaction=None, receipt=None)
        
        Send a **BEGIN** frame to begin a STOMP transaction.
        """
        self.sendFrame(self.session.begin(transaction, receipt))
        
    @connected
    def abort(self, transaction, receipt=None):
        """abort(transaction=None, receipt=None)
        
        Send an **ABORT** frame to abort a STOMP transaction.
        """
        self.sendFrame(self.session.abort(transaction, receipt))
        
    @connected
    def commit(self, transaction, receipt=None):
        """commit(transaction=None, receipt=None)
        
        Send a **COMMIT** frame to commit a STOMP transaction.
        """
        self.sendFrame(self.session.commit(transaction, receipt))
    
    @contextlib.contextmanager
    @connected
    def transaction(self, transaction=None, receipt=None):
        """transaction(transaction=None, receipt=None)
        
        A context manager for STOMP transactions. Upon entering the :obj:`with` block, a transaction will be begun and upon exiting, that transaction will be committed or (if an error occurred) aborted.
        
        **Example:**
        
        >>> client = Stomp(StompConfig('tcp://localhost:61613'))
        >>> client.connect()
        >>> client.subscribe('/queue/test', {'ack': 'client-individual'})
        ('destination', '/queue/test')
        >>> client.canRead(0) # Check that queue is empty.
        False
        >>> with client.transaction(receipt='important') as transaction:
        ...     client.send('/queue/test', 'message with transaction header', {'transaction': transaction})
        ...     client.send('/queue/test', 'message without transaction header')
        ...     raise RuntimeError('poof')
        ... 
        Traceback (most recent call last):
          File "<stdin>", line 4, in <module>
        RuntimeError: poof
        >>> client.receiveFrame()
        StompFrame(command='RECEIPT', headers={'receipt-id': 'important-begin'}, body='')
        >>> client.receiveFrame()
        StompFrame(command='RECEIPT', headers={'receipt-id': 'important-abort'}, body='')
        >>> frame = client.receiveFrame()
        >>> frame.command, frame.body
        ('MESSAGE', 'message without transaction header')
        >>> client.ack(frame)
        >>> client.canRead(0) # frame with transaction header was dropped by the broker
        False
        >>> client.disconnect()
        """
        transaction = self.session.transaction(transaction)
        self.begin(transaction, receipt and ('%s-begin' % receipt))
        try:
            yield transaction
            self.commit(transaction, receipt and ('%s-commit' % receipt))
        except:
            self.abort(transaction, receipt and ('%s-abort' % receipt))
            raise
    
    def message(self, frame):
        """If you received a **MESSAGE** frame, this method will produce a token which allows you to match it against its subscription.
        
        :param frame: a **MESSAGE** frame.
        
        .. note :: If the client is not aware of the subscription, or if we are not connected, this method will raise a :class:`~.StompProtocolError`.
        """
        return self.session.message(frame)
    
    def receipt(self, frame):
        """If you received a **RECEIPT** frame, this method will extract the receipt id which you employed to request that receipt.
        
        :param frame: A **MESSAGE** frame (a :class:`~.StompFrame` object).
        
        .. note :: If the client is not aware of the outstanding receipt, this method will raise a :class:`~.StompProtocolError`.
        """
        return self.session.receipt(frame)
    
    # frame transport
    
    def close(self, flush=True):
        """Close both the client's :attr:`session` (a :class:`~.StompSession` object) and its transport (that is, the wire-level connection with the broker).
        
        :param flush: Decides whether the client's :attr:`session` should forget its active subscriptions or not.
        
        .. note :: If you do not flush the subscriptions, they will be replayed upon this client's next :meth:`~.sync.client.Stomp.connect`!
        """
        self.session.close(flush)
        try:
            self.__transport and self.__transport.disconnect()
        finally:
            self._transport = None
    
    def canRead(self, timeout=None):
        """Tell whether there is an incoming STOMP frame available for us to read.

        :param timeout: This is the time (in seconds) to wait for a frame to become available. If :obj:`None`, we will wait indefinitely.
        
        .. note :: If the wire-level connection is not available, this method will raise a :class:`~.StompConnectionError`!
        """
        return self._transport.canRead(timeout)
        
    def sendFrame(self, frame):
        """Send a raw STOMP frame.
        
        :param frame: Any STOMP frame (represented as a :class:`~.StompFrame` object).

        .. note :: If we are not connected, this method, and all other API commands for sending STOMP frames except :meth:`~.sync.client.Stomp.connect`, will raise a :class:`~.StompConnectionError`. Use this command only if you have to bypass the :class:`~.StompSession` logic and you know what you're doing!
        """
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Sending %s' % frame.info())
        self._transport.send(frame)
            
    def receiveFrame(self):
        """Fetch the next available frame.
        
        .. note :: If we are not connected, this method will raise a :class:`~.StompConnectionError`. Keep in mind that this method will block forever if there are no frames incoming on the wire. Be sure to use peek with ``self.canRead(timeout)`` before!
        """
        frame = self._transport.receive()
        if frame and self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Received %s' % frame.info())
        return frame
    
    @property
    def _transport(self):
        transport = self.__transport
        if not transport:
            raise StompConnectionError('Not connected')
        try:
            transport.canRead(0)
        except Exception as e:
            self.close(flush=False)
            raise e
        return transport
    
    @_transport.setter
    def _transport(self, transport):
        self.__transport = transport
        