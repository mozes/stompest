"""The synchronous client is dead simple. It does not assume anything about your concurrency model (thread vs process) or force you to use it any particular way. It gets out of your way and lets you do what you want.

Examples
--------

Producer
^^^^^^^^

.. literalinclude:: ../../stompest/examples/sync/producer.py

Consumer
^^^^^^^^

.. literalinclude:: ../../stompest/examples/sync/consumer.py

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
from stompest.protocol import StompFailoverProtocol, StompSession
from stompest.util import checkattr

from .transport import StompFrameTransport
from stompest.protocol import commands

LOG_CATEGORY = __name__

connected = checkattr('_transport')

class Stomp(object):
    """A synchronous STOMP client. This is the successor of the :class:`simple.Stomp` client in stompest 1.x, but the API is not backward compatible.

    :param config: A :class:`StompConfig` object
    """
    factory = StompFrameTransport
    
    def __init__(self, config):
        self.log = logging.getLogger(LOG_CATEGORY)
        self._config = config
        self.session = StompSession(self._config.version, self._config.check)
        self._failover = StompFailoverProtocol(config.uri)
        self._transport = None
    
    def connect(self, headers=None, versions=None, host=None, connectTimeout=None, connectedTimeout=None):
        """connect(headers=None, versions=None, host=None, connectTimeout=None, connectedTimeout=None)
        
        Establish a connection to a STOMP broker. If the wire-level connect fails, attempt a failover according to the settings in the client's :class:`StompConfig` object. If there are active subscriptions in the session, replay them when the STOMP session is established. The negotiated version which is applicable to the established STOMP session is stored in the client's :class:`StompSession` attribute :attr:`session`.
        
        :param headers: Additional STOMP headers. Example: ``headers={'client-id': 'me-myself-and-i'}``
        :param versions: The STOMP versions we wish to support. The default is :obj:`None`, which means that we will offer the broker to accept any version prior or equal to the one you specified in the :class:`StompConfig` object. The actual version is stored in the client's :class:`StompSession` object (attribute :attr:`session`). Example: ``versions=['1.0', '1.1']``
        :param host: The ``host`` header which gives this client a human readable name on the broker side. Example: ``host=moon``
        :param connectTimeout: This is the time (in seconds) to wait for the wire-level connection to be established. If :obj:`None`, we will wait indefinitely.
        :param connectedTimeout: This is the time (in seconds) to wait for the STOMP connection to be established (that is, the broker's ``CONNECTED`` frame to arrive). If :obj:`None`, we will wait indefinitely.
        
        .. seealso :: :mod:`protocol.failover`, :mod:`protocol.session`
        """
        try: # preserve existing connection
            self._transport
        except StompConnectionError:
            pass
        else:
            raise StompConnectionError('Already connected to %s' % self._transport)
        
        try:
            for (broker, connectDelay) in self._failover:
                transport = self.factory(broker['host'], broker['port'], self.session.version)
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
        
        Send a STOMP ``DISCONNECT`` command and terminate the STOMP connection.
        
        :param receipt: Send a STOMP ``receipt`` header with this id. Example: ``receipt='tell-me-when-you-got-that-message'``
        
        .. note :: Calling this method will clear the session's active subscriptions unless you request a ``RECEIPT`` response from the broker. In the latter case, you have to disconnect the wire-level connection and flush the subscriptions yourself by calling ``self.close(flush=True)``.
        """
        self.sendFrame(self.session.disconnect(receipt))
        if not receipt:
            self.close()
    
    # STOMP frames

    @connected
    def send(self, destination, body='', headers=None, receipt=None):
        """send(destination, body='', headers=None, receipt=None)
        
        Send a STOMP ``SEND`` command.
        
        :param destination: Destination for the frame. Example: ``destination='/queue/somewhere'``
        :param body: Message body. Binary content is allowed but must be accompanied by the STOMP header ``content-length`` which specifies the number of bytes in the message body.
        :param headers: Additional STOMP headers. Example: ``headers={'content-length': '1001'}``
        :param receipt: See :meth:`disconnect`.
        """
        self.sendFrame(commands.send(destination, body, headers, receipt))
        
    @connected
    def subscribe(self, destination, headers, receipt=None):
        """subscribe(destination, handler, headers=None, receipt=None)
        
        Send a STOMP ``SUBSCRIBE`` command to subscribe to a STOMP destination. This method returns a token which you must be kept if you wish to match incoming ``MESSAGE`` frames to this subscription or to :meth:`unsubscribe` later.
        
        :param destination: Destination for the subscription. Example: ``destination='/topic/news'``
        :param headers: Additional STOMP headers. Example: ``headers={'activemq.prefetchSize': '100'}``
        :param receipt: See :meth:`disconnect`.
        """
        frame, token = self.session.subscribe(destination, headers, receipt)
        self.sendFrame(frame)
        return token
    
    @connected
    def unsubscribe(self, token, receipt=None):
        """unsubscribe(token, receipt=None)
        
        Send a STOMP ``UNSUBSCRIBE`` command to terminate an existing subscription.
        
        :param token: The result of the :meth:`subscribe` command which initiated the subscription in question.
        :param receipt: See :meth:`disconnect`.
        """
        self.sendFrame(self.session.unsubscribe(token, receipt))
        
    @connected
    def ack(self, headers, receipt=None):
        """ack(frame, receipt=None)
        
        Send a STOMP ``ACK`` command for a received STOMP message.
        
        :param frame: The ``StompFrame`` object representing the ``MESSAGE`` frame we wish to ack.
        :param receipt: See :meth:`disconnect`.
        """
        self.sendFrame(self.session.ack(headers, receipt))
    
    @connected
    def nack(self, headers, receipt=None):
        """nack(frame, receipt=None)
        
        Send a STOMP ``NACK`` command for a received STOMP message.
        
        :param frame: The ``StompFrame`` object representing the ``MESSAGE`` frame we wish to nack.
        :param receipt: See :meth:`disconnect`.
        
        .. note :: This command will raise a :class:`StompProtocolError` if you try and issue it in a STOMP 1.0 session.
        """
        self.sendFrame(self.session.nack(headers, receipt))
    
    @connected
    def begin(self, transaction, receipt=None):
        """begin(transaction=None, receipt=None)
        
        Send a STOMP ``BEGIN`` command to begin a STOMP transaction.
        
        :param transaction: The id of the transaction.
        :param receipt: See :meth:`disconnect`.
        
        .. note :: If you try and begin a pending transaction twice, this will result in a :class:`StompProtocolError`.
        """
        self.sendFrame(self.session.begin(transaction, receipt))
        
    @connected
    def abort(self, transaction, receipt=None):
        """abort(transaction=None, receipt=None)
        
        Send a STOMP ``ABORT`` command to abort a STOMP transaction.
        
        :param transaction: The id of the transaction.
        :param receipt: See :meth:`disconnect`.
        
        .. note :: If you try and abort a transaction which is not pending, this will result in a :class:`StompProtocolError`.
        """
        self.sendFrame(self.session.abort(transaction, receipt))
        
    @connected
    def commit(self, transaction, receipt=None):
        """commit(transaction=None, receipt=None)
        
        Send a STOMP ``COMMIT`` command to commit a STOMP transaction.
        
        :param transaction: The id of the transaction.
        :param receipt: See :meth:`disconnect`.
        
        .. note :: If you try and commit a transaction which is not pending, this will result in a :class:`StompProtocolError`.
        """
        self.sendFrame(self.session.commit(transaction, receipt))
    
    @contextlib.contextmanager
    @connected
    def transaction(self, transaction=None, receipt=None):
        """transaction(transaction=None, receipt=None)
        
        A context manager for STOMP transactions. Upon entering the ``with`` block, a transaction will be begun and upon exiting, that transaction will be committed or (if an error occurred) aborted.

        :param transaction: The id of the transaction.
        :param receipt: If this is not :obj:`None`, it will be used as the ``receipt`` header with all STOMP commands (``BEGIN``, ``ABORT``, ``COMMMIT``) belonging to this transaction (be prepared to handle the broker's ``RECEIPT`` responses!).
        
        .. note :: If you try and commit a transaction which is not pending, this will result in a :class:`StompProtocolError`.
        """
        transaction = self.session.transaction(transaction)
        self.begin(transaction, receipt)
        try:
            yield transaction
            self.commit(transaction, receipt)
        except:
            self.abort(transaction, receipt)
    
    def message(self, frame):
        """If you received a ``MESSAGE`` frame, this method will produce a token which allows you to match it against its subscription.
        
        :param frame: a STOMP ``MESSAGE`` frame.
        
        .. note :: If the client is not aware of the subscription, or if we are not connected, this method will raise a :class:`StompProcolError`.
        """
        return self.session.message(frame)
    
    def receipt(self, frame):
        """If you received a ``RECEIPT`` frame, this method will extract the receipt id which you employed to request that receipt.
        
        :param frame: A STOMP ``MESSAGE`` frame (a :class:`StompFrame` object).
        
        .. note :: If the client is not aware of the outstanding receipt, this method will raise a :class:`StompProcolError`.
        """
        return self.session.receipt(frame)
    
    # frame transport
    
    def close(self, flush=True):
        """Close both the client's :attr:`session` (a :class:`StompSession` object) and its transport (that is, the wire-level connection with the broker).
        
        :param flush: Decides whether the client's :attr:`session` should forget its active subscriptions or not.
        
        .. note :: If you do not flush the subscriptions, they will be replayed upon this client's next :meth:`connect`!
        """
        self.session.close(flush)
        try:
            self.__transport and self.__transport.disconnect()
        finally:
            self._transport = None
    
    def canRead(self, timeout=None):
        """Tell whether there is an incoming STOMP frame available for us to read.

        :param timeout: This is the time (in seconds) to wait for a frame to become available. If :obj:`None`, we will wait indefinitely.
        
        .. note :: If the wire-level connection is not available, this method will raise a ``StompConnectionError``!
        """
        return self._transport.canRead(timeout)
        
    def sendFrame(self, frame):
        """Send a raw STOMP frame.
        
        :param frame: Any STOMP frame (represented as a :class:`StompFrame` object).

        .. note :: If we are not connected, this method, and all other API commands for sending STOMP frames except :meth:`connect`, will raise a :class:`StompConnectionError`. Use this command only if you have to bypass the :class:`StompSession` logic and you know what you're doing!
        """
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug('Sending %s' % frame.info())
        self._transport.send(frame)
            
    def receiveFrame(self):
        """Fetch the next available STOMP frame.
        
        .. note :: If we are not connected, this method will raise a :class:`StompConnectionError`. Keep in mind that this method will block forever if there are no frames incoming on the wire. Be sure to use peek with ``self.canRead(timeout)`` before!
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
        