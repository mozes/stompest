# -*- coding: iso-8859-1 -*-
"""This module implements a low-level and stateless API for all commands of the STOMP protocol version supported by stompest. All STOMP command frames are represented as :class:`~.frame.StompFrame` objects. It forms the basis for :class:`~.session.StompSession` which represents the full state of an abstract STOMP protocol session and (via :class:`~.session.StompSession`) of both high-level STOMP clients. You can use the commands API independently of other stompest modules to roll your own STOMP related functionality.

.. note :: Whenever you have to pass a **version** parameter to a command, this is because the behavior of that command depends on the STOMP protocol version of your current session. The default version is the value of :attr:`StompSpec.DEFAULT_VERSION`, which is currently :obj:`'1.0'` but may change in upcoming versions of stompest (and by you, if you wish to override it). Any command which does not conform to the STOMP protocol version in question will result in a :class:`~.error.StompProtocolError`.

Examples:

>>> from stompest.protocol import commands
>>> versions = list(commands.versions('1.1'))
>>> print versions
['1.0', '1.1']
>>> print repr(commands.connect(versions=versions))
StompFrame(command='CONNECT', headers={'host': 'earth.solar-system', 'accept-version': '1.0,1.1'}, body='')
>>> frame, token = commands.subscribe('/queue/test', {'ack': 'client-individual', 'activemq.prefetchSize': '100'})
>>> print repr(frame)
StompFrame(command='SUBSCRIBE', headers={'ack': 'client-individual', 'destination': '/queue/test', 'activemq.prefetchSize': '100'}, body='')
>>> frame = StompFrame('MESSAGE', {'destination': '/queue/test', 'message-id': '007'}, '¿qué tal estás?')
>>> print repr(frame)
StompFrame(command='MESSAGE', headers={'destination': '/queue/test', 'message-id': '007'}, body='\\xc2\\xbfqu\\xc3\\xa9 tal est\\xc3\\xa1s?')
>>> commands.message(frame, version='1.0') == token # This message matches your subscription.
True
>>> commands.message(frame, version='1.1')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
stompest.error.StompProtocolError: Invalid MESSAGE frame (subscription header mandatory in version 1.1) [headers={'destination': '/queue/test', 'message-id': '007'}]
>>> print repr(commands.disconnect(receipt='message-12345'))
StompFrame(command='DISCONNECT', headers={'receipt': 'message-12345'}, body='')

.. seealso :: Specification of STOMP protocols `1.0 <http://stomp.github.com//stomp-specification-1.0.html>`_ and `1.1 <http://stomp.github.com//stomp-specification-1.1.html>`_, your favorite broker's documentation for additional STOMP headers.
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
import socket

from stompest.error import StompProtocolError

from .frame import StompFrame
from .spec import StompSpec

# outgoing frames

def stomp(versions, host, login=None, passcode=None, headers=None):
    """Create a **STOMP** frame. Not supported in STOMP protocol 1.0, synonymous to :func:`connect` for STOMP protocol 1.1 and higher.
    """
    if (versions is None) or (list(versions) == [StompSpec.VERSION_1_0]):
        raise StompProtocolError('Unsupported command (version %s): %s' % (StompSpec.VERSION_1_0, StompSpec.NACK))
    frame = connect(login=login, passcode=passcode, headers=headers, versions=versions, host=host)
    return StompFrame(StompSpec.STOMP, frame.headers)

def connect(login=None, passcode=None, headers=None, versions=None, host=None):
    """Create a **CONNECT** frame.
    
    :param login: The **login** header.
    :param passcode: The **passcode** header.
    :param headers: Additional STOMP headers.
    :param versions: A list of the STOMP versions we wish to support. The default is :obj:`None`, which means that we will offer the broker to accept any version prior or equal to the default STOMP protocol version.
    :param host: The **host** header which gives this client a human readable name on the broker side.
    """
    headers = dict(headers or [])
    versions = [StompSpec.VERSION_1_0] if (versions is None) else list(sorted(_version(v) for v in versions))
    if list(versions) == [StompSpec.VERSION_1_0]:
        if (login is None) or (passcode is None):
            raise StompProtocolError('Incomplete credentials [login=%s, passcode=%s]' % (login, passcode))
    else:
        headers[StompSpec.ACCEPT_VERSION_HEADER] = ','.join(_version(version) for version in versions)
        if host is None:
            host = socket.gethostbyaddr(socket.gethostname())[0]
        headers[StompSpec.HOST_HEADER] = host
    if login is not None:
        headers[StompSpec.LOGIN_HEADER] = login
    if passcode is not None:
        headers[StompSpec.PASSCODE_HEADER] = passcode
    return StompFrame(StompSpec.CONNECT, headers)

def disconnect(receipt=None):
    """Create a **DISCONNECT** frame.
    
    :param receipt: Add a **receipt** header with this id to request a **RECEIPT** frame from the broker. If :obj:`None`, no such header is added.
    """
    headers = {}
    frame = StompFrame(StompSpec.DISCONNECT, headers)
    _addReceiptHeader(frame, receipt)
    return frame

def send(destination, body='', headers=None, receipt=None):
    """Create a **SEND** frame.
    
    :param destination: Destination for the frame.
    :param body: Message body. Binary content is allowed but must be accompanied by the STOMP header **content-length** which specifies the number of bytes in the message body.
    :param headers: Additional STOMP headers.
    :param receipt: See :func:`disconnect`.
    """
    frame = StompFrame(StompSpec.SEND, dict(headers or []), body)
    frame.headers[StompSpec.DESTINATION_HEADER] = destination
    _addReceiptHeader(frame, receipt)
    return frame
    
def subscribe(destination, headers, receipt=None, version=None):
    """Create a pair (frame, token) of a **SUBSCRIBE** frame and a token which you have to keep if you wish to match incoming **MESSAGE** frames to this subscription  with :func:`message` or to :func:`unsubscribe` later.
    
    :param destination: Destination for the subscription.
    :param headers: Additional STOMP headers.
    :param receipt: See :func:`disconnect`.
    """
    version = _version(version)
    frame = StompFrame(StompSpec.SUBSCRIBE, dict(headers or []))
    frame.headers[StompSpec.DESTINATION_HEADER] = destination
    _addReceiptHeader(frame, receipt)
    subscription = None
    try:
        subscription = _checkHeader(frame, StompSpec.ID_HEADER, version)
    except StompProtocolError:
        if (version != StompSpec.VERSION_1_0):
            raise
    token = (StompSpec.DESTINATION_HEADER, destination) if (subscription is None) else (StompSpec.ID_HEADER, subscription) 
    return frame, token

def unsubscribe(token, receipt=None, version=None):
    """Create an **UNSUBSCRIBE** frame.
    
    :param token: The result of the :func:`subscribe` command which you used to initiate the subscription in question.
    :param receipt: See :meth:`disconnect`.
    """
    version = _version(version)
    frame = StompFrame(StompSpec.UNSUBSCRIBE, dict([token]))
    _addReceiptHeader(frame, receipt)
    try:
        _checkHeader(frame, StompSpec.ID_HEADER, version)
    except StompProtocolError:
        if version != StompSpec.VERSION_1_0:
            raise
        _checkHeader(frame, StompSpec.DESTINATION_HEADER)
    return frame

def ack(frame, receipt=None, version=None):
    """Create an **ACK** frame for a received **MESSAGE** frame.
    
    :param frame: The :class:`~.frame.StompFrame` object representing the **MESSAGE** frame we wish to ack.
    :param receipt: See :func:`disconnect`.
    """
    frame = StompFrame(StompSpec.ACK, _ackHeaders(frame, version))
    _addReceiptHeader(frame, receipt)
    return frame

def nack(frame, receipt=None, version=None):
    """Create a **NACK** frame for a received **MESSAGE** frame.
    
    :param frame: The :class:`~.frame.StompFrame` object representing the **MESSAGE** frame we wish to nack.
    :param receipt: See :func:`disconnect`.
    """
    version = _version(version)
    if version == StompSpec.VERSION_1_0:
        raise StompProtocolError('%s not supported (version %s)' % (StompSpec.NACK, version))
    frame = StompFrame(StompSpec.NACK, _ackHeaders(frame, version))
    _addReceiptHeader(frame, receipt)
    return frame

def begin(transaction, receipt=None):
    """Create a **BEGIN** frame.
    
    :param transaction: The id of the transaction.
    :param receipt: See :meth:`disconnect`.
    """
    frame = StompFrame(StompSpec.BEGIN, {StompSpec.TRANSACTION_HEADER: transaction})
    _addReceiptHeader(frame, receipt)
    return frame

def abort(transaction, receipt=None):
    """Create an **ABORT** frame.
    
    :param transaction: The id of the transaction.
    :param receipt: See :meth:`disconnect`.
    """
    frame = StompFrame(StompSpec.ABORT, {StompSpec.TRANSACTION_HEADER: transaction})
    _addReceiptHeader(frame, receipt)
    return frame

def commit(transaction, receipt=None):
    """Create a **COMMIT** frame.
    
    :param transaction: The id of the transaction.
    :param receipt: See :meth:`disconnect`.
    """
    frame = StompFrame(StompSpec.COMMIT, {StompSpec.TRANSACTION_HEADER: transaction})
    _addReceiptHeader(frame, receipt)
    return frame

# incoming frames

def connected(frame, versions=None):
    """Handle a **CONNECTED** frame.
    
    :param versions: The same **versions** parameter you used to create the **CONNECT** frame.
    """
    versions = [StompSpec.VERSION_1_0] if (versions is None) else list(sorted(_version(v) for v in versions))
    version = versions[-1]
    _checkCommand(frame, [StompSpec.CONNECTED])
    headers = frame.headers
    try:
        if version != StompSpec.VERSION_1_0:
            version = _version(headers.get(StompSpec.VERSION_HEADER, StompSpec.VERSION_1_0))
            if version not in versions:
                raise StompProtocolError('')
    except StompProtocolError:
        raise StompProtocolError('Server version incompatible with accepted versions %s [headers=%s]' % (versions, headers))
    
    server = None if (version == StompSpec.VERSION_1_0) else headers.get(StompSpec.SERVER_HEADER)
    
    try:
        id_ = headers[StompSpec.SESSION_HEADER]
    except KeyError:
        if version == StompSpec.VERSION_1_0:
            raise StompProtocolError('Invalid %s frame (%s header is missing) [headers=%s]' % (StompSpec.CONNECTED, StompSpec.SESSION_HEADER, headers))
        id_ = None
        
    return version, server, id_

def message(frame, version):
    """Handle a **MESSAGE** frame. Returns a token which you can use to match this message to its subscription.
    
    .. seealso :: The :func:`subscribe` command.
    """
    version = _version(version)
    _checkCommand(frame, [StompSpec.MESSAGE])
    _checkHeader(frame, StompSpec.MESSAGE_ID_HEADER)
    destination = _checkHeader(frame, StompSpec.DESTINATION_HEADER)
    subscription = None
    try:
        subscription = _checkHeader(frame, StompSpec.SUBSCRIPTION_HEADER, version)
    except StompProtocolError:
        if version != StompSpec.VERSION_1_0:
            raise
    token = (StompSpec.DESTINATION_HEADER, destination) if (subscription is None) else (StompSpec.ID_HEADER, subscription)
    return token

def receipt(frame, version):
    """Handle a **RECEIPT** frame. Returns the receipt id which you can use to match this receipt to the command that requested it.
    """
    version = _version(version)
    _checkCommand(frame, [StompSpec.RECEIPT])
    _checkHeader(frame, StompSpec.RECEIPT_ID_HEADER)
    return frame.headers[StompSpec.RECEIPT_ID_HEADER]

def error(frame, version):
    """Handle an **ERROR** frame. Does not really do anything except checking that this is an **ERROR** frame.
    """
    version = _version(version)
    _checkCommand(frame, [StompSpec.ERROR])

# STOMP protocol version

def version(version=None):
    """Check whether **version** is a valid STOMP protocol version.
    
    :param version: A candidate version, or :obj:`None` (which is equivalent to the value of :attr:`StompSpec.DEFAULT_VERSION`). 
    """
    if version is None:
        version = StompSpec.DEFAULT_VERSION
    if version not in StompSpec.VERSIONS:
        raise StompProtocolError('Version is not supported [%s]' % version)
    return version
_version = version

def versions(version):
    """Obtain all versions prior or equal to **version**.
    """
    version = _version(version)
    for v in StompSpec.VERSIONS:
        yield v
        if v == version:
            break
_versions = versions

# private helper methods

def _ackHeaders(frame, version):
    version = _version(version)
    _checkCommand(frame, [StompSpec.MESSAGE])
    _checkHeader(frame, StompSpec.MESSAGE_ID_HEADER, version)
    if version != StompSpec.VERSION_1_0:
        _checkHeader(frame, StompSpec.SUBSCRIPTION_HEADER, version)
    return dict((key, value) for (key, value) in frame.headers.iteritems() if key in (StompSpec.SUBSCRIPTION_HEADER, StompSpec.MESSAGE_ID_HEADER, StompSpec.TRANSACTION_HEADER))

def _addReceiptHeader(frame, receipt):
    if not receipt:
        return
    if not isinstance(receipt, basestring):
        raise StompProtocolError('Invalid receipt (not a string): %s' % repr(receipt))
    frame.headers[StompSpec.RECEIPT_HEADER] = str(receipt)

def _checkCommand(frame, commands=None):
    if frame.command not in (commands or StompSpec.COMMANDS):
        raise StompProtocolError('Cannot handle command: %s [expected=%s, headers=%s]' % (frame.command, ', '.join(commands), frame.headers))

def _checkHeader(frame, header, version=None):
    try:
        return frame.headers[header]
    except KeyError:
        version = (' in version %s' % version) if version else ''
        raise StompProtocolError('Invalid %s frame (%s header mandatory%s) [headers=%s]' % (frame.command, header, version, frame.headers))
