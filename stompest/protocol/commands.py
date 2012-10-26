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
import socket

from stompest.error import StompProtocolError

from .frame import StompFrame
from .spec import StompSpec

# outgoing frames

def stomp(versions, host, login=None, passcode=None, headers=None):
    if (versions is None) or (list(versions) == [StompSpec.VERSION_1_0]):
        raise StompProtocolError('Unsupported command (version %s): %s' % (StompSpec.VERSION_1_0, StompSpec.NACK))
    frame = connect(login=login, passcode=passcode, headers=headers, versions=versions, host=host)
    return StompFrame(StompSpec.STOMP, frame.headers)

def connect(login=None, passcode=None, headers=None, versions=None, host=None):
    headers = dict(headers or [])
    if versions is None:
        versions = [StompSpec.VERSION_1_0]
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

def disconnect(receipt=None, version=None):
    headers = {}
    version = _version(version)
    if receipt is not None:
        if version == StompSpec.VERSION_1_0:
            raise StompProtocolError('%s not supported (version %s)' % (StompSpec.RECEIPT_HEADER, version))
        headers[StompSpec.RECEIPT_HEADER] = receipt
    return StompFrame(StompSpec.DISCONNECT, headers)

def send(destination, body='', headers=None, receipt=None):
    frame = StompFrame(StompSpec.SEND, dict(headers or []), body)
    frame.headers[StompSpec.DESTINATION_HEADER] = destination
    _addReceiptHeader(frame, receipt)
    return frame
    
def subscribe(destination, headers, receipt=None, version=None):
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
    frame = StompFrame(StompSpec.ACK, _ackHeaders(frame, version))
    _addReceiptHeader(frame, receipt)
    return frame

def nack(frame, receipt=None, version=None):
    version = _version(version)
    if version == StompSpec.VERSION_1_0:
        raise StompProtocolError('%s not supported (version %s)' % (StompSpec.NACK, version))
    frame = StompFrame(StompSpec.NACK, _ackHeaders(frame, version))
    _addReceiptHeader(frame, receipt)
    return frame

def begin(transaction, receipt=None):
    frame = StompFrame(StompSpec.BEGIN, {StompSpec.TRANSACTION_HEADER: transaction})
    _addReceiptHeader(frame, receipt)
    return frame

def abort(transaction, receipt=None):
    frame = StompFrame(StompSpec.ABORT, {StompSpec.TRANSACTION_HEADER: transaction})
    _addReceiptHeader(frame, receipt)
    return frame

def commit(transaction, receipt=None):
    frame = StompFrame(StompSpec.COMMIT, {StompSpec.TRANSACTION_HEADER: transaction})
    _addReceiptHeader(frame, receipt)
    return frame

# incoming frames

def handle(frame, version):
    _checkCommand(frame, StompSpec.SERVER_COMMANDS)
    return {
        StompSpec.CONNECTED: connected,
        StompSpec.MESSAGE: message,
        StompSpec.RECEIPT: receipt,
        StompSpec.ERROR: error
    }[frame.cmd](frame, version)

def connected(frame, version=None):
    clientVersion = _version(version)
    serverVersion = clientVersion
    _checkCommand(frame, [StompSpec.CONNECTED])
    headers = frame.headers
    try:
        if clientVersion != StompSpec.VERSION_1_0:
            serverVersion = _version(headers.get(StompSpec.VERSION_HEADER, StompSpec.VERSION_1_0))
            if serverVersion not in versions(clientVersion):
                raise StompProtocolError('')
    except StompProtocolError:
        raise StompProtocolError('Server version incompatible with client version %s [headers=%s]' % (clientVersion, headers))
    
    server = None if (serverVersion == StompSpec.VERSION_1_0) else headers.get(StompSpec.SERVER_HEADER)
    
    try:
        id_ = headers[StompSpec.SESSION_HEADER]
    except KeyError:
        if serverVersion == StompSpec.VERSION_1_0:
            raise StompProtocolError('Invalid %s frame (%s header is missing) [headers=%s]' % (StompSpec.CONNECTED, StompSpec.SESSION_HEADER, headers))
        id_ = None
        
    return serverVersion, server, id_

def message(frame, version):
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
    version = _version(version)
    _checkCommand(frame, [StompSpec.RECEIPT])
    _checkHeader(frame, StompSpec.RECEIPT_ID_HEADER)
    return frame.headers[StompSpec.RECEIPT_ID_HEADER]

def error(frame, version):
    version = _version(version)
    _checkCommand(frame, [StompSpec.ERROR])

# STOMP protocol version

def versions(version):
    version = _version(version)
    for v in StompSpec.VERSIONS:
        yield v
        if v == version:
            break
_versions = versions

def version(version=None):
    if version is None:
        version = StompSpec.DEFAULT_VERSION
    if version not in StompSpec.VERSIONS:
        raise StompProtocolError('Version is not supported [%s]' % version)
    return version
_version = version

# private helper methods

def _ackHeaders(frame, version):
    version = _version(version)
    _checkCommand(frame, [StompSpec.MESSAGE])
    _checkHeader(frame, StompSpec.MESSAGE_ID_HEADER, version)
    if version != StompSpec.VERSION_1_0:
        _checkHeader(frame, StompSpec.SUBSCRIPTION_HEADER, version)
    return dict((key, value) for (key, value) in frame.headers.iteritems() if key in (StompSpec.SUBSCRIPTION_HEADER, StompSpec.MESSAGE_ID_HEADER, StompSpec.TRANSACTION_HEADER))

def _addReceiptHeader(frame, receipt):
    if receipt is not None:
        frame.headers[StompSpec.RECEIPT_HEADER] = receipt

def _checkCommand(frame, commands=None):
    if frame.cmd not in (commands or StompSpec.COMMANDS):
        raise StompProtocolError('Cannot handle command: %s [expected=%s, headers=%s]' % (frame.cmd, ', '.join(commands), frame.headers))

def _checkHeader(frame, header, version=None):
    try:
        return frame.headers[header]
    except KeyError:
        version = ('in version %s' % version) if version else ''
        raise StompProtocolError('Invalid %s frame (%s header mandatory%s) [headers=%s]' % (frame.cmd, header, version, frame.headers))
