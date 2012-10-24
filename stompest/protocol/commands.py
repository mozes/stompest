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
import uuid

from stompest.error import StompProtocolError

from .frame import StompFrame
from .spec import StompSpec

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

def connected(headers, version=None):
    clientVersion = _version(version)
    serverVersion = clientVersion
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
            raise StompProtocolError('Invalid CONNECTED frame (%s header is missing) [headers=%s]' % (StompSpec.SESSION_HEADER, headers))
        id_ = None
        
    return serverVersion, server, id_

def disconnect(receipt=None, version=None):
    headers = {}
    version = _version(version)
    if receipt is not None:
        if version == StompSpec.VERSION_1_0:
            raise StompProtocolError('%s not supported (version %s)' % (StompSpec.RECEIPT_HEADER, version))
        headers[StompSpec.RECEIPT_HEADER] = receipt
    return StompFrame(StompSpec.DISCONNECT, headers)

def ack(headers, version=None):
    headers = _checkAck(headers, version)        
    return StompFrame(StompSpec.ACK, headers)

def _checkAck(headers, version):
    version = _version(version)
    if StompSpec.MESSAGE_ID_HEADER not in headers:
        raise StompProtocolError('Invalid ACK (%s header mandatory in version %s) [headers=%s]' % (StompSpec.MESSAGE_ID_HEADER, version, headers))
    if version != StompSpec.VERSION_1_0:
        if StompSpec.SUBSCRIPTION_HEADER not in headers:
            raise StompProtocolError('Invalid ACK (%s header mandatory in version %s) [headers=%s]' % (StompSpec.SUBSCRIPTION_HEADER, version, headers))
    return dict((key, value) for (key, value) in headers.iteritems() if key in (StompSpec.SUBSCRIPTION_HEADER, StompSpec.MESSAGE_ID_HEADER, StompSpec.TRANSACTION_HEADER))
    
def nack(headers, version):
    version = _version(version)
    if version == StompSpec.VERSION_1_0:
        raise StompProtocolError('Unsupported command (version %s): %s' % (version, StompSpec.NACK))
    headers = _checkAck(headers, version)
    return StompFrame(StompSpec.NACK, headers)

def subscribe(destination, headers, version):
    version = _version(version)
    if (version != StompSpec.VERSION_1_0) and (StompSpec.ID_HEADER not in headers):
        raise StompProtocolError('Invalid SUBSCRIBE (%s header mandatory in version %s) [headers=%s]' % (StompSpec.ID_HEADER, version, headers))
    headers = dict(headers or [])
    headers[StompSpec.DESTINATION_HEADER] = destination
    return StompFrame(StompSpec.SUBSCRIBE, headers)
    
def unsubscribe(subscription, version):
    version = _version(version)
    headers = dict(getattr(subscription, 'headers', subscription))
    for header in (StompSpec.ID_HEADER, StompSpec.DESTINATION_HEADER):
        try:
            return StompFrame(StompSpec.UNSUBSCRIBE, {header: headers[header]})
        except KeyError:
            if (version, header) == (StompSpec.VERSION_1_0, StompSpec.ID_HEADER):
                continue
        raise StompProtocolError('Invalid UNSUBSCRIBE (%s header mandatory in version %s) [headers=%s]' % (header, version, headers))

def send(destination, body='', headers=None):
    headers = dict(headers or [])
    headers[StompSpec.DESTINATION_HEADER] = destination
    return StompFrame(StompSpec.SEND, headers, body)
    
def transaction(transactionId=None):
    return {StompSpec.TRANSACTION_HEADER: str(transactionId or uuid.uuid4())}

def abort(transaction):
    return StompFrame(StompSpec.ABORT, transaction)

def begin(transaction):
    return StompFrame(StompSpec.BEGIN, transaction)
    
def commit(transaction):
    return StompFrame(StompSpec.COMMIT, transaction)

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
