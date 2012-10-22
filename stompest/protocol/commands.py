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
import uuid

from stompest.error import StompProtocolError

from .frame import StompFrame
from .spec import StompSpec

def connect(username, password, headers=None):
    headers = dict(headers or [])
    headers.update({StompSpec.LOGIN_HEADER: username, StompSpec.PASSCODE_HEADER: password})
    return StompFrame(StompSpec.CONNECT, headers)

def disconnect():
    return StompFrame(StompSpec.DISCONNECT)

def ack(headers):
    headers = dict((key, value) for (key, value) in headers.iteritems() if key in (StompSpec.SUBSCRIPTION_HEADER, StompSpec.MESSAGE_ID_HEADER, StompSpec.TRANSACTION_HEADER))
    return StompFrame(StompSpec.ACK, headers)
    
def nack(headers):
    headers = dict((key, value) for (key, value) in headers.iteritems() if key in (StompSpec.SUBSCRIPTION_HEADER, StompSpec.MESSAGE_ID_HEADER, StompSpec.TRANSACTION_HEADER))
    return StompFrame(StompSpec.NACK, headers)

def subscribe(destination, headers, version):
    if (version != '1.0') and (StompSpec.ID_HEADER not in headers):
        raise StompProtocolError('invalid subscription (id header missing) [%s]' % headers)
    headers = dict(headers or [])
    headers[StompSpec.DESTINATION_HEADER] = destination
    return StompFrame(StompSpec.SUBSCRIBE, headers)
    
def unsubscribe(subscription, version):
    headers = getattr(subscription, 'headers', subscription)
    for header in (StompSpec.ID_HEADER, StompSpec.DESTINATION_HEADER):
        try:
            return StompFrame(StompSpec.UNSUBSCRIBE, {header: headers[header]})
        except KeyError:
            if (version, header) == ('1.0', StompSpec.ID_HEADER):
                continue
        raise StompProtocolError('invalid unsubscription (%s header missing) [%s]' % (header, headers))

def send(destination, body='', headers=None):
    headers = dict(headers or [])
    headers[StompSpec.DESTINATION_HEADER] = destination
    return StompFrame(StompSpec.SEND, headers, body)
    
def transaction(transactionId=None):
    return {StompSpec.TRANSACTION_HEADER: transactionId or uuid.uuid4()}

def abort(transaction):
    return StompFrame(StompSpec.ABORT, transaction)

def begin(transaction):
    return StompFrame(StompSpec.BEGIN, transaction)
    
def commit(transaction):
    return StompFrame(StompSpec.COMMIT, transaction)
