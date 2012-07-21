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
from .frame import StompFrame

import uuid

def connect(username, password, headers=None):
    headers = dict(headers) if headers else {}
    headers.update( {'login': username, 'passcode': password})
    return StompFrame('CONNECT', headers)

def disconnect():
    return StompFrame('DISCONNECT')

def ack(headers):
    return StompFrame('ACK', headers)
    
def nack(headers):
    return StompFrame('NACK', headers)
    
def subscribe(headers):
    return StompFrame('SUBSCRIBE', headers)

def unsubscribe(headers):
    return StompFrame('UNSUBSCRIBE', headers)

def send(destination, body='', headers=None):
    headers = dict(headers) if headers else {}
    headers['destination'] = destination
    return StompFrame('SEND', headers, body)
    
def transaction(transactionId=None):
    return {'transaction': transactionId or uuid.uuid4()}

def abort(transaction):
    return StompFrame('ABORT', transaction)

def begin(transaction):
    return StompFrame('BEGIN', transaction)
    
def commit(transaction):
    return StompFrame('COMMIT', transaction)
