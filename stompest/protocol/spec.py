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
class StompSpec(object):
    ABORT = 'ABORT'
    ACK = 'ACK'
    BEGIN = 'BEGIN'
    COMMIT = 'COMMIT'
    CONNECT = 'CONNECT'
    DISCONNECT = 'DISCONNECT'
    NACK = 'NACK'
    SEND = 'SEND'
    STOMP = 'STOMP'
    SUBSCRIBE = 'SUBSCRIBE'
    UNSUBSCRIBE = 'UNSUBSCRIBE'
    
    CLIENT_COMMANDS = {
        '1.0': set([
            ABORT, ACK, BEGIN, COMMIT, CONNECT, DISCONNECT,
            SEND, SUBSCRIBE, UNSUBSCRIBE
        ]),
        '1.1': set([
            ABORT, ACK, BEGIN, COMMIT, CONNECT, DISCONNECT,
            NACK, SEND, STOMP, SUBSCRIBE, UNSUBSCRIBE
        ])
    }
    
    CONNECTED = 'CONNECTED'
    ERROR = 'ERROR'
    MESSAGE = 'MESSAGE'
    RECEIPT = 'RECEIPT'
    
    SERVER_COMMANDS = {
        '1.0': set([CONNECTED, ERROR, MESSAGE, RECEIPT]),
        '1.1': set([CONNECTED, ERROR, MESSAGE, RECEIPT])
    }
    
    COMMANDS = dict(CLIENT_COMMANDS)
    for (version, commands) in SERVER_COMMANDS.iteritems():
        COMMANDS.setdefault(version, set()).update(commands)
    
    LINE_DELIMITER = '\n'
    FRAME_DELIMITER = '\x00'
    HEADER_SEPARATOR = ':'
    
    ACCEPT_VERSION_HEADER = 'accept-version'
    ACK_HEADER = 'ack'
    CONTENT_LENGTH_HEADER = 'content-length'
    CONTENT_TYPE_HEADER = 'content-type'
    DESTINATION_HEADER = 'destination'
    HEART_BEAT_HEADER = 'heart-beat'
    HOST_HEADER = 'host'
    ID_HEADER = 'id'
    LOGIN_HEADER = 'login'
    MESSAGE_ID_HEADER = 'message-id'
    PASSCODE_HEADER = 'passcode'
    RECEIPT_HEADER = 'receipt'
    RECEIPT_ID_HEADER = 'receipt-id'
    SESSION_HEADER = 'session'
    SERVER_HEADER = 'server'
    SUBSCRIPTION_HEADER = 'subscription'
    TRANSACTION_HEADER = 'transaction'
    VERSION_HEADER = 'version'
