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
    VALID_COMMANDS = {
        '1.0': set([
            'ABORT', 'ACK', 'BEGIN', 'COMMIT', 'CONNECT', 'CONNECTED', 'DISCONNECT',
            'ERROR', 'MESSAGE', 'RECEIPT', 'SEND', 'STOMP', 'SUBSCRIBE', 'UNSUBSCRIBE'
        ]),
        '1.1': set([
            'ABORT', 'ACK', 'BEGIN', 'COMMIT', 'CONNECT', 'CONNECTED', 'DISCONNECT',
            'ERROR', 'MESSAGE', 'NACK', 'RECEIPT', 'SEND', 'STOMP', 'SUBSCRIBE', 'UNSUBSCRIBE'
        ])
    }
    
    LINE_DELIMITER = '\n'
    FRAME_DELIMITER = '\x00'
    HEADER_SEPARATOR = ':'
    CONTENT_LENGTH_HEADER = 'content-length'
    