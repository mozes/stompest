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
from copy import deepcopy

_RESERVED_HEADERS = ['message-id', 'timestamp', 'expires', 'priority', 'destination']

def filterReservedHeaders(headers):
    filtered = headers.copy()
    for hdr in _RESERVED_HEADERS:
        if hdr in filtered:
            del filtered[hdr]
    return filtered
    
def cloneStompMessage(msg, persistent=None):
    msg = deepcopy(msg)
    headers = filterReservedHeaders(msg['headers'])
    if persistent is not None:
        headers['persistent'] = str(bool(persistent)).lower()
    msg['headers'] = headers
    return msg
