"""
Copyright 2011, 2012 Mozes, Inc.

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
import copy
import functools

from stompest.protocol import StompSpec

_RESERVED_HEADERS = [StompSpec.MESSAGE_ID_HEADER, StompSpec.DESTINATION_HEADER, 'timestamp', 'expires', 'priority']

def filterReservedHeaders(headers):
    return dict((header, value) for (header, value) in headers.iteritems() if header not in _RESERVED_HEADERS)

def checkattr(attribute):
    def _checkattr(f):
        @functools.wraps(f)
        def __checkattr(self, *args, **kwargs):
            getattr(self, attribute)
            return f(self, *args, **kwargs)
        return __checkattr
    return _checkattr

def cloneFrame(frame, persistent=None):
    frame = copy.deepcopy(frame)
    headers = filterReservedHeaders(frame.headers)
    if persistent is not None:
        headers['persistent'] = str(bool(persistent)).lower()
    frame.headers = headers
    return frame
