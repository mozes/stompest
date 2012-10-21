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
from stompest.error import StompProtocolError
from stompest.protocol import StompFrame

from .spec import StompSpec

class StompSession(object):
    SUPPORTED_VERSIONS = ['1.0', '1.1']
    DEFAULT_VERSION = '1.0'
    
    def __init__(self, version=None):
        self.version = version
        self._subscriptions = []
    
    def replay(self):
        subscriptions, self._subscriptions = self._subscriptions, []
        for (headers, context) in subscriptions:
            dest = headers.pop(StompSpec.DESTINATION_HEADER)
            yield dest, headers, context

    def subscribe(self, dest, headers, context=None):
        if (self.version != '1.0') and (StompSpec.ID_HEADER not in headers):
            raise StompProtocolError('invalid subscription (id header missing) [%s]' % headers)
        headers = dict(headers or [])
        headers[StompSpec.DESTINATION_HEADER] = dest
        self._subscriptions.append((dict(headers), context))
        return StompFrame(StompSpec.SUBSCRIBE, headers)
    
    def unsubscribe(self, subscription):
        header, value = self._getUnsubscribeHeader(subscription)
        self._subscriptions = [(h, c) for (h, c) in self._subscriptions if (header not in h) or (h[header] != value)]
        return StompFrame(cmd=StompSpec.UNSUBSCRIBE, headers={header: value})
    
    def _getUnsubscribeHeader(self, subscription):
        headers = getattr(subscription, 'headers', subscription)
        for header in (StompSpec.ID_HEADER, StompSpec.DESTINATION_HEADER):
            try:
                return header, headers[header]
            except KeyError:
                if (self.version, header) == ('1.0', StompSpec.ID_HEADER):
                    continue
            raise StompProtocolError('invalid unsubscription (%s header missing) [%s]' % (header, headers))
    
    @property
    def version(self):
        return self._version
    
    @version.setter
    def version(self, version):
        version = version or self.DEFAULT_VERSION
        if version not in self.SUPPORTED_VERSIONS:
            raise StompProtocolError('version is not supported [%s]' % version)
        self._version = version

