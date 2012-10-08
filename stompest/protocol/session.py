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
from stompest.error import StompError
from stompest.protocol.spec import StompSpec
from stompest.protocol.failover import StompFailoverProtocol

class StompSession(object):
    SUPPORTED_VERSIONS = ['1.0', '1.1']
    DEFAULT_VERSION = '1.0'
    
    def __init__(self, uri, stompFactory, version=None):
        self.version = version
        self._subscriptions = []
        self._protocol = StompFailoverProtocol(uri) # syntax of uri: cf. stompest.util
        self._stompFactory = stompFactory
    
    def __iter__(self):
        for broker, delay in self._protocol:
            yield self._stompFactory(broker), delay

    def replay(self):
        subscriptions, self._subscriptions = self._subscriptions, []
        return subscriptions

    def subscribe(self, headers, context=None):
        if StompSpec.DESTINATION_HEADER not in headers:
            raise StompError('invalid subscription (destination header missing) [%s]' % headers)
        if (self.version != '1.0') and (StompSpec.ID_HEADER not in headers):
            raise StompError('invalid subscription (id header missing) [%s]' % headers)
        self._subscriptions.append((dict(headers), context))
    
    def unsubscribe(self, headers):
        if (self.version != '1.0') and (StompSpec.ID_HEADER not in headers):
            raise StompError('invalid unsubscription (id header missing) [%s]' % headers)
        if StompSpec.ID_HEADER in headers:
            self._subscriptions = [(h, c) for (h, c) in self._subscriptions if (StompSpec.ID_HEADER not in h) or (h[StompSpec.ID_HEADER] != headers[StompSpec.ID_HEADER])]
        else:
            self._subscriptions = [(h, c) for (h, c) in self._subscriptions if h[StompSpec.DESTINATION_HEADER] != headers[StompSpec.DESTINATION_HEADER]]
    
    @property
    def version(self):
        return self._version
    
    @version.setter
    def version(self, version):
        version = version or self.DEFAULT_VERSION
        if version not in self.SUPPORTED_VERSIONS:
            raise StompError('version is not supported [%s]' % version)
        self._version = version

