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

import commands
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
            destination = headers.pop(StompSpec.DESTINATION_HEADER)
            yield destination, headers, context
    
    def send(self, destination, body, headers):
        return commands.send(destination, body, headers)
    
    def subscribe(self, destination, headers, context=None):
        frame = commands.subscribe(destination, headers, self.version)
        self._subscriptions.append((dict(frame.headers), context))
        return frame
    
    def unsubscribe(self, subscription):
        frame = commands.unsubscribe(subscription, self.version)
        header, value = dict(frame.headers).popitem()
        self._subscriptions = [(h, c) for (h, c) in self._subscriptions if (header not in h) or (h[header] != value)]
        return frame
    
    @property
    def version(self):
        return self._version
    
    @version.setter
    def version(self, version):
        version = version or self.DEFAULT_VERSION
        if version not in self.SUPPORTED_VERSIONS:
            raise StompProtocolError('version is not supported [%s]' % version)
        self._version = version

