"""
Twisted STOMP client

Copyright 2011 Mozes, Inc.

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
import functools

from twisted.internet import defer, reactor, task
from twisted.internet.endpoints import clientFromString

from stompest.error import StompStillRunningError

def endpointFactory(broker, timeout=None):
    timeout = (':timeout=%d' % timeout) if timeout else ''
    locals().update(broker)
    return clientFromString(reactor, '%(protocol)s:host=%(host)s:port=%(port)d%(timeout)s' % locals())

def exclusive(f):
    @functools.wraps(f)
    def _exclusive(*args, **kwargs):
        if _exclusive.running:
            raise StompStillRunningError('%s still running' % f.__name__)
        _exclusive.running = True
        task.deferLater(reactor, 0, f, *args, **kwargs).addBoth(_reload).chainDeferred(_exclusive.result)
        return _exclusive.result
    
    def _reload(result=None):
        _exclusive.running = False
        _exclusive.result = defer.Deferred()
        _exclusive.waiting = None
        return result
    
    @defer.inlineCallbacks
    def wait(timeout):
        try:
            _exclusive.waiting = defer.Deferred()
            timeout = timeout and task.deferLater(reactor, timeout, _exclusive.waiting.cancel)
            result = yield _exclusive.waiting
            timeout and timeout.cancel()
        finally:
            _exclusive.waiting = None
        defer.returnValue(result)
        
    _reload()
    _exclusive.wait = wait
    return _exclusive

def sendToErrorDestinationAndRaise(client, failure, frame, errorDestination):
    client.sendToErrorDestination(frame, errorDestination)
    raise failure