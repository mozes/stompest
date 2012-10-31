"""
Twisted STOMP client

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
import contextlib
import functools

from twisted.internet import defer, reactor, task
from twisted.internet.endpoints import clientFromString

from stompest.error import StompStillRunningError

LOG_CATEGORY = 'stompest.async.util'

class InFlightOperations(object):
    def __init__(self, info, keyError=KeyError):
        self._info = info
        self._keyError = keyError
        self._keys = set()
        self.waiting = None
        
    @contextlib.contextmanager
    def __call__(self, key=None, log=None):
        self.enter(key)
        log and log.debug('%s %s started.' % (self._info, key))
        try:
            yield
        except Exception as e:
            log and log.error('%s %s failed: %s' % (self._info, key, e))
            raise
        finally:
            self.exit(key)
        log and log.debug('%s %s complete.' % (self._info, key))
        
    def __nonzero__(self):
        return bool(self._keys)
        
    def cancel(self):
        self.waiting.cancel()
        self.waiting = None

    def enter(self, key=None):
        if key in self._keys:
            raise self._keyError('[%s] %s already in progress.' % (self._info, key))
        self._keys.add(key)
        
    def exit(self, key=None):
        self._keys.remove(key)
        if self.waiting and (not self):
            self.waiting.callback(None)
    
    @defer.inlineCallbacks
    def wait(self, timeout=None):
        self.waiting = defer.Deferred()
        if timeout is not None:
            timeout = task.deferLater(reactor, timeout, self.cancel)
        try:
            result = yield self.waiting
        finally:
            timeout and timeout.cancel()
            self.waiting = None
        defer.returnValue(result)
    
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
        return result
    _reload()
    
    return _exclusive

def endpointFactory(broker, timeout=None):
    timeout = (':timeout=%d' % timeout) if timeout else ''
    locals().update(broker)
    return clientFromString(reactor, '%(protocol)s:host=%(host)s:port=%(port)d%(timeout)s' % locals())

def sendToErrorDestinationAndRaise(client, failure, frame, errorDestination):
    client.sendToErrorDestination(failure, frame, errorDestination)
    raise failure