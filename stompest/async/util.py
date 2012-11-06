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
import collections
import contextlib
import functools

from twisted.internet import defer, reactor, task
from twisted.internet.endpoints import clientFromString

from stompest.error import StompAlreadyRunningError, StompCancelledError, StompNotRunningError

class InFlightOperations(collections.MutableMapping):
    def __init__(self, info):
        self.info = info
        self._waiting = {}
            
    def __len__(self):
        return len(self._waiting)
    
    def __iter__(self):
        return iter(self._waiting)
    
    def __getitem__(self, key):
        try:
            return self._waiting[key]
        except KeyError:
            raise StompNotRunningError('%s not in progress' % self._info(key))
    
    def __setitem__(self, key, value):
        if key in self:
            raise StompAlreadyRunningError('%s already in progress' % self._info(key))
        if not isinstance(value, Waiting):
            raise ValueError('invalid value: %s' % value)
        self._waiting[key] = value
    
    def __delitem__(self, key):
        del self._waiting[key]
    
    @contextlib.contextmanager
    def __call__(self, key, log=None):
        self[key] = waiting = Waiting(self._info(key))
        info = self._info(key)
        log and log.debug('%s started.' % info)
        try:
            yield waiting
            waiting = self[key]
            if not waiting.called:
                waiting.callback(None)
        except Exception as e:
            log and log.error('%s failed [%s]' % (info, e))
            if not waiting.called:
                waiting.errback(e)
            raise
        finally:
            self.pop(key)
        log and log.debug('%s complete.' % info)
    
    def _info(self, key):
        return ' '.join(str(x) for x in (self.info, key) if x is not None)
    
class Waiting(object):
    def __init__(self, info):
        self._waiting = []
        self._deferred = defer.Deferred()
        self._info = info
    
    @property
    def called(self):
        return self._deferred.called
    
    def callback(self, result):
        self._deferred.callback(None)
        for waiting in self._waiting:
            if not waiting.called:
                waiting.callback(result)
    
    def errback(self, fail=None):
        self._deferred.callback(None)
        for waiting in self._waiting:
            if not waiting.called:
                waiting.errback(fail)
    
    def cancel(self):
        self._deferred.callback(None)
        for waiting in self._waiting:
            if not waiting.called:
                waiting.cancel()
    
    def wait(self, timeout=None):
        if self.called:
            self._deferred.callback(None)
        waiting = defer.Deferred()
        self._waiting.append(waiting)
        return wait(waiting, timeout, StompCancelledError('Waited too long for %s to complete. [timeout=%s]' % (self._info, timeout)))

@defer.inlineCallbacks
def wait(deferred, timeout, fail=None):
    if timeout is not None:
        timeout = reactor.callLater(timeout, deferred.errback, fail) #@UndefinedVariable
    try:
        result = yield deferred
    finally:
        if timeout and not timeout.called:
            timeout.cancel()
    defer.returnValue(result)

def exclusive(f):
    @functools.wraps(f)
    def _exclusive(*args, **kwargs):
        if _exclusive.running:
            raise StompAlreadyRunningError('%s still running' % f.__name__)
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