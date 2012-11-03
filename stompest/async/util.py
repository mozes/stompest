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
import collections
import contextlib
import functools

from twisted.internet import defer, reactor, task
from twisted.internet.endpoints import clientFromString

from stompest.error import StompStillRunningError

class InFlightOperations(collections.MutableMapping):
    def __init__(self, info, keyError=KeyError):
        self.info = info
        self._keyError = keyError
        self._waiting = {}
    
    def __contains__(self, key):
        return key in self._waiting
    
    def __len__(self):
        return len(self._waiting)
    
    def __iter__(self):
        return iter(self._waiting)
    
    def __getitem__(self, key):
        try:
            return self._waiting[key]
        except KeyError:
            raise self._keyError('%s not in progress.' % self._info(key))
    
    def __setitem__(self, key, value):
        if key in self:
            raise self._keyError('%s already in progress.' % self._info(key))
        self._waiting[key] = value
    
    def __delitem__(self, key):
        self[key]
        del self._waiting[key]
    
    def get(self, key=None, default=None):
        return super(InFlightOperations, self).get(key, default)
    
    @contextlib.contextmanager
    def __call__(self, key=None, log=None):
        self.enter(key)
        info = self._info(key)
        log and log.debug('%s started.' % info)
        try:
            yield
        except Exception as e:
            log and log.error('%s failed: %s' % (info, e))
            raise
        finally:
            if key not in self:
                self.log.warning('%s was cancelled in the meantime.' % info)
                return
            self.exit(key)
        log and log.debug('%s complete.' % info)
    
    def enter(self, key=None):
        self[key] = defer.Deferred()
        
    def exit(self, key=None):
        waiting = self.pop(key)
        if not waiting.called:
            waiting.callback(None)
        
    def cancel(self, key=None):
        waiting = self.pop(key)
        if not waiting.called:
            waiting.cancel()
    
    @defer.inlineCallbacks
    def wait(self, key, timeout=None):
        waiting = self[key]
        if timeout is not None:
            timeout = task.deferLater(reactor, timeout, self.cancel, key)
        try:
            yield waiting
        finally:
            timeout and timeout.cancel()
        
    @defer.inlineCallbacks
    def waitall(self, timeout=None):
        if not self:
            return
        waiting = [self.wait(key, timeout) for key in self]
        yield task.cooperate(iter(waiting)).whenDone()
    
    def _info(self, key):
        return ' '.join(filter(None, (self.info, key)))
    
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

def connected(f):
    @functools.wraps(f)
    def _connected(self, *args, **kwargs):
        self._protocol
        return f(self, *args, **kwargs)
    return _connected

def endpointFactory(broker, timeout=None):
    timeout = (':timeout=%d' % timeout) if timeout else ''
    locals().update(broker)
    return clientFromString(reactor, '%(protocol)s:host=%(host)s:port=%(port)d%(timeout)s' % locals())

def sendToErrorDestinationAndRaise(client, failure, frame, errorDestination):
    client.sendToErrorDestination(failure, frame, errorDestination)
    raise failure