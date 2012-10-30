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
import contextlib
import functools

from twisted.internet import defer, reactor, task
from twisted.internet.endpoints import clientFromString

from stompest.error import StompStillRunningError, StompProtocolError

LOG_CATEGORY = 'stompest.async.util'

class ActiveHandlers(object):
    def __init__(self):
        self._handlers = set()
        self.waiting = None
    
    @contextlib.contextmanager
    def __call__(self, handler, log=None):
        self._start(handler)
        log and log.debug('[%s] Handler started.' % handler)
        try:
            yield
        except Exception as e:
            log and log.error('[%s] Handler failed: %s' % (handler, e))
            raise
        finally:
            self._finish(handler)
        log and log.debug('[%s] Handler complete.' % handler)
        
    def __nonzero__(self):
        return bool(self._handlers)
        
    def cancel(self):
        self.waiting.cancel()
        self.waiting = None

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
    
    def _start(self, handler):
        if handler in self._handlers:
            raise StompProtocolError('[%s] Handler already in progress.' % handler)
        self._handlers.add(handler)
        
    def _finish(self, handler):
        self._handlers.remove(handler)
        if self.waiting and (not self):
            self.waiting.callback(None)
    
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
    _reload()
    
    @defer.inlineCallbacks
    def wait(timeout=None):
        _exclusive.waiting = defer.Deferred()
        if timeout is not None:
            timeout = task.deferLater(reactor, timeout, _exclusive.waiting.cancel)
        try:
            result = yield _exclusive.waiting
        finally:
            timeout and timeout.cancel()
            _exclusive.waiting = None
        defer.returnValue(result)
    _exclusive.wait = wait
    
    def cancel():
        _exclusive.waiting.cancel()
        _exclusive.waiting = None
    _exclusive.cancel = cancel
    
    return _exclusive

def endpointFactory(broker, timeout=None):
    timeout = (':timeout=%d' % timeout) if timeout else ''
    locals().update(broker)
    return clientFromString(reactor, '%(protocol)s:host=%(host)s:port=%(port)d%(timeout)s' % locals())

def sendToErrorDestinationAndRaise(client, failure, frame, errorDestination):
    client.sendToErrorDestination(frame, errorDestination)
    raise failure