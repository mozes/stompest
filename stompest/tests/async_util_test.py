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
from twisted.internet import defer, reactor, task
from twisted.trial import unittest

from stompest.async.util import exclusive
from stompest.error import StompStillRunningError
from twisted.internet.defer import CancelledError

class ExclusiveWrapperTest(unittest.TestCase):
    @defer.inlineCallbacks
    def test_exclusive_wrapper(self):
        @exclusive
        @defer.inlineCallbacks
        def f(d):
            result = yield task.deferLater(reactor, 0, lambda: (d.errback(RuntimeError('hi')) or 4711))
            defer.returnValue(result)
            
        d = defer.Deferred()
        running = f(d)
        self.assertRaises(StompStillRunningError, lambda: f(d))
        self.assertFalse(running.called)
        result = yield running
        self.assertEquals(result, 4711)        
        self.assertFailure(d, RuntimeError)
        
        @exclusive
        @defer.inlineCallbacks
        def g():
            yield task.deferLater(reactor, 0, lambda: {}[None])
        
        for _ in xrange(5):
            running = g()
            for _ in xrange(5):
                self.assertRaises(StompStillRunningError, g)
            try:
                yield running
            except KeyError:
                pass
            else:
                raise
        
        @exclusive
        def h(*args, **kwargs):
            return task.deferLater(reactor, 0, lambda: (args, kwargs))
        
        running = h(1, 2, a=3, b=4)
        self.assertRaises(StompStillRunningError, h)

        result = yield running
        self.assertEquals(result, ((1, 2), {'a': 3, 'b': 4}))
    
    @defer.inlineCallbacks
    def test_wait_in_wrapped_function(self):
        @exclusive
        @defer.inlineCallbacks
        def f(timeout=None):
            result = yield f.wait(timeout)
            defer.returnValue(result)
        
        running = f()
        yield task.deferLater(reactor, 0, lambda: None) # activates f.wait()
        self.assertRaises(StompStillRunningError, lambda: f())
        self.assertFalse(running.called)
        self.assertNotEquals(f.waiting, None)
        f.waiting.callback(4711)
        result = yield running
        self.assertEquals(result, 4711)
        self.assertTrue(running.called)
        self.assertEquals(f.waiting, None)
        
        running = f()
        yield task.deferLater(reactor, 0, lambda: None)
        f.waiting.errback(RuntimeError('hi'))
        try:
            result = yield running
        except RuntimeError:
            pass
        else:
            raise
        self.assertTrue(running.called)
        self.assertEquals(f.waiting, None)
        
        running = f(0.01)
        yield task.deferLater(reactor, 0, lambda: None)
        try:
            result = yield running
        except CancelledError:
            pass
        self.assertTrue(running.called)
        self.assertEquals(f.waiting, None)        
      
if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
