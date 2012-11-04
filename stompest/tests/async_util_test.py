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
import logging

from twisted.internet import defer, reactor, task
from twisted.trial import unittest

from stompest.async.util import exclusive, InFlightOperations
from stompest.error import StompCancelledError, StompAlreadyRunningError

logging.basicConfig(level=logging.DEBUG)

LOG_CATEGORY = __name__

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
        self.assertRaises(StompAlreadyRunningError, lambda: f(d))
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
                self.assertRaises(StompAlreadyRunningError, g)
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
        self.assertRaises(StompAlreadyRunningError, h)

        result = yield running
        self.assertEquals(result, ((1, 2), {'a': 3, 'b': 4}))
    
class InFlightOperationsTest(unittest.TestCase):
    def test_dict_interface(self):
        op = InFlightOperations('test')
        self.assertEquals(list(op), [])
        self.assertRaises(KeyError, op.__getitem__, 1)
        self.assertRaises(KeyError, lambda: op[1])
        self.assertRaises(KeyError, op.pop, 1)
        self.assertIdentical(op.get(1), None)
        self.assertIdentical(op.get(1, 2), 2)
        op[1] = []
        self.assertEquals(list(op), [1])
        self.assertEquals(op[1], [])
        self.assertEquals(op.get(1), [])
        self.assertRaises(KeyError, op.__setitem__, 1, [])
        self.assertEquals(op.pop(1), [])
        self.assertRaises(KeyError, op.pop, 1)
        op[1] = []
        self.assertEquals(op.popitem(), (1, []))
        self.assertEquals(list(op), [])
        self.assertEquals(op.setdefault(1, []), [])
        self.assertEquals(op.setdefault(1, []), [])
    
    @defer.inlineCallbacks
    def test_context(self):
        op = InFlightOperations('test')
        with op() as x:
            self.assertEquals(x, None)
            self.assertEquals(list(op), [None])
            self.assertEquals(op.get(x), [])
            self.assertIdentical(op.get(x), op.get())
        self.assertEquals(list(op), [])
        
        with op(log=logging.getLogger(LOG_CATEGORY)) as x:
            self.assertEquals(x, None)
            self.assertEquals(list(op), [None])
            self.assertEquals(op.get(x), [])
            self.assertIdentical(op.get(x), op.get())
        self.assertEquals(list(op), [])
        
        try:
            with op(log=logging.getLogger(LOG_CATEGORY)):
                reactor.callLater(0, op.cancel)
                yield op.wait(timeout=None)
        except StompCancelledError:
            pass
        else:
            raise
        self.assertEquals(list(op), [])
        
        with op(log=logging.getLogger(LOG_CATEGORY)):
            w1 = op.wait(timeout=None)
            w2 = op.wait(timeout=None)
            reactor.callLater(0, op.cancel)
            for w in (w1, w2):
                try:
                    yield w
                except StompCancelledError:
                    pass
                else:
                    raise
                self.assertEquals(list(op), [])
        self.assertEquals(list(op), [])
        
        try:
            with op():
                raise RuntimeError('hi')
        except RuntimeError:
            pass
        self.assertEquals(list(op), [])
        
        try:
            with op():
                wait = op.wait()
                raise RuntimeError('hi')
        except RuntimeError:
            pass
        self.assertEquals(list(op), [])
        try:
            yield wait
        except StompCancelledError:
            pass
        except:
            raise
        
            with op(log=logging.getLogger(LOG_CATEGORY)):
                reactor.callLater(0, op.cancel)
                try:
                    yield op.wait(timeout=None)
                except StompCancelledError:
                    pass
                else:
                    raise
                self.assertEquals(list(op), [None])
        self.assertEquals(list(op), [])
        
        with op(1, logging.getLogger(LOG_CATEGORY)) as x:
            self.assertEquals(x, None)
            self.assertEquals(list(op), [1])
            reactor.callLater(0, op.cancel, 1)
            try:
                yield op.wait(1, None)
            except StompCancelledError:
                pass
            else:
                raise
            self.assertEquals(list(op), [])
        self.assertEquals(list(op), [])
        
    @defer.inlineCallbacks
    def test_plain(self):
        op = InFlightOperations('test')
        op.enter()
        self.assertEquals(list(op), [None])
        self.assertEquals(op.get(), [])
        op.exit()
        self.assertEquals(list(op), [])
        
        op = InFlightOperations('test')
        op.enter()
        d = op.wait()
        op.cancel()
        self.assertEquals(list(op), [])
        try:
            yield d
        except StompCancelledError:
            pass
        else:
            raise
        op.exit()
        self.assertEquals(list(op), [])
        
    @defer.inlineCallbacks
    def test_wait_and_timeout(self):
        op = InFlightOperations('test')
        with op():
            for _ in xrange(5):
                try:
                    yield op.wait(timeout=0)
                except StompCancelledError:
                    pass
                else:
                    raise
            self.assertEquals(list(op), [None])
        self.assertEquals(list(op), [])
        
if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
