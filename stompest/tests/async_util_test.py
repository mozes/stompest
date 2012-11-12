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
from twisted.internet.defer import CancelledError
from twisted.trial import unittest

from stompest.async.util import exclusive, InFlightOperations, wait
from stompest.error import StompAlreadyRunningError, StompCancelledError

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
        op[1] = w = defer.Deferred()
        self.assertEquals(list(op), [1])
        self.assertIdentical(op[1], w)
        self.assertIdentical(op.get(1), w)
        self.assertRaises(KeyError, op.__setitem__, 1, defer.Deferred())
        self.assertIdentical(op.pop(1), w)
        self.assertRaises(KeyError, op.pop, 1)
        op[1] = w
        self.assertEquals(op.popitem(), (1, w))
        self.assertEquals(list(op), [])
        self.assertIdentical(op.setdefault(1, w), w)
        self.assertIdentical(op.setdefault(1, w), w)
    
    @defer.inlineCallbacks
    def test_context_single(self):
        op = InFlightOperations('test')
        with op(1) as w:
            self.assertEquals(list(op), [1])
            self.assertIsInstance(w, defer.Deferred)
            self.assertIdentical(w, op[1])
            self.assertIdentical(op.get(1), op[1])
        self.assertEquals(list(op), [])
        
        with op(key=2, log=logging.getLogger(LOG_CATEGORY)):
            self.assertEquals(list(op), [2])
            self.assertIsInstance(op.get(2), defer.Deferred)
            self.assertIdentical(op.get(2), op[2])
        self.assertEquals(list(op), [])
        
        try:
            with op(None, logging.getLogger(LOG_CATEGORY)) as w:
                reactor.callLater(0, w.cancel) #@UndefinedVariable
                yield wait(w, timeout=None, fail=None)
        except CancelledError:
            pass
        else:
            raise
        self.assertEquals(list(op), [])
        
        try:
            with op(None, logging.getLogger(LOG_CATEGORY)) as w:
                reactor.callLater(0, w.errback, StompCancelledError('4711')) #@UndefinedVariable
                yield wait(w)
        except StompCancelledError as e:
            self.assertEquals(str(e), '4711')
        else:
            raise
        self.assertEquals(list(op), [])
        
        with op(None, logging.getLogger(LOG_CATEGORY)) as w:
            reactor.callLater(0, w.callback, 4711) #@UndefinedVariable
            result = yield wait(w)
            self.assertEquals(result, 4711)
        self.assertEquals(list(op), [])
        
        try:
            with op(None) as w:
                raise RuntimeError('hi')
        except RuntimeError:
            pass
        self.assertEquals(list(op), [])
        try:
            yield w
        except RuntimeError as e:
            self.assertEquals(str(e), 'hi')
        else:
            raise
        
        try:
            with op(None) as w:
                d = wait(w)
                raise RuntimeError('hi')
        except RuntimeError:
            pass
        self.assertEquals(list(op), [])
        try:
            yield d
        except RuntimeError as e:
            self.assertEquals(str(e), 'hi')
        else:
            pass
    
    @defer.inlineCallbacks
    def test_timeout(self):
        op = InFlightOperations('test')
        with op(None) as w:
            try:
                yield wait(w, timeout=0, fail=RuntimeError('hi'))
            except RuntimeError as e:
                self.assertEquals(str(e), 'hi')
            else:
                raise
            self.assertEquals(list(op), [None])
        self.assertEquals(list(op), [])
 
if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
