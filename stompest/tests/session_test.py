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
import unittest

from stompest.error import StompProtocolError
from stompest.protocol import StompSession, StompSpec
from stompest.protocol.frame import StompFrame
   
class StompSessionTest(unittest.TestCase):
    def test_session_init(self):
        session = StompSession()
        self.assertEquals(session.version, StompSession.DEFAULT_VERSION)

        session = StompSession('1.1')
        self.assertEquals(session.version, '1.1')
        
        self.assertRaises(StompProtocolError, lambda: StompSession(version='1.2'))

    def test_session_subscribe(self):
        session = StompSession()
        
        headers = {'bla2': 'bla3'}
        subscription = session.subscribe('bla1', headers)
        self.assertEquals(subscription, StompFrame(StompSpec.SUBSCRIBE, {StompSpec.DESTINATION_HEADER: 'bla1', 'bla2': 'bla3'}))
        self.assertEquals(session.token(subscription), (StompSpec.DESTINATION_HEADER, 'bla1'))
        
        headersWithId1 = {StompSpec.ID_HEADER: 'bla2', 'bla3': 'bla4'}
        subscriptionWithId1 = session.subscribe('bla2', headersWithId1)
        self.assertEquals(subscriptionWithId1, StompFrame(StompSpec.SUBSCRIBE, {StompSpec.DESTINATION_HEADER: 'bla2', StompSpec.ID_HEADER: 'bla2', 'bla3': 'bla4'}))
        
        headersWithId2 = {StompSpec.ID_HEADER: 'bla3', 'bla4': 'bla5'}
        session.subscribe('bla2', headersWithId2)
        
        subscriptions = list(session.replay())
        self.assertEquals(subscriptions, [('bla1', headers, None), ('bla2', headersWithId1, None), ('bla2', headersWithId2, None)])
        self.assertEquals(list(session.replay()), [])

        session.subscribe('bla2', headersWithId2)
        session.flush()
        self.assertEquals(list(session.replay()), [])
        
        frames = [session.subscribe(dest, headers_) for dest, headers_, _ in subscriptions]
        session.unsubscribe(session.token(frames[1]))
        
        subscriptions_ = list(session.replay())
        self.assertEquals(subscriptions_, [('bla1', headers, None), ('bla2', headersWithId2, None)])
        
        frames = [session.subscribe(dest, headers_) for dest, headers_, _ in subscriptions]
        session.unsubscribe(frames[1])
        self.assertEquals(list(session.replay()), subscriptions_)

        frames = [session.subscribe(dest, headers_) for dest, headers_, _ in subscriptions]
        session.unsubscribe(frames[1].headers)
        self.assertEquals(list(session.replay()), subscriptions_)

        frames = [session.subscribe(dest, headers_) for dest, headers_, _ in subscriptions_]
        session.unsubscribe(session.token(frames[0]))
        self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None)])
        
        frames = [session.subscribe(dest, headers_) for dest, headers_, _ in subscriptions_]
        session.unsubscribe(frames[0])
        self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None)])
        
        frames = [session.subscribe(dest, headers_) for dest, headers_, _ in subscriptions_]
        session.unsubscribe(frames[0].headers)
        self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None)])
        
        session = StompSession(version='1.1')
        self.assertRaises(StompProtocolError, lambda: session.subscribe('bla1', headers))
        self.assertRaises(StompProtocolError, lambda: session.unsubscribe((StompSpec.DESTINATION_HEADER, 'bla1')))
        frame = session.subscribe('bla2', headersWithId1)
        session.subscribe('bla2', headersWithId2)
        session.unsubscribe(session.token(frame))
        
        subscriptions = list(session.replay())
        self.assertEquals(subscriptions, [('bla2', headersWithId2, None)])
        
if __name__ == '__main__':
    unittest.main()