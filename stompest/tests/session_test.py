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
        subscription.headers.pop(StompSpec.ID_HEADER)
        self.assertEquals(subscription, StompFrame(StompSpec.SUBSCRIBE, {'destination': 'bla1', 'bla2': 'bla3'}))
        
        headersWithId1 = {StompSpec.ID_HEADER: 'bla2', 'bla3': 'bla4'}
        subscriptionWithId1 = session.subscribe('bla2', headersWithId1)
        self.assertEquals(subscriptionWithId1, StompFrame(StompSpec.SUBSCRIBE, {'destination': 'bla2', StompSpec.ID_HEADER: 'bla2', 'bla3': 'bla4'}))
        
        headersWithId2 = {StompSpec.ID_HEADER: 'bla3', 'bla4': 'bla5'}
        session.subscribe('bla2', headersWithId2)
        
        subscriptions = list(session.replay())
        subscriptions[0][1].pop(StompSpec.ID_HEADER)
        self.assertEquals(subscriptions, [('bla1', headers, None), ('bla2', headersWithId1, None), ('bla2', headersWithId2, None)])
        self.assertEquals(list(session.replay()), [])
        
        for s in ({StompSpec.ID_HEADER: headersWithId1[StompSpec.ID_HEADER]}, subscriptionWithId1):
            for dest, headers_, _ in subscriptions:
                session.subscribe(dest, headers_)
            session.unsubscribe(s)
            
            subscriptions_ = list(session.replay())
            subscriptions_[0][1].pop(StompSpec.ID_HEADER)
            self.assertEquals(subscriptions_, [('bla1', headers, None), ('bla2', headersWithId2, None)])
        
        for s in ({StompSpec.DESTINATION_HEADER: 'bla1'}, subscription):
            for dest, headers_, _ in subscriptions_:
                session.subscribe(dest, headers_)
            session.unsubscribe(s)
            
            self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None)])

        session = StompSession(version='1.1')
        self.assertRaises(StompProtocolError, lambda: session.subscribe('bla1', headers))
        self.assertRaises(StompProtocolError, lambda: session.unsubscribe({StompSpec.DESTINATION_HEADER: 'bla1'}))
        session.subscribe('bla2', headersWithId1)
        session.subscribe('bla2', headersWithId2)
        session.unsubscribe(headersWithId1)
        
        subscriptions = list(session.replay())
        self.assertEquals(subscriptions, [('bla2', headersWithId2, None)])
        
if __name__ == '__main__':
    unittest.main()