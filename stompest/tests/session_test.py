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

from stompest.error import StompError
from stompest.protocol.session import StompSession
   
class StompSessionTest(unittest.TestCase):
    def test_session_init(self):
        session = StompSession()
        self.assertEquals(session.version, StompSession.DEFAULT_VERSION)

        session = StompSession('1.1')
        self.assertEquals(session.version, '1.1')
        
        self.assertRaises(StompError, lambda: StompSession(version='1.2'))

    def test_session_subscribe(self):
        session = StompSession()
        
        headers = {'destination': 'bla1', 'bla2': 'bla3'}
        session.subscribe(headers)
        
        headersWithId1 = {'destination': 'bla2', 'id': 'bla2', 'bla3': 'bla4'}
        session.subscribe(headersWithId1)
        
        headersWithId2 = {'destination': 'bla2', 'id': 'bla3', 'bla4': 'bla5'}
        session.subscribe(headersWithId2)
        
        subscriptions = session.replay()
        self.assertEquals(subscriptions, [(headers, None), (headersWithId1, None), (headersWithId2, None)])
        self.assertEquals(session.replay(), [])
        
        for header, _ in subscriptions:
            session.subscribe(header)
        session.unsubscribe({'id': headersWithId1['id']})
        
        subscriptions = session.replay()
        self.assertEquals(subscriptions, [(headers, None), (headersWithId2, None)])
        
        for header, _ in subscriptions:
            session.subscribe(header)
        session.unsubscribe(headers)
        self.assertEquals(session.replay(), [(headersWithId2, None)])
        
        headersWithoutDestination = {'bla2': 'bla3'}
        self.assertRaises(StompError, lambda: session.subscribe(headersWithoutDestination))
    
        session = StompSession(version='1.1')
        self.assertRaises(StompError, lambda: session.subscribe(headers))
        self.assertRaises(StompError, lambda: session.unsubscribe(headers))
        session.subscribe(headersWithId1)
        session.subscribe(headersWithId2)
        session.unsubscribe(headersWithId1)
        self.assertEquals(session.replay(), [(headersWithId2, None)])
        
if __name__ == '__main__':
    unittest.main()