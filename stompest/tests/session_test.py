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
from stompest.protocol import StompSession, StompSpec
   
class StompSessionTest(unittest.TestCase):
    def test_session_init(self):
        session = StompSession()
        self.assertEquals(session.version, StompSession.DEFAULT_VERSION)

        session = StompSession('1.1')
        self.assertEquals(session.version, '1.1')
        
        self.assertRaises(StompError, lambda: StompSession(version='1.2'))

    def test_session_subscribe(self):
        session = StompSession()
        
        headers = {'bla2': 'bla3'}
        session.subscribe('bla1', headers)
        
        headersWithId1 = {StompSpec.ID_HEADER: 'bla2', 'bla3': 'bla4'}
        session.subscribe('bla2', headersWithId1)
        
        headersWithId2 = {StompSpec.ID_HEADER: 'bla3', 'bla4': 'bla5'}
        session.subscribe('bla2', headersWithId2)
        
        subscriptions = list(session.replay())
        self.assertEquals(subscriptions, [('bla1', headers, None), ('bla2', headersWithId1, None), ('bla2', headersWithId2, None)])
        self.assertEquals(list(session.replay()), [])
        
        for dest, headers_, _ in subscriptions:
            session.subscribe(dest, headers_)
        session.unsubscribe({StompSpec.ID_HEADER: headersWithId1[StompSpec.ID_HEADER]})
        
        subscriptions = list(session.replay())
        self.assertEquals(subscriptions, [('bla1', headers, None), ('bla2', headersWithId2, None)])
        
        for dest, headers_, _ in subscriptions:
            session.subscribe(dest, headers_)
        session.unsubscribe({StompSpec.DESTINATION_HEADER: 'bla1'})
        self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None)])
        
        session = StompSession(version='1.1')
        session.subscribe('bla2', headersWithId1)
        session.subscribe('bla2', headersWithId2)
        session.unsubscribe(headersWithId1)
        self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None)])
        
if __name__ == '__main__':
    unittest.main()