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
import itertools
import mock
import unittest

from stompest.error import StompConnectTimeout, StompError
from stompest.protocol.session import StompSession, StompFailoverProtocol
   
class StompSessionTest(unittest.TestCase):
    def test_session_init(self):
        uri = 'failover:tcp://remote1:61615,tcp://localhost:61616,tcp://remote2:61617?randomize=false'
        stompFactory = mock.Mock()
        session = StompSession(uri, stompFactory)
        self.assertEquals(session.version, StompSession.DEFAULT_VERSION)
        self.assertEquals(stompFactory.mock_calls, map(mock.call, []))

        session = StompSession(uri, stompFactory, version='1.1')
        self.assertEquals(session.version, '1.1')
        
        self.assertRaises(StompError, lambda: StompSession(uri, stompFactory, version='1.2'))

    def _test_session_failover(self):
        uri = 'failover:tcp://remote1:61615,tcp://localhost:61616,tcp://remote2:61617?randomize=false,startupMaxReconnectAttempts=3,initialReconnectDelay=7,backOffMultiplier=3.0,maxReconnectAttempts=1'
        stompFactory = lambda broker: broker
        session = iter(StompSession(uri, stompFactory))
        protocol = StompFailoverProtocol(uri)
        self.assertEquals(itertools.islice(session, 4), itertools.islice(protocol, 4))
        self.assertRaises(StompConnectTimeout, session.next)
        
    def test_session_subscribe(self):
        uri = 'tcp://remote1:61615'
        stompFactory = lambda broker: broker
        
        session = StompSession(uri, stompFactory)
        
        headers = {'destination': 'bla1', 'bla2': 'bla3'}
        session.subscribe(headers)
        
        headersWithId1 = {'destination': 'bla2', 'id': 'bla2', 'bla3': 'bla4'}
        session.subscribe(headersWithId1)
        
        headersWithId2 = {'destination': 'bla2', 'id': 'bla3', 'bla4': 'bla5'}
        session.subscribe(headersWithId2)
        
        subscriptions = session.replay()
        self.assertEquals(subscriptions, [headers, headersWithId1, headersWithId2])
        self.assertEquals(session.replay(), [])
        
        for header in subscriptions:
            session.subscribe(header)
        session.unsubscribe({'id': headersWithId1['id']})
        
        subscriptions = session.replay()
        self.assertEquals(subscriptions, [headers, headersWithId2])
        
        for header in subscriptions:
            session.subscribe(header)
        session.unsubscribe(headers)
        self.assertEquals(session.replay(), [headersWithId2])
        
        headersWithoutDestination = {'bla2': 'bla3'}
        self.assertRaises(StompError, lambda: session.subscribe(headersWithoutDestination))
    
        session = StompSession(uri, stompFactory, version='1.1')
        self.assertRaises(StompError, lambda: session.subscribe(headers))
        self.assertRaises(StompError, lambda: session.unsubscribe(headers))
        session.subscribe(headersWithId1)
        session.subscribe(headersWithId2)
        session.unsubscribe(headersWithId1)
        self.assertEquals(session.replay(), [headersWithId2])
        
if __name__ == '__main__':
    unittest.main()