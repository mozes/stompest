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
from stompest.protocol import StompSession, StompSpec, commands
from stompest.protocol.frame import StompFrame
   
class StompSessionTest(unittest.TestCase):
    def test_session_init(self):
        session = StompSession()
        self.assertEquals(session.version, StompSpec.DEFAULT_VERSION)
        session.send('', '', {})
        session.subscribe('bla1', {'bla2': 'bla3'})
        session.unsubscribe((StompSpec.DESTINATION_HEADER, 'bla1'))

        session = StompSession(check=True)
        self.assertRaises(StompProtocolError, lambda: session.send('', '', {}))
        self.assertRaises(StompProtocolError, lambda: session.subscribe('bla1', {'bla2': 'bla3'}))
        self.assertRaises(StompProtocolError, lambda: session.unsubscribe((StompSpec.DESTINATION_HEADER, 'bla1')))
        
        session = StompSession('1.1', check=True)
        self.assertEquals(session.version, '1.1')
        
        self.assertRaises(StompProtocolError, lambda: StompSession(version='1.2'))
        self.assertRaises(StompProtocolError, lambda: session.send('', '', {}))
    
    def test_session_connect(self):
        session = StompSession(StompSpec.VERSION_1_0)
        self.assertEquals(session.version, '1.0')
        self.assertEquals(session.server, None)
        self.assertEquals(session.id, None)
        self.assertEquals(session.state, StompSession.DISCONNECTED)
        frame = session.connect(login='', passcode='')
        self.assertEquals(session.state, StompSession.CONNECTING)
        self.assertEquals(frame, commands.connect(login='', passcode='', versions=None))
        session.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'}))
        self.assertEquals(session.state, StompSession.CONNECTED)
        self.assertEquals(session.version, StompSpec.VERSION_1_0)
        self.assertEquals(session.server, None)
        self.assertEquals(session.id, 'hi')
        frame = session.disconnect()
        self.assertEquals(frame, commands.disconnect())
        session.close()
        self.assertEquals(session.server, None)
        self.assertEquals(session.id, None)
        self.assertEquals(session.state, StompSession.DISCONNECTED)
        self.assertRaises(StompProtocolError, session.connect, login='', passcode='', versions=['1.1'])

        session = StompSession(version='1.1')
        self.assertEquals(session.version, '1.1')
        frame = session.connect(login='', passcode='')
        self.assertEquals(frame, commands.connect('', '', {}, ['1.0', '1.1']))
        self.assertEquals(session.state, StompSession.CONNECTING)
        session.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SERVER_HEADER: 'moon', StompSpec.SESSION_HEADER: '4711', StompSpec.VERSION_HEADER: StompSpec.VERSION_1_1}))
        self.assertEquals(session.state, StompSession.CONNECTED)
        self.assertEquals(session.server, 'moon')
        self.assertEquals(session.id, '4711')
        self.assertEquals(session.state, StompSession.CONNECTED)
        self.assertEquals(session.version, '1.1')
        frame = session.disconnect('4711')
        self.assertEquals(frame, commands.disconnect('4711'))
        session.close()
        self.assertEquals(session.server, None)
        self.assertEquals(session.id, None)
        self.assertEquals(session.state, StompSession.DISCONNECTED)

        session.connect(login='', passcode='', versions=['1.0', '1.1'])
        session.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: '4711'}))
        self.assertEquals(session.state, StompSession.CONNECTED)
        self.assertEquals(session.server, None)
        self.assertEquals(session.id, '4711')
        self.assertEquals(session.version, '1.0')
        self.assertRaises(StompProtocolError, session.disconnect, 4711)
        frame = session.disconnect()
        session.close()
        self.assertEquals(frame, commands.disconnect())
        self.assertEquals(session.server, None)
        self.assertEquals(session.id, None)
        self.assertEquals(session.state, StompSession.DISCONNECTED)
        
        session = StompSession(version='1.1')
        frame = session.connect(login='', passcode='', versions=['1.1'])
        self.assertEquals(frame, commands.connect('', '', {}, ['1.1']))
        self.assertEquals(session._versions, ['1.1'])
        self.assertEquals(session.state, StompSession.CONNECTING)
        session.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SERVER_HEADER: 'moon', StompSpec.SESSION_HEADER: '4711', StompSpec.VERSION_HEADER: StompSpec.VERSION_1_1}))
        self.assertEquals(session.state, StompSession.CONNECTED)
        self.assertEquals(session._versions, ['1.0', '1.1'])
        self.assertEquals(session.version, '1.1')
        session.disconnect('4711')
        session.close()
        self.assertEquals(session.state, StompSession.DISCONNECTED)
        
    def test_session_subscribe(self):
        session = StompSession()
        headers = {'bla2': 'bla3'}
        frame, token = session.subscribe('bla1', headers, receipt='4711')
        self.assertEquals((frame, token), commands.subscribe('bla1', headers, '4711', version='1.0'))
        
        self.assertEquals(token, (StompSpec.DESTINATION_HEADER, 'bla1'))
        self.assertEquals(token, commands.message(StompFrame(StompSpec.MESSAGE, dict([token, (StompSpec.MESSAGE_ID_HEADER, '4711')])), version='1.0'))
        
        headersWithId1 = {StompSpec.ID_HEADER: 'bla2', 'bla3': 'bla4'}
        frame, tokenWithId1 = session.subscribe('bla2', headersWithId1)
        self.assertEquals((frame, tokenWithId1), commands.subscribe('bla2', headersWithId1, version='1.0'))
        self.assertEquals(tokenWithId1, (StompSpec.ID_HEADER, 'bla2'))
        self.assertEquals(tokenWithId1, commands.message(StompFrame(StompSpec.MESSAGE, dict([(StompSpec.SUBSCRIPTION_HEADER, 'bla2'), (StompSpec.DESTINATION_HEADER, 'bla2'), (StompSpec.MESSAGE_ID_HEADER, '4711')])), version='1.0'))
        
        headersWithId2 = {StompSpec.ID_HEADER: 'bla3', 'bla4': 'bla5'}
        session.subscribe('bla2', headersWithId2)
        
        subscriptions = list(session.replay())
        self.assertEquals(subscriptions, [('bla1', headers, '4711', None), ('bla2', headersWithId1, None, None), ('bla2', headersWithId2, None, None)])
        self.assertEquals(list(session.replay()), [])

        session.subscribe('bla2', headersWithId2)
        self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None, None)])
        session.subscribe('bla2', headersWithId2)
        self.assertRaises(StompProtocolError, session.subscribe, 'bla2', headersWithId2)
        self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None, None)])
        session.subscribe('bla2', headersWithId2)
        session.disconnect()
        session.close(flush=False)
        self.assertEquals(list(session.replay()), [('bla2', headersWithId2, None, None)])
        session.subscribe('bla2', headersWithId2)
        session.close(flush=True)
        self.assertEquals(list(session.replay()), [])
        
        subscriptionsWithoutId1 = [('bla1', headers, None, None), ('bla2', headersWithId2, None, None)]
        
        s = [session.subscribe(dest, headers_) for dest, headers_, _, _ in subscriptions]
        session.unsubscribe(s[1][1])
        self.assertEquals(list(session.replay()), subscriptionsWithoutId1)
        
        subscriptionWithId2 = [('bla2', headersWithId2, None, None)]
        
        s = [session.subscribe(dest, headers_) for dest, headers_, _, _ in subscriptionsWithoutId1]
        session.unsubscribe(s[0][1])
        self.assertEquals(list(session.replay()), subscriptionWithId2)
        
        session.disconnect()

        session = StompSession()
        session.connect(login='', passcode='')
        session.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'}))

        session.subscribe('bla1', headers)
        self.assertRaises(StompProtocolError, lambda: session.unsubscribe((StompSpec.ID_HEADER, 'blub')))
        self.assertRaises(StompProtocolError, lambda: session.unsubscribe(('bla', 'blub')))
        
        frame, token = session.subscribe('bla2', headersWithId1)
        session.subscribe('bla2', headersWithId2)
        session.unsubscribe(token)
        self.assertRaises(StompProtocolError, lambda: session.unsubscribe(token))
        
        session = StompSession(version='1.1')
        session.connect(login='', passcode='')
        session.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SERVER_HEADER: 'moon', StompSpec.SESSION_HEADER: '4711', StompSpec.VERSION_HEADER: StompSpec.VERSION_1_1}))
        
        self.assertRaises(StompProtocolError, lambda: session.subscribe('bla1', headers))
        self.assertRaises(StompProtocolError, lambda: session.unsubscribe((StompSpec.DESTINATION_HEADER, 'bla1')))
        frame, token = session.subscribe('bla2', headersWithId1)
        session.subscribe('bla2', headersWithId2)
        session.unsubscribe(token)
        self.assertRaises(StompProtocolError, lambda: session.unsubscribe(token))

        subscriptions = list(session.replay())
        self.assertEquals(subscriptions, subscriptionWithId2)
        
        session.subscribe('bla2', headersWithId2)
        session.close(flush=True)
        self.assertEquals(list(session.replay()), [])        
        
    def test_session_disconnect(self):
        session = StompSession('1.1', check=True)
        session.connect(login='', passcode='')
        session.connected(StompFrame(StompSpec.CONNECTED, {StompSpec.SESSION_HEADER: 'hi'}))
        headers = {StompSpec.ID_HEADER: 4711}
        session.subscribe('bla', headers)
        frame = session.disconnect(receipt='4711')
        self.assertEquals(frame, commands.disconnect(receipt='4711'))
        self.assertEquals(session.state, session.DISCONNECTING)
        session.close(flush=False)
        self.assertEquals(session.state, session.DISCONNECTED)
        self.assertEquals(list(session.replay()), [('bla', headers, None, None)])
        self.assertEquals(list(session.replay()), [])
        
        self.assertRaises(StompProtocolError, session.disconnect)
        
    def test_session_nack(self):
        session = StompSession(version='1.1')
        frame_ = lambda h: StompFrame(StompSpec.MESSAGE, h)
        for headers in [
            {StompSpec.MESSAGE_ID_HEADER: '4711', StompSpec.SUBSCRIPTION_HEADER: 'bla'},
            {StompSpec.MESSAGE_ID_HEADER: '4711', StompSpec.SUBSCRIPTION_HEADER: 'bla', 'foo': 'bar'}
        ]:
            self.assertEquals(session.nack(frame_(headers)), commands.nack(frame_(headers), version='1.1'))
        
        headers = {StompSpec.MESSAGE_ID_HEADER: '4711', StompSpec.SUBSCRIPTION_HEADER: 'bla'}
        self.assertEquals(session.nack(frame_(headers), receipt='4711'), commands.nack(frame_(headers), receipt='4711', version='1.1'))
        
        self.assertRaises(StompProtocolError, session.nack, frame_({}))
        self.assertRaises(StompProtocolError, session.nack, frame_({StompSpec.MESSAGE_ID_HEADER: '4711'}))
        self.assertRaises(StompProtocolError, session.nack, frame_({StompSpec.SUBSCRIPTION_HEADER: 'bla'}))
        
        session = StompSession(version='1.1', check=True)
        self.assertRaises(StompProtocolError, lambda: session.nack(frame_({StompSpec.MESSAGE_ID_HEADER: '4711', StompSpec.SUBSCRIPTION_HEADER: 'bla'})))
        
    def test_session_transaction(self):
        session = StompSession()
        
        transaction = session.transaction()
        headers = {StompSpec.TRANSACTION_HEADER: transaction, StompSpec.RECEIPT_HEADER: 'bla'}
        frame = session.begin(transaction, receipt='bla')
        self.assertEquals(frame, commands.begin(transaction, receipt='bla'))
        self.assertEquals(frame, StompFrame(StompSpec.BEGIN, headers))
        headers.pop(StompSpec.RECEIPT_HEADER)
        self.assertRaises(StompProtocolError, session.begin, transaction)
        frame = session.abort(transaction)
        self.assertEquals(frame, commands.abort(transaction))
        self.assertEquals(frame, StompFrame(StompSpec.ABORT, headers))
        self.assertRaises(StompProtocolError, session.abort, transaction)
        self.assertRaises(StompProtocolError, session.commit, transaction)
        
        transaction = session.transaction(4711)
        headers = {StompSpec.TRANSACTION_HEADER: '4711'}
        frame = session.begin(transaction)
        self.assertEquals(frame, commands.begin(transaction))
        self.assertEquals(frame, StompFrame(StompSpec.BEGIN, headers))
        frame = session.commit(transaction)
        self.assertEquals(frame, commands.commit(transaction))
        self.assertEquals(frame, StompFrame(StompSpec.COMMIT, headers))
        self.assertRaises(StompProtocolError, session.commit, transaction)
        self.assertRaises(StompProtocolError, session.abort, transaction)
        
        session = StompSession(check=True)
        self.assertRaises(StompProtocolError, session.begin, 4711)
        self.assertRaises(StompProtocolError, session.abort, None)
        self.assertRaises(StompProtocolError, session.commit, None)

if __name__ == '__main__':
    unittest.main()