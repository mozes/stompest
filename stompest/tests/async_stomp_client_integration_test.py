# -*- coding: iso-8859-1 -*-
"""
Copyright 2011, 2012 Mozes, Inc.

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

from twisted.internet import reactor, defer
from twisted.trial import unittest

from stompest.simple import Stomp
from stompest.async import StompConfig, StompCreator
from stompest.protocol.spec import StompSpec

logging.basicConfig(level=logging.DEBUG)

HOST = 'localhost'
PORT = 61613

class StompestTestError(Exception):
    pass

class HandlerExceptionWithErrorQueueIntegrationTestCase(unittest.TestCase):
    msg1 = 'choke on this'
    msg1Hdrs = {'food': 'barf', 'persistent': 'true'}
    msg2 = 'follow up message'
    queue = '/queue/asyncStompestHandlerExceptionWithErrorQueueUnitTest'
    errorQueue = '/queue/zzz.error.asyncStompestHandlerExceptionWithErrorQueueUnitTest'
        
    def cleanQueues(self):
        self.cleanQueue(self.queue)
        self.cleanQueue(self.errorQueue)
    
    def cleanQueue(self, queue):
        stomp = Stomp(HOST, PORT)
        stomp.connect()
        stomp.subscribe(queue, {StompSpec.ACK_HEADER: 'client'})
        while stomp.canRead(1):
            frame = stomp.receiveFrame()
            stomp.ack(frame)
            print "Dequeued old message: %s" % frame
        stomp.disconnect()
        
    def setUp(self):
        self.cleanQueues()
        
        self.unhandledMsg = None
        self.errQMsg = None
        self.consumedMsg = None
        self.msgsHandled = 0

    @defer.inlineCallbacks
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect(self):
        config = StompConfig(HOST, PORT)
        creator = StompCreator(config, alwaysDisconnectOnUnhandledMsg=True)
        
        #Connect
        stomp = yield creator.getConnection()
        
        #Enqueue two messages
        stomp.send(self.queue, self.msg1, self.msg1Hdrs)
        stomp.send(self.queue, self.msg2)
        
        #Barf on first message so it will get put in error queue
        #Use selector to guarantee message order.  AMQ doesn't not guarantee order by default
        stomp.subscribe(self.queue, self._saveMsgAndBarf, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1, 'selector': "food = 'barf'"}, errorDestination=self.errorQueue)
        
        #Client disconnected and returned error
        try:
            yield stomp.getDisconnectedDeferred()
        except StompestTestError:
            pass
        else:
            self.assertTrue(False)
        
        #Reconnect and subscribe again - consuming second message then disconnecting
        stomp = yield creator.getConnection()
        stomp.subscribe(self.queue, self._eatOneMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1}, errorDestination=self.errorQueue)
        
        #Client disconnects without error
        yield stomp.getDisconnectedDeferred()
        
        #Reconnect and subscribe to error queue
        stomp = yield creator.getConnection()
        stomp.subscribe(self.errorQueue, self._saveErrMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Wait for disconnect
        yield stomp.getDisconnectedDeferred()
        
        #Verify that first message was in error queue
        self.assertEquals(self.msg1, self.errQMsg['body'])
        self.assertEquals(self.msg1Hdrs['food'], self.errQMsg['headers']['food'])
        self.assertNotEquals(self.unhandledMsg['headers']['message-id'], self.errQMsg['headers']['message-id'])
        
        #Verify that second message was consumed
        self.assertEquals(self.msg2, self.consumedMsg['body'])

    @defer.inlineCallbacks
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_no_disconnect(self):
        config = StompConfig(HOST, PORT)
        creator = StompCreator(config)
        
        #Connect
        stomp = yield creator.getConnection()
        
        #Enqueue two messages
        stomp.send(self.queue, self.msg1, self.msg1Hdrs)
        stomp.send(self.queue, self.msg2)
        
        #Barf on first msg, disconnect on second msg
        stomp.subscribe(self.queue, self._barfOneEatOneAndDisonnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1}, errorDestination=self.errorQueue)
        
        #Client disconnects without error
        yield stomp.getDisconnectedDeferred()
        
        #Reconnect and subscribe to error queue
        stomp = yield creator.getConnection()
        stomp.subscribe(self.errorQueue, self._saveErrMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Wait for disconnect
        yield stomp.getDisconnectedDeferred()
        
        #Verify that one message was in error queue (can't guarantee order)
        self.assertNotEquals(None, self.errQMsg)
        self.assertTrue(self.errQMsg['body'] in (self.msg1, self.msg2))

    @defer.inlineCallbacks
    def test_onhandlerException_disconnect(self):
        config = StompConfig(HOST, PORT)
        creator = StompCreator(config)
        
        #Connect
        stomp = yield creator.getConnection()
        
        #Enqueue a message
        stomp.send(self.queue, self.msg1, self.msg1Hdrs)
        
        #Barf on first msg (implicit disconnect)
        stomp.subscribe(self.queue, self._saveMsgAndBarf, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Client disconnected and returned error
        try:
            yield stomp.getDisconnectedDeferred()
        except StompestTestError:
            pass
        else:
            self.assertTrue(False)
            
        #Reconnect and subscribe again - consuming retried message and disconnecting
        stomp = yield creator.getConnection()
        stomp.subscribe(self.queue, self._eatOneMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Client disconnects without error
        yield stomp.getDisconnectedDeferred()
        
        #Verify that message was retried
        self.assertEquals(self.msg1, self.unhandledMsg['body'])
        self.assertEquals(self.msg1, self.consumedMsg['body'])
        self.assertEquals(self.unhandledMsg['headers']['message-id'], self.consumedMsg['headers']['message-id'])

    def _saveMsgAndBarf(self, stomp, msg):
        print 'Save message and barf'
        self.unhandledMsg = msg
        raise StompestTestError('this is a test')
        
    def _barfOneEatOneAndDisonnect(self, stomp, msg):
        self.msgsHandled += 1
        if self.msgsHandled == 1:
            self._saveMsgAndBarf(stomp, msg)
        self._eatOneMsgAndDisconnect(stomp, msg)
        
    def _eatOneMsgAndDisconnect(self, stomp, msg):
        print 'Eat message and disconnect'
        self.consumedMsg = msg
        stomp.disconnect()
                
    def _saveErrMsgAndDisconnect(self, stomp, msg):
        print 'Save error message and disconnect'
        self.errQMsg = msg
        stomp.disconnect()

class GracefulDisconnectTestCase(unittest.TestCase):
    numMsgs = 5
    msgCount = 0
    msg = 'test'
    queue = '/queue/aysncStompestGracefulDisconnectUnitTest'
    
    def setUp(self):
        self.cleanQueues()
    
    def cleanQueues(self):
        self.cleanQueue(self.queue)
    
    def cleanQueue(self, queue):
        stomp = Stomp(HOST, PORT)
        stomp.connect()
        stomp.subscribe(queue, {StompSpec.ACK_HEADER: 'client'})
        while stomp.canRead(1):
            frame = stomp.receiveFrame()
            stomp.ack(frame)
            print "Dequeued old message: %s" % frame
        stomp.disconnect()
        
    @defer.inlineCallbacks
    def test_onDisconnect_waitForOutstandingMessagesToFinish(self):
        self.config = StompConfig(HOST, PORT)
        self.creator = StompCreator(self.config)
        
        config = StompConfig(HOST, PORT)
        creator = StompCreator(config)
        
        #Connect
        stomp = yield creator.getConnection()
        
        for _ in xrange(self.numMsgs):
            stomp.send(self.queue, self.msg)
        stomp.subscribe(self.queue, self._msgHandler, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': self.numMsgs})
        
        #Wait for disconnect
        yield stomp.getDisconnectedDeferred()
        
        #Reconnect and subscribe again to make sure that all messages in the queue were ack'ed
        stomp = yield creator.getConnection()
        self.timeExpired = False
        self.timeoutDelayedCall = reactor.callLater(1, self._timesUp, stomp)
        stomp.subscribe(self.queue, self._eatOneMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': self.numMsgs})

        #Wait for disconnect
        yield stomp.getDisconnectedDeferred()
        
        #Time should have expired if there were no messages left in the queue
        self.assertTrue(self.timeExpired)
                
    def _msgHandler(self, stomp, msg):
        self.msgCount += 1
        if self.msgCount < self.numMsgs:
            d = defer.Deferred() 
            reactor.callLater(1, d.callback, None)
            return d
        else:
            stomp.disconnect()
            
    def _timesUp(self, stomp):
        print "Time's up!!!"
        self.timeExpired = True
        stomp.disconnect()

    def _eatOneMsgAndDisconnect(self, stomp, msg):
        self.timeoutDelayedCall.cancel()
        stomp.disconnect()

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()