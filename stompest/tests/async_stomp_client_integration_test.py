"""
Copyright 2011 Mozes, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import logging
import stomper
from twisted.trial import unittest
from stompest.simple import Stomp
from stompest.async import StompConfig, StompCreator, StompClient
from stompest.error import StompError
from twisted.internet import error, reactor, defer

logging.basicConfig(level=logging.DEBUG)

class StompestTestError(Exception):
    pass

def getClientAckMode():
    mode = 'client'
    if supportsClientIndividual():
        mode = 'client-individual'
    return mode

def supportsClientIndividual():
    supported = False
    queue = '/queue/testClientAckMode'
    stomp = Stomp('localhost', 61613)
    stomp.connect()
    stomp.send(queue, 'test')
    stomp.subscribe(queue, {'ack': 'client-individual'})
    frame = stomp.receiveFrame()
    #Do not ACK.  If client-individual mode is supported, the messages will still be on the broker
    stomp.disconnect()
    stomp.connect()
    stomp.subscribe(queue, {'ack': 'client'})
    if stomp.canRead(1):
        frame = stomp.receiveFrame()
        stomp.ack(frame)
        supported = True
    stomp.disconnect()
    return supported

class HandlerExceptionWithErrorQueueIntegrationTestCase(unittest.TestCase):
    msg1 = 'choke on this'
    msg1Hdrs = {'food': 'barf', 'persistent': 'true'}
    msg2 = 'follow up message'
    queue = '/queue/asyncStompestHandlerExceptionWithErrorQueueUnitTest'
    errorQueue = '/queue/zzz.error.asyncStompestHandlerExceptionWithErrorQueueUnitTest'
    disconnected = defer.Deferred()
    disconnectedAgain = defer.Deferred()
    disconnectedFromErrQ = defer.Deferred()
    ackMode = getClientAckMode()
        
    def cleanQueues(self):
        self.cleanQueue(self.queue)
        self.cleanQueue(self.errorQueue)
    
    def cleanQueue(self, queue):
        stomp = Stomp('localhost', 61613)
        stomp.connect()
        stomp.subscribe(queue, {'ack': 'client'})
        while stomp.canRead(1):
            frame = stomp.receiveFrame()
            stomp.ack(frame)
            print "Dequeued old message: %s" % frame
        stomp.disconnect()        
    
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_errback(self):
        self.cleanQueues()
        self.config = StompConfig('localhost', 61613)
        self.creator = StompCreator(self.config)
        deferConnected = self.creator.getConnection().addCallback(self._onConnected)
        deferredTestErr = self.assertFailure(self.disconnected, StompestTestError).addCallback(self._reconnectAfterErr)
        self.disconnectedAgain.addCallback(self._reconnectForErrQ)
        self.disconnectedFromErrQ.addCallback(self._verifyErrMsg) #asserts err message content is correct
        return defer.DeferredList([
            deferredTestErr, #asserts that handler exception triggered disconnect and was propagated to disconnect deferred
            self.disconnectedAgain, #asserts that message causing the exception was ack'ed to broker (otherwise, this message would not get delivered)
            self.disconnectedFromErrQ, #asserts that we were able to read a message from the error queue
            ]
        )
        
    def _onConnected(self, stomp):
        stomp.getDisconnectedDeferred().addCallback(self.disconnected.callback).addErrback(self.disconnected.errback)
        stomp.send(self.queue, self.msg1, self.msg1Hdrs)
        stomp.send(self.queue, self.msg2)
        #Use selector to guarantee message order.  AMQ doesn't not guarantee order by default
        stomp.subscribe(self.queue, self._saveMsgAndBarf, {'ack': self.ackMode, 'activemq.prefetchSize': 1, 'selector': "food = 'barf'"}, errorDestination=self.errorQueue)
        
    def _saveMsgAndBarf(self, stomp, msg):
        print 'Save message and barf'
        self.assertEquals(self.msg1, msg['body'])
        self.unhandledMsg = msg
        raise StompestTestError('this is a test')
        
    def _reconnectAfterErr(self, result):
        self.creator.getConnection().addCallback(self._onConnectedAgain)
        
    def _onConnectedAgain(self, stomp):
        stomp.getDisconnectedDeferred().addCallback(self.disconnectedAgain.callback).addErrback(self.disconnectedAgain.errback)
        stomp.subscribe(self.queue, self._eatOneMsgAndDisconnect, {'ack': self.ackMode, 'activemq.prefetchSize': 1}, errorDestination=self.errorQueue)
        
    def _eatOneMsgAndDisconnect(self, stomp, msg):
        print 'Eat message and disconnect'
        self.assertEquals(self.msg2, msg['body'])
        stomp.disconnect()
        
    def _reconnectForErrQ(self, result):
        self.creator.getConnection().addCallback(self._onConnectedForErrQ)
        
    def _onConnectedForErrQ(self, stomp):
        stomp.getDisconnectedDeferred().addCallback(self.disconnectedFromErrQ.callback).addErrback(self.disconnectedFromErrQ.errback)
        stomp.subscribe(self.errorQueue, self._saveErrMsgAndDisconnect, {'ack': self.ackMode, 'activemq.prefetchSize': 1})
        
    def _saveErrMsgAndDisconnect(self, stomp, msg):
        print 'Save error message and disconnect'
        self.errQMsg = msg
        stomp.disconnect()
        
    def _verifyErrMsg(self, result):
        self.assertEquals(self.msg1, self.errQMsg['body'])
        self.assertEquals(self.msg1Hdrs['food'], self.errQMsg['headers']['food'])
        self.assertNotEquals(self.unhandledMsg['headers']['message-id'], self.errQMsg['headers']['message-id'])

class GracefulDisconnectTestCase(unittest.TestCase):
    numMsgs = 5
    msgCount = 0
    msg = 'test'
    queue = '/queue/aysncStompestGracefulDisconnectUnitTest'
    disconnected = defer.Deferred()
    disconnectedAgain = defer.Deferred()
    ackMode = getClientAckMode()
    
    def test_onDisconnect_waitForOutstandingMessagesToFinish(self):
        self.config = StompConfig('localhost', 61613)
        self.creator = StompCreator(self.config)
        deferConnected = self.creator.getConnection().addCallback(self._onConnected)
        self.disconnected.addCallback(self._reconnect)
        self.disconnectedAgain.addCallback(self._verifyAllMsgsAcked)
        return defer.DeferredList([
                self.disconnected,
                self.disconnectedAgain,
            ]
        )
        
    def _onConnected(self, stomp):
        stomp.getDisconnectedDeferred().addCallback(self.disconnected.callback).addErrback(self.disconnected.errback)
        for i in range(self.numMsgs):
            stomp.send(self.queue, self.msg)
        stomp.subscribe(self.queue, self._msgHandler, {'ack': self.ackMode, 'activemq.prefetchSize': self.numMsgs})
        
    def _msgHandler(self, stomp, msg):
        self.msgCount += 1
        if self.msgCount < self.numMsgs:
            d = defer.Deferred() 
            reactor.callLater(1, d.callback, None)
            return d
        else:
            stomp.disconnect()
            
    def _reconnect(self, result):
        self.creator.getConnection().addCallback(self._onReconnect)
        
    def _onReconnect(self, stomp):
        stomp.getDisconnectedDeferred().addCallback(self.disconnectedAgain.callback).addErrback(self.disconnectedAgain.errback)
        self.timeExpired = False
        self.timeoutDelayedCall = reactor.callLater(1, self._timesUp, stomp)
        stomp.subscribe(self.queue, self._eatOneMsgAndDisconnect, {'ack': self.ackMode, 'activemq.prefetchSize': self.numMsgs})

    def _timesUp(self, stomp):
        print "Times up!!!"
        self.timeExpired = True
        stomp.disconnect()

    def _eatOneMsgAndDisconnect(self, stomp, msg):
        self.timeoutDelayedCall.cancel()
        stomp.disconnect()
        
    def _verifyAllMsgsAcked(self, result):
        print "VERIFY!"
        self.assertTrue(self.timeExpired)
        return result

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()