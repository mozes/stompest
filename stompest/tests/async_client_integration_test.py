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

from twisted.internet import reactor, defer, task
from twisted.trial import unittest

from stompest import async, sync
from stompest.protocol import StompConfig, StompSpec

logging.basicConfig(level=logging.DEBUG)

HOST = 'localhost'
PORT = 61613

CONFIG = StompConfig('tcp://%s:%s' % (HOST, PORT))

class StompestTestError(Exception):
    pass

class HandlerExceptionWithErrorQueueIntegrationTestCase(unittest.TestCase):
    msg1 = 'choke on this'
    msg1Hdrs = {'food': 'barf', 'persistent': 'true'}
    msg2 = 'follow up message'
    queue = '/queue/asyncFailoverHandlerExceptionWithErrorQueueUnitTest'
    errorQueue = '/queue/zzz.error.asyncStompestHandlerExceptionWithErrorQueueUnitTest'
        
    def cleanQueues(self):
        self.cleanQueue(self.queue)
        self.cleanQueue(self.errorQueue)
    
    def cleanQueue(self, queue):
        stomp = sync.Stomp(CONFIG)
        stomp.connect()
        stomp.subscribe(queue, {StompSpec.ACK_HEADER: 'client'})
        while stomp.canRead(1):
            frame = stomp.receiveFrame()
            stomp.ack(frame)
            print "Dequeued old %s" % frame.info()
        stomp.disconnect()
        
    def setUp(self):
        self.cleanQueues()
        
        self.unhandledMsg = None
        self.errQMsg = None
        self.consumedMsg = None
        self.msgsHandled = 0
    
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect_version_1_0(self):
        return self._test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect('1.0')
        
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect_version_1_1(self):
        return self._test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect('1.1')
        
    @defer.inlineCallbacks
    def _test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_disconnect(self, version):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config, alwaysDisconnectOnUnhandledMsg=True, version=version)
        
        #Connect
        client = yield client.connect()
        
        #Enqueue two messages
        client.send(self.queue, self.msg1, self.msg1Hdrs)
        client.send(self.queue, self.msg2)
        
        #Barf on first message so it will get put in error queue
        #Use selector to guarantee message order.  AMQ doesn't not guarantee order by default
        headers = {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1, 'selector': "food = 'barf'"}
        if version != '1.0':
            headers[StompSpec.ID_HEADER] = '4711'
        client.subscribe(self.queue, self._saveMsgAndBarf, headers, errorDestination=self.errorQueue)
        
        #Client disconnected and returned error
        try:
            yield client.disconnected
        except StompestTestError:
            pass
        else:
            self.assertTrue(False)
        
        client = async.Stomp(config) # take a fresh client to prevent replay (we were disconnected by an error)
        
        #Reconnect and subscribe again - consuming second message then disconnecting
        client = yield client.connect()
        headers.pop('selector')
        client.subscribe(self.queue, self._eatOneMsgAndDisconnect, headers, errorDestination=self.errorQueue)
        
        #Client disconnects without error
        yield client.disconnected
        
        #Reconnect and subscribe to error queue
        client = yield client.connect()
        client.subscribe(self.errorQueue, self._saveErrMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Wait for disconnect
        yield client.disconnected
        
        #Verify that first message was in error queue
        self.assertEquals(self.msg1, self.errQMsg.body)
        self.assertEquals(self.msg1Hdrs['food'], self.errQMsg.headers['food'])
        self.assertNotEquals(self.unhandledMsg.headers['message-id'], self.errQMsg.headers['message-id'])
        
        #Verify that second message was consumed
        self.assertEquals(self.msg2, self.consumedMsg.body)

    @defer.inlineCallbacks
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_no_disconnect(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config)
        
        #Connect
        client = yield client.connect()
        
        #Enqueue two messages
        client.send(self.queue, self.msg1, self.msg1Hdrs)
        client.send(self.queue, self.msg2)
        
        #Barf on first msg, disconnect on second msg
        client.subscribe(self.queue, self._barfOneEatOneAndDisonnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1}, errorDestination=self.errorQueue)
        
        #Client disconnects without error
        yield client.disconnected
        
        #Reconnect and subscribe to error queue
        client = yield client.connect()
        client.subscribe(self.errorQueue, self._saveErrMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Wait for disconnect
        yield client.disconnected
        
        #Verify that one message was in error queue (can't guarantee order)
        self.assertNotEquals(None, self.errQMsg)
        self.assertTrue(self.errQMsg.body in (self.msg1, self.msg2))

    @defer.inlineCallbacks
    def test_onhandlerException_disconnect(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config, alwaysDisconnectOnUnhandledMsg=True)
        
        #Connect
        client = yield client.connect()
        
        #Enqueue a message
        client.send(self.queue, self.msg1, self.msg1Hdrs)
        
        #Barf on first msg (implicit disconnect)
        client.subscribe(self.queue, self._saveMsgAndBarf, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Client disconnected and returned error
        try:
            yield client.disconnected
        except StompestTestError:
            pass
        else:
            self.assertTrue(False)
         
        #Reconnect and subscribe again - consuming retried message and disconnecting
        client = async.Stomp(config) # take a fresh client to prevent replay (we were disconnected by an error)
        client = yield client.connect()
        client.subscribe(self.queue, self._eatOneMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Client disconnects without error
        yield client.disconnected
        
        #Verify that message was retried
        self.assertEquals(self.msg1, self.unhandledMsg.body)
        self.assertEquals(self.msg1, self.consumedMsg.body)
        self.assertEquals(self.unhandledMsg.headers['message-id'], self.consumedMsg.headers['message-id'])

    def _saveMsgAndBarf(self, client, msg):
        print 'Save message and barf'
        self.unhandledMsg = msg
        raise StompestTestError('this is a test')
        
    def _barfOneEatOneAndDisonnect(self, client, msg):
        self.msgsHandled += 1
        if self.msgsHandled == 1:
            self._saveMsgAndBarf(client, msg)
        self._eatOneMsgAndDisconnect(client, msg)
        
    def _eatOneMsgAndDisconnect(self, client, msg):
        print 'Eat message and disconnect'
        self.consumedMsg = msg
        client.disconnect()
                
    def _saveErrMsgAndDisconnect(self, client, msg):
        print 'Save error message and disconnect'
        self.errQMsg = msg
        client.disconnect()

class GracefulDisconnectTestCase(unittest.TestCase):
    numMsgs = 5
    msgCount = 0
    msg = 'test'
    queue = '/queue/asyncFailoverGracefulDisconnectUnitTest'
    
    def setUp(self):
        self.cleanQueues()
    
    def cleanQueues(self):
        self.cleanQueue(self.queue)
    
    def cleanQueue(self, queue):
        stomp = sync.Stomp(CONFIG)
        stomp.connect()
        stomp.subscribe(queue, {StompSpec.ACK_HEADER: 'client'})
        while stomp.canRead(1):
            frame = stomp.receiveFrame()
            stomp.ack(frame)
            print "Dequeued old %s" % frame.info()
        stomp.disconnect()
        
    @defer.inlineCallbacks
    def test_onDisconnect_waitForOutstandingMessagesToFinish(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config)
        
        #Connect
        client = yield client.connect()
        
        for _ in xrange(self.numMsgs):
            client.send(self.queue, self.msg)
        client.subscribe(self.queue, self._msgHandler, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': self.numMsgs})
        
        #Wait for disconnect
        yield client.disconnected
        
        #Reconnect and subscribe again to make sure that all messages in the queue were ack'ed
        client = yield client.connect()
        self.timeExpired = False
        self.timeoutDelayedCall = reactor.callLater(1, self._timesUp, client) #@UndefinedVariable
        client.subscribe(self.queue, self._eatOneMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': self.numMsgs})

        #Wait for disconnect
        yield client.disconnected
        
        #Time should have expired if there were no messages left in the queue
        self.assertTrue(self.timeExpired)
                
    def _msgHandler(self, client, msg):
        self.msgCount += 1
        if self.msgCount < self.numMsgs:
            d = defer.Deferred() 
            reactor.callLater(1, d.callback, None) #@UndefinedVariable
            return d
        else:
            client.disconnect()
            
    def _timesUp(self, client):
        print "Time's up!!!"
        self.timeExpired = True
        client.disconnect()

    def _eatOneMsgAndDisconnect(self, client, msg):
        self.timeoutDelayedCall.cancel()
        client.disconnect()

class SubscribeTestCase(unittest.TestCase):
    msg = 'test'
    queue = '/queue/asyncFailoverSubscribeTestCase'
    
    def setUp(self):
        self.cleanQueues()
    
    def cleanQueues(self):
        self.cleanQueue(self.queue)
    
    def cleanQueue(self, queue):
        stomp = sync.Stomp(CONFIG)
        stomp.connect()
        stomp.subscribe(queue, {StompSpec.ACK_HEADER: 'client'})
        while stomp.canRead(1):
            frame = stomp.receiveFrame()
            stomp.ack(frame)
            print "Dequeued old %s" % frame.info()
        stomp.disconnect()
        
    @defer.inlineCallbacks
    def test_unsubscribe(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config)
        
        client = yield client.connect()
        
        for _ in xrange(2):
            client.send(self.queue, self.msg)
            
        self.subscription = client.subscribe(self.queue, self._eatOneMsgAndUnsubscribe, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1'})
        
        self.signal = defer.Deferred()
        yield self.signal
        
        client.subscribe(self.queue, self._eatOneMsgAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1'})

        yield client.disconnected
              
    @defer.inlineCallbacks  
    def _eatOneMsgAndUnsubscribe(self, client, msg):
        client.unsubscribe(self.subscription)
        yield task.deferLater(reactor, 0.25, lambda: None) # wait for UNSUBSCRIBE command to be processed by the server
        self.signal.callback(None)
                
    def _eatOneMsgAndDisconnect(self, client, msg):
        client.disconnect()
        
if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()