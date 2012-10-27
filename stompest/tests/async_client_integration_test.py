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

class AsyncClientBaseTestCase(unittest.TestCase):
    queue = None
    errorQueue = None
    
    def cleanQueues(self):
        self.cleanQueue(self.queue)
        self.cleanQueue(self.errorQueue)
    
    def cleanQueue(self, queue):
        if not queue:
            return
        
        stomp = sync.Stomp(CONFIG)
        stomp.connect()
        stomp.subscribe(queue, {StompSpec.ACK_HEADER: 'client'})
        while stomp.canRead(0.2):
            frame = stomp.receiveFrame()
            stomp.ack(frame)
            print "Dequeued old %s" % frame.info()
        stomp.disconnect()
        
    def setUp(self):
        self.cleanQueues()
        
        self.unhandledFrame = None
        self.errorQueueFrame = None
        self.consumedFrame = None
        self.framesHandled = 0
        
    def _saveFrameAndBarf(self, _, frame):
        print 'Save message and barf'
        self.unhandledFrame = frame
        raise StompestTestError('this is a test')
        
    def _barfOneEatOneAndDisonnect(self, client, frame):
        self.framesHandled += 1
        if self.framesHandled == 1:
            self._saveFrameAndBarf(client, frame)
        self._eatOneFrameAndDisconnect(client, frame)
    
    def _eatOneFrameAndDisconnect(self, client, frame):
        print 'Eat message and disconnect'
        self.consumedFrame = frame
        client.disconnect()
    
    def _saveErrorFrameAndDisconnect(self, client, frame):
        print 'Save error message and disconnect'
        self.errorQueueFrame = frame
        client.disconnect()

    @defer.inlineCallbacks  
    def _eatOneFrameAndUnsubscribe(self, client, _):
        'Eat message and unsubscribe'
        client.unsubscribe(self.subscription)
        yield task.deferLater(reactor, 0.25, lambda: None) # wait for UNSUBSCRIBE command to be processed by the server
        self.signal.callback(None)

class HandlerExceptionWithErrorQueueIntegrationTestCase(AsyncClientBaseTestCase):
    frame1 = 'choke on this'
    msg1Hdrs = {'food': 'barf', 'persistent': 'true'}
    frame2 = 'follow up message'
    queue = '/queue/asyncFailoverHandlerExceptionWithErrorQueueUnitTest'
    errorQueue = '/queue/zzz.error.asyncStompestHandlerExceptionWithErrorQueueUnitTest'
    
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
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        client.send(self.queue, self.frame2)
        
        #Barf on first message so it will get put in error queue
        #Use selector to guarantee message order.  AMQ doesn't guarantee order by default
        headers = {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1, 'selector': "food = 'barf'"}
        if version != '1.0':
            headers[StompSpec.ID_HEADER] = '4711'
        client.subscribe(self.queue, self._saveFrameAndBarf, headers, errorDestination=self.errorQueue)
        
        #Client disconnected and returned error
        try:
            yield client.disconnected
        except StompestTestError:
            pass
        else:
            raise
        
        client = async.Stomp(config) # take a fresh client to prevent replay (we were disconnected by an error)
        
        #Reconnect and subscribe again - consuming second message then disconnecting
        client = yield client.connect()
        headers.pop('selector')
        client.subscribe(self.queue, self._eatOneFrameAndDisconnect, headers, errorDestination=self.errorQueue)
        
        #Client disconnects without error
        yield client.disconnected
        
        #Reconnect and subscribe to error queue
        client = yield client.connect()
        client.subscribe(self.errorQueue, self._saveErrorFrameAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Wait for disconnect
        yield client.disconnected
        
        #Verify that first message was in error queue
        self.assertEquals(self.frame1, self.errorQueueFrame.body)
        self.assertEquals(self.msg1Hdrs['food'], self.errorQueueFrame.headers['food'])
        self.assertNotEquals(self.unhandledFrame.headers['message-id'], self.errorQueueFrame.headers['message-id'])
        
        #Verify that second message was consumed
        self.assertEquals(self.frame2, self.consumedFrame.body)

    @defer.inlineCallbacks
    def test_onhandlerException_ackMessage_filterReservedHdrs_send2ErrorQ_and_no_disconnect(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config)
        
        #Connect
        client = yield client.connect()
        
        #Enqueue two messages
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        client.send(self.queue, self.frame2)
        
        #Barf on first frame, disconnect on second frame
        client.subscribe(self.queue, self._barfOneEatOneAndDisonnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1}, errorDestination=self.errorQueue)
        
        #Client disconnects without error
        yield client.disconnected
        
        #Reconnect and subscribe to error queue
        client = yield client.connect()
        client.subscribe(self.errorQueue, self._saveErrorFrameAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Wait for disconnect
        yield client.disconnected
        
        #Verify that one message was in error queue (can't guarantee order)
        self.assertNotEquals(None, self.errorQueueFrame)
        self.assertTrue(self.errorQueueFrame.body in (self.frame1, self.frame2))

    @defer.inlineCallbacks
    def test_onhandlerException_disconnect(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config, alwaysDisconnectOnUnhandledMsg=True)
        
        #Connect
        client = yield client.connect()
        
        #Enqueue a message
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        
        #Barf on first frame (implicit disconnect)
        client.subscribe(self.queue, self._saveFrameAndBarf, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Client disconnected and returned error
        try:
            yield client.disconnected
        except StompestTestError:
            pass
        else:
            raise
         
        #Reconnect and subscribe again - consuming retried message and disconnecting
        client = async.Stomp(config) # take a fresh client to prevent replay (we were disconnected by an error)
        client = yield client.connect()
        client.subscribe(self.queue, self._eatOneFrameAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1})
        
        #Client disconnects without error
        yield client.disconnected
        
        #Verify that message was retried
        self.assertEquals(self.frame1, self.unhandledFrame.body)
        self.assertEquals(self.frame1, self.consumedFrame.body)
        self.assertEquals(self.unhandledFrame.headers['message-id'], self.consumedFrame.headers['message-id'])

class GracefulDisconnectTestCase(AsyncClientBaseTestCase):
    numMsgs = 5
    msgCount = 0
    frame = 'test'
    queue = '/queue/asyncFailoverGracefulDisconnectUnitTest'
    
    @defer.inlineCallbacks
    def test_onDisconnect_waitForOutstandingMessagesToFinish(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config)
        
        #Connect
        client = yield client.connect()
        
        for _ in xrange(self.numMsgs):
            client.send(self.queue, self.frame)
        client.subscribe(self.queue, self._frameHandler, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': self.numMsgs})
        
        #Wait for disconnect
        yield client.disconnected
        
        #Reconnect and subscribe again to make sure that all messages in the queue were ack'ed
        client = yield client.connect()
        self.timeExpired = False
        self.timeoutDelayedCall = reactor.callLater(1, self._timesUp, client) #@UndefinedVariable
        client.subscribe(self.queue, self._eatOneFrameAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': self.numMsgs})

        #Wait for disconnect
        yield client.disconnected
        
        #Time should have expired if there were no messages left in the queue
        self.assertTrue(self.timeExpired)
                
    def _frameHandler(self, client, _):
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

class SubscribeTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncFailoverSubscribeTestCase'
    
    @defer.inlineCallbacks
    def test_unsubscribe(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config)
        
        client = yield client.connect()
        
        for _ in xrange(2):
            client.send(self.queue, self.frame)
            
        self.subscription = client.subscribe(self.queue, self._eatOneFrameAndUnsubscribe, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1'})
        
        self.signal = defer.Deferred()
        yield self.signal
        
        client.subscribe(self.queue, self._eatOneFrameAndDisconnect, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1'})

        yield client.disconnected
        
if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()