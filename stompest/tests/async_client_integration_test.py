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
from stompest.async.util import sendToErrorDestinationAndRaise
from stompest.config import StompConfig
from stompest.error import StompConnectionError
from stompest.protocol import StompSpec

logging.basicConfig(level=logging.DEBUG)
LOG_CATEGORY = __name__

HOST = 'localhost'
PORT = 61613

CONFIG = StompConfig('tcp://%s:%s' % (HOST, PORT))

class StompestTestError(Exception):
    pass

class AsyncClientBaseTestCase(unittest.TestCase):
    queue = None
    errorQueue = None
    log = logging.getLogger(LOG_CATEGORY)

    TIMEOUT = 0.1

    def cleanQueues(self):
        self.cleanQueue(self.queue)
        self.cleanQueue(self.errorQueue)

    def cleanQueue(self, queue):
        if not queue:
            return

        client = sync.Stomp(CONFIG)
        client.connect()
        client.subscribe(queue, {StompSpec.ACK_HEADER: 'auto'})
        while client.canRead(0.2):
            frame = client.receiveFrame()
            self.log.debug('Dequeued old %s' % frame.info())
        client.disconnect()

    def setUp(self):
        self.cleanQueues()
        self.unhandledFrame = None
        self.errorQueueFrame = None
        self.consumedFrame = None
        self.framesHandled = 0

    def _saveFrameAndBarf(self, _, frame):
        self.log.info('Save message and barf')
        self.unhandledFrame = frame
        raise StompestTestError('this is a test')

    def _barfOneEatOneAndDisonnect(self, client, frame):
        self.framesHandled += 1
        if self.framesHandled == 1:
            self._saveFrameAndBarf(client, frame)
        self._eatOneFrameAndDisconnect(client, frame)

    def _eatOneFrameAndDisconnect(self, client, frame):
        self.log.debug('Eat message and disconnect')
        self.consumedFrame = frame
        client.disconnect()

    def _saveErrorFrameAndDisconnect(self, client, frame):
        self.log.debug('Save error message and disconnect')
        self.errorQueueFrame = frame
        client.disconnect()

    def _eatFrame(self, client, frame):
        self.log.debug('Eat message')
        self.consumedFrame = frame
        self.framesHandled += 1

    def _nackFrame(self, client, frame):
        self.log.debug('NACK message')
        self.consumedFrame = frame
        self.framesHandled += 1
        client.nack(frame)

    def _onMessageFailedSendToErrorDestinationAndRaise(self, client, failure, frame, errorDestination):
        sendToErrorDestinationAndRaise(client, failure, frame, errorDestination)

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
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT), version=version)
        client = async.Stomp(config)

        #Connect
        client = yield client.connect()

        if client.session.version != version:
            print 'Broker does not support STOMP protocol %s. Skipping this test case.' % version
            yield client.disconnect()
            defer.returnValue(None)

        #Enqueue two messages
        client.send(self.queue, self.frame1, self.msg1Hdrs)
        client.send(self.queue, self.frame2)

        defaultHeaders = {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1}
        if version != '1.0':
            defaultHeaders[StompSpec.ID_HEADER] = '4711'

        #Barf on first message so it will get put in error queue
        #Use selector to guarantee message order.  AMQ doesn't guarantee order by default
        headers = {'selector': "food = 'barf'"}
        headers.update(defaultHeaders)
        client.subscribe(self.queue, self._saveFrameAndBarf, headers, errorDestination=self.errorQueue, onMessageFailed=self._onMessageFailedSendToErrorDestinationAndRaise)

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
        client.subscribe(self.errorQueue, self._saveErrorFrameAndDisconnect, defaultHeaders)

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
        client = async.Stomp(config)

        client = yield client.connect()

        #Enqueue a message
        client.send(self.queue, self.frame1, self.msg1Hdrs)

        #Barf on first frame (implicit disconnect)
        client.subscribe(self.queue, self._saveFrameAndBarf, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': 1}, ack=False, onMessageFailed=self._onMessageFailedSendToErrorDestinationAndRaise)

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
        client = async.Stomp(config, receiptTimeout=1.0)

        #Connect
        client = yield client.connect()
        yield task.cooperate(iter([client.send(self.queue, self.frame, receipt='message-%d' % j) for j in xrange(self.numMsgs)])).whenDone()
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
            client.disconnect(receipt='bye-bye')

    def _timesUp(self, client):
        self.log.debug("Time's up!!!")
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

        token = yield client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1'})
        client.send(self.queue, self.frame)
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)

        client.unsubscribe(token)
        client.send(self.queue, self.frame)
        yield task.deferLater(reactor, 0.2, lambda: None)
        self.assertEquals(self.framesHandled, 1)

        client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1'})
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)
        yield client.disconnect()

    @defer.inlineCallbacks
    def test_replay(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT))
        client = async.Stomp(config)
        client = yield client.connect()
        client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1'})
        client.send(self.queue, self.frame)
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)
        client._protocol.loseConnection()
        try:
            yield client.disconnected
        except StompConnectionError:
            pass
        client = yield client.connect()
        client.send(self.queue, self.frame)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)

        try:
            yield client.disconnect(failure=RuntimeError('Hi'))
        except RuntimeError as e:
            self.assertEquals(str(e), 'Hi')

        client = yield client.connect()
        client.send(self.queue, self.frame)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)

        yield client.disconnect()

class NackTestCase(AsyncClientBaseTestCase):
    frame = 'test'
    queue = '/queue/asyncFailoverSubscribeTestCase'

    @defer.inlineCallbacks
    def test_nack(self):
        config = StompConfig(uri='tcp://%s:%d' % (HOST, PORT), version='1.1')
        client = async.Stomp(config)

        client = yield client.connect()
        if client.session.version == '1.0':
            print 'Broker does only support STOMP protocol 1.0. Skipping this test case.'
            yield client.disconnect()
            defer.returnValue(None)

        client.subscribe(self.queue, self._nackFrame, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1', 'id': '4711'}, ack=False)
        client.send(self.queue, self.frame)
        while self.framesHandled != 1:
            yield task.deferLater(reactor, 0.01, lambda: None)

        yield client.disconnect()

        client = yield client.connect()
        client.subscribe(self.queue, self._eatFrame, {StompSpec.ACK_HEADER: 'client-individual', 'activemq.prefetchSize': '1', 'id': '4711'}, ack=True)
        client.send(self.queue, self.frame)
        while self.framesHandled != 2:
            yield task.deferLater(reactor, 0.01, lambda: None)

        yield client.disconnect()

if __name__ == '__main__':
    import sys
    from twisted.scripts import trial
    sys.argv.extend([sys.argv[0]])
    trial.run()
