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

from stompest.error import StompConnectTimeout
from stompest.protocol.session import StompConfiguration, StompSession

class SessionTest(unittest.TestCase):
    def test_configuration(self):
        uri = 'tcp://localhost:61613'
        configuration = StompConfiguration(uri)
        self.assertEqual(configuration.brokers, [{'host': 'localhost', 'protocol': 'tcp', 'port': 61613}])
        self.assertEqual(configuration.options, {'priorityBackup': False, 'initialReconnectDelay': 10, 'reconnectDelayJitter': 0, 'maxReconnectDelay': 30000, 'backOffMultiplier': 2.0, 'startupMaxReconnectAttempts': 0, 'maxReconnectAttempts': -1, 'useExponentialBackOff': True, 'randomize': True})

        uri = 'tcp://123.456.789.0:61616?randomize=true,maxReconnectAttempts=-1'
        configuration = StompConfiguration(uri)
        self.assertTrue(configuration.options['randomize'])
        self.assertEqual(configuration.options['maxReconnectAttempts'], -1)
        self.assertEqual(configuration.brokers, [{'host': '123.456.789.0', 'protocol': 'tcp', 'port': 61616}])

        uri = 'failover:(tcp://primary:61616,tcp://secondary:61616)?randomize=false,maxReconnectAttempts=2,backOffMultiplier=3.0'
        configuration = StompConfiguration(uri)
        self.assertEqual(configuration.uri, uri)
        self.assertFalse(configuration.options['randomize'])
        self.assertEqual(configuration.options['backOffMultiplier'], 3.0)
        self.assertEqual(configuration.options['maxReconnectAttempts'], 2)
        self.assertEqual(configuration.brokers, [{'host': 'primary', 'protocol': 'tcp', 'port': 61616}, {'host': 'secondary', 'protocol': 'tcp', 'port': 61616}])

        uri = 'failover:tcp://remote1:61616,tcp://localhost:61616,tcp://remote2:61616?priorityBackup=true'
        configuration = StompConfiguration(uri)
        self.assertEqual(configuration.options['priorityBackup'], True)
        self.assertEqual(configuration.brokers, [{'host': 'localhost', 'protocol': 'tcp', 'port': 61616}, {'host': 'remote1', 'protocol': 'tcp', 'port': 61616}, {'host': 'remote2', 'protocol': 'tcp', 'port': 61616}])
    
    def test_configuration_invalid_uris(self):
        for uri in [
            'ssl://localhost:61613', 'tcp://:61613', 'tcp://61613', 'tcp:localhost:61613', 'tcp:/localhost',
            'tcp://localhost:', 'tcp://localhost:a', 'tcp://localhost:61613?randomize=1', 'tcp://localhost:61613?randomize=True',
            'tcp://localhost:61613??=False', 'tcp://localhost:61613?a=False', 'tcp://localhost:61613?maxReconnectDelay=False'
            'failover:(tcp://primary:61616, tcp://secondary:61616)', 'failover:tcp://primary:61616, tcp://secondary:61616',
            'failover:tcp://primary:61616,tcp://secondary:61616)', 'failover:(tcp://primary:61616,tcp://secondary:61616',
        ]:
            self.assertRaises(ValueError, lambda: StompConfiguration(uri))
    
    def test_session(self):
        uri = 'failover:tcp://remote1:61615,tcp://localhost:61616,tcp://remote2:61617?randomize=false,startupMaxReconnectAttempts=3,initialReconnectDelay=7,backOffMultiplier=3.0,maxReconnectAttempts=1'
        stompFactory = mock.Mock()
        session = StompSession(uri, stompFactory)
        self.assertEquals(session.version, StompSession.DEFAULT_VERSION)
        self.assertEquals(stompFactory.mock_calls, map(mock.call, [
            {'host': 'remote1', 'protocol': 'tcp', 'port': 61615},
            {'host': 'localhost', 'protocol': 'tcp', 'port': 61616},
            {'host': 'remote2', 'protocol': 'tcp', 'port': 61617}
        ]))
        
        stompFactory = lambda broker: broker
        
        session = StompSession(uri, stompFactory)
        expectedDelaysAndBrokers = [
            (0, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.007, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}),
            (0.021, {'host': 'remote2', 'protocol': 'tcp', 'port': 61617}),
            (0.063, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615})
        ]
        self._test_reconnect(iter(session), expectedDelaysAndBrokers, maxReconnectAttempts=3)
        
        expectedDelaysAndBrokers = [
            (0, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.007, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616})
        ]
        self._test_reconnect(iter(session), expectedDelaysAndBrokers, maxReconnectAttempts=1)
        
        uri = 'failover:(tcp://remote1:61615,tcp://localhost:61616)?randomize=false,startupMaxReconnectAttempts=3,initialReconnectDelay=7,maxReconnectDelay=8'
        session = StompSession(uri, stompFactory)
        
        expectedDelaysAndBrokers = [
            (0, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.007, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}),
            (0.008, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.008, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616})
        ]   
        self._test_reconnect(iter(session), expectedDelaysAndBrokers, maxReconnectAttempts=3)
        
    def _test_reconnect(self, brokersAndDelays, expectedDelaysAndBrokers, maxReconnectAttempts):
        for (n, ((broker, delay), (expectedDelay, expectedBroker))) in enumerate(itertools.izip(brokersAndDelays, expectedDelaysAndBrokers)):
            self.assertAlmostEquals(delay, expectedDelay, delta=0.0005)
            self.assertEquals(broker, expectedBroker)
            
            if n == maxReconnectAttempts:
                break
        
        self.assertRaises(StompConnectTimeout, brokersAndDelays.next)
        
if __name__ == '__main__':
    unittest.main()