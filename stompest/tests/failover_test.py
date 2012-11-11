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
import unittest

from stompest.error import StompConnectTimeout
from stompest.protocol.failover import StompFailoverUri, StompFailoverTransport

class StompFailoverUriTest(unittest.TestCase):
    def test_configuration(self):
        uri = 'tcp://localhost:61613'
        configuration = StompFailoverUri(uri)
        self.assertEquals(configuration.brokers, [{'host': 'localhost', 'protocol': 'tcp', 'port': 61613}])
        self.assertEquals(configuration.options, {'priorityBackup': False, 'initialReconnectDelay': 10, 'reconnectDelayJitter': 0, 'maxReconnectDelay': 30000, 'backOffMultiplier': 2.0, 'startupMaxReconnectAttempts': 0, 'maxReconnectAttempts': -1, 'useExponentialBackOff': True, 'randomize': True})

        uri = 'tcp://123.456.789.0:61616?randomize=true,maxReconnectAttempts=-1,priorityBackup=true'
        configuration = StompFailoverUri(uri)
        self.assertTrue(configuration.options['randomize'])
        self.assertEquals(configuration.options['priorityBackup'], True)
        self.assertEquals(configuration.options['maxReconnectAttempts'], -1)
        self.assertEquals(configuration.brokers, [{'host': '123.456.789.0', 'protocol': 'tcp', 'port': 61616}])

        uri = 'failover:(tcp://primary:61616,tcp://secondary:61616)?randomize=false,maxReconnectAttempts=2,backOffMultiplier=3.0'
        configuration = StompFailoverUri(uri)
        self.assertEquals(configuration.uri, uri)
        self.assertFalse(configuration.options['randomize'])
        self.assertEquals(configuration.options['backOffMultiplier'], 3.0)
        self.assertEquals(configuration.options['maxReconnectAttempts'], 2)
        self.assertEquals(configuration.brokers, [
            {'host': 'primary', 'protocol': 'tcp', 'port': 61616},
            {'host': 'secondary', 'protocol': 'tcp', 'port': 61616}
        ])
    
    def test_configuration_invalid_uris(self):
        for uri in [
            'ssl://localhost:61613', 'tcp://:61613', 'tcp://61613', 'tcp:localhost:61613', 'tcp:/localhost',
            'tcp://localhost:', 'tcp://localhost:a', 'tcp://localhost:61613?randomize=1', 'tcp://localhost:61613?randomize=True',
            'tcp://localhost:61613??=False', 'tcp://localhost:61613?a=False', 'tcp://localhost:61613?maxReconnectDelay=False'
            'failover:(tcp://primary:61616, tcp://secondary:61616)', 'failover:tcp://primary:61616, tcp://secondary:61616',
            'failover:tcp://primary:61616,tcp://secondary:61616)', 'failover:(tcp://primary:61616,tcp://secondary:61616',
        ]:
            self.assertRaises(ValueError, lambda: StompFailoverUri(uri))

class StompFailoverTest(unittest.TestCase):
    def test_time_scales_and_reconnect_attempts(self):
        uri = 'failover:tcp://remote1:61615,tcp://localhost:61616,tcp://remote2:61617?randomize=false,startupMaxReconnectAttempts=3,initialReconnectDelay=7,backOffMultiplier=3.0,maxReconnectAttempts=1'
        protocol = StompFailoverTransport(uri)
        expectedDelaysAndBrokers = [
            (0, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.007, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}),
            (0.021, {'host': 'remote2', 'protocol': 'tcp', 'port': 61617}),
            (0.063, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615})
        ]
        self._test_failover(iter(protocol), expectedDelaysAndBrokers)
        
        expectedDelaysAndBrokers = [
            (0, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.007, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616})
        ]
        self._test_failover(iter(protocol), expectedDelaysAndBrokers)
        
        uri = 'failover:(tcp://remote1:61615,tcp://localhost:61616)?randomize=false,startupMaxReconnectAttempts=3,initialReconnectDelay=7,maxReconnectDelay=8,maxReconnectAttempts=0'
        protocol = StompFailoverTransport(uri)
        
        expectedDelaysAndBrokers = [
            (0, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.007, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}),
            (0.008, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.008, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616})
        ]   
        self._test_failover(iter(protocol), expectedDelaysAndBrokers)
        
        expectedDelaysAndBrokers = [
            (0, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615})
        ]   
        self._test_failover(iter(protocol), expectedDelaysAndBrokers)
        
        uri = 'failover:(tcp://remote1:61615,tcp://localhost:61616)?randomize=false,startupMaxReconnectAttempts=2,initialReconnectDelay=3,useExponentialBackOff=false'
        protocol = StompFailoverTransport(uri)
        
        expectedDelaysAndBrokers = [
            (0, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}),
            (0.003, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}),
            (0.003, {'host': 'remote1', 'protocol': 'tcp', 'port': 61615})
        ]   
        self._test_failover(iter(protocol), expectedDelaysAndBrokers)
    
    def test_priority_backup(self):
        uri = 'failover:tcp://remote1:61616,tcp://localhost:61616,tcp://127.0.0.1:61615,tcp://remote2:61616?startupMaxReconnectAttempts=3,priorityBackup=true,randomize=false'
        protocol = StompFailoverTransport(uri)
        self._test_failover(iter(protocol), [
            (0, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}),
            (0.01, {'host': '127.0.0.1', 'protocol': 'tcp', 'port': 61615}),
            (0.02, {'host': 'remote1', 'protocol': 'tcp', 'port': 61616}),
            (0.04, {'host': 'remote2', 'protocol': 'tcp', 'port': 61616})
        ])
    
    def test_randomize(self):
        uri = 'failover:tcp://remote1:61616,tcp://localhost:61616,tcp://127.0.0.1:61615,tcp://remote2:61616?priorityBackup=true,randomize=true,startupMaxReconnectAttempts=3'
        protocol = StompFailoverTransport(uri)
        localShuffled = remoteShuffled = 0
        localHosts = ['localhost', '127.0.0.1']
        remoteHosts = ['remote1', 'remote2']
        while (localShuffled * remoteShuffled) == 0:
            protocol = StompFailoverTransport(uri)
            hosts = [broker['host'] for (broker, _) in itertools.islice(protocol, 4)]
            self.assertEquals(set(hosts[:2]), set(localHosts))
            if (hosts[:2] != localHosts):
                localShuffled += 1
            self.assertEquals(set(hosts[2:]), set(remoteHosts))
            if (hosts[2:] != remoteHosts):
                remoteShuffled += 1
    
    def test_jitter(self):
        uri = 'failover:tcp://remote1:61616?useExponentialBackOff=false,startupMaxReconnectAttempts=1,reconnectDelayJitter=4'
        for j in itertools.count():
            protocol = iter(StompFailoverTransport(uri))
            protocol.next()
            _, delay = protocol.next()
            self.assertAlmostEqual(delay, 0.01, delta=0.004)
            if (j > 10) and (abs(delay - 0.01) > 0.003):
                break
    
    def _test_failover(self, brokersAndDelays, expectedDelaysAndBrokers):
        for (expectedDelay, expectedBroker) in expectedDelaysAndBrokers:
            broker, delay = brokersAndDelays.next()
            self.assertEquals(delay, expectedDelay)
            self.assertEquals(broker, expectedBroker)
            
        self.assertRaises(StompConnectTimeout, brokersAndDelays.next)
    
if __name__ == '__main__':
    unittest.main()