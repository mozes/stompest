"""
Copyright 2011 Mozes, Inc.

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
from stompest.session import StompConfiguration

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

if __name__ == '__main__':
    unittest.main()