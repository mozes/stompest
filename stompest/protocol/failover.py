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
import collections
import random
import re
import socket

from stompest.error import StompConnectTimeout

class StompFailoverProtocol(object):
    def __init__(self, uri):
        self._config = StompConfiguration(uri)
        self._maxReconnectAttempts = None
    
    def __iter__(self):
        self._reset()
        while True:
            for broker in self._brokers():
                yield broker, self._delay()
    
    def _brokers(self):
        config = self._config
        options = config.options
        brokers = list(config.brokers)
        if options['randomize']:
            random.shuffle(brokers)
        if options['priorityBackup']:
            brokers.sort(key=lambda b: b['host'] in config.LOCAL_HOST_NAMES, reverse=True)
        return brokers
    
    def _delay(self):
        options = self._config.options
        self._reconnectAttempts += 1
        if self._reconnectAttempts == 0:
            return 0
        if (self._maxReconnectAttempts != -1) and (self._reconnectAttempts > self._maxReconnectAttempts):
            raise StompConnectTimeout('Reconnect timeout: %d attempts'  % self._maxReconnectAttempts)
        delay = max(0, (min(self._reconnectDelay + (random.random() * options['reconnectDelayJitter']), options['maxReconnectDelay'])))
        self._reconnectDelay *= (options['backOffMultiplier'] if options['useExponentialBackOff'] else 1)
        return delay / 1000.0

    def _reset(self):
        options = self._config.options
        self._reconnectDelay = options['initialReconnectDelay']
        if self._maxReconnectAttempts is None:
            self._maxReconnectAttempts = options['startupMaxReconnectAttempts']
        else:
            self._maxReconnectAttempts = options['maxReconnectAttempts']
        self._reconnectAttempts = -1
        
class StompConfiguration(object):
    LOCAL_HOST_NAMES = set([
        'localhost',
        '127.0.0.1',
        socket.gethostbyname(socket.gethostname()),
        socket.gethostname(),
        socket.getfqdn(socket.gethostname())
    ])
    
    _configurationOption = collections.namedtuple('_configurationOption', ['parser', 'default'])
    _bool = {'true': True, 'false': False}.__getitem__
    
    _FAILOVER_PREFIX = 'failover:'
    _REGEX_URI = re.compile('^(?P<protocol>tcp)://(?P<host>[^:]+):(?P<port>\d+)$')
    _REGEX_BRACKETS =  re.compile('^\((?P<uri>.+)\)$')
    _SUPPORTED_OPTIONS = { # cf. http://activemq.apache.org/failover-transport-reference.html
        'initialReconnectDelay': _configurationOption(int, 10) # how long to wait before the first reconnect attempt (in ms)
        , 'maxReconnectDelay': _configurationOption(int, 30000) # the maximum amount of time we ever wait between reconnect attempts (in ms)
        , 'useExponentialBackOff': _configurationOption(_bool, True) # should an exponential backoff be used between reconnect attempts
        , 'backOffMultiplier': _configurationOption(float, 2.0) # the exponent used in the exponential backoff attempts
        , 'maxReconnectAttempts': _configurationOption(int, -1) # -1 is default and means retry forever, 0 means don't retry (only try connection once but no retry), >0 means the maximum number of reconnect attempts before an error is sent back to the client
        , 'startupMaxReconnectAttempts': _configurationOption(int, 0) # if not 0, then this is the maximum number of reconnect attempts before an error is sent back to the client on the first attempt by the client to start a connection, once connected the maxReconnectAttempts option takes precedence
        , 'reconnectDelayJitter': _configurationOption(int, 0) # jitter in ms by which reconnect delay is blurred in order to avoid stampeding
        , 'randomize': _configurationOption(_bool, True) # use a random algorithm to choose the the URI to use for reconnect from the list provided
        , 'priorityBackup': _configurationOption(_bool, False) # if set, prefer local connections to remote connections
        #, 'backup': _configurationOption(_bool, False), # initialize and hold a second transport connection - to enable fast failover
        #, 'timeout': _configurationOption(int, -1), # enables timeout on send operations (in miliseconds) without interruption of reconnection process
        #, 'trackMessages': _configurationOption(_bool, False), # keep a cache of in-flight messages that will flushed to a broker on reconnect
        #, 'maxCacheSize': _configurationOption(int, 131072), # size in bytes for the cache, if trackMessages is enabled
        #, 'updateURIsSupported': _configurationOption(_bool, True), # determines whether the client should accept updates to its list of known URIs from the connected broker
    }
    
    def __init__(self, uri, login='', passcode=''):
        self._parse(uri)
        self.login = login
        self.passcode = passcode
    
    def _parse(self, uri):
        self.uri = uri
        try:
            (uri, _, options) = uri.partition('?')
            if uri.startswith(self._FAILOVER_PREFIX):
                (_, _, uri) = uri.partition(self._FAILOVER_PREFIX)
            
            try:
                self._setOptions(options)
            except Exception, msg:
                raise ValueError('invalid options: %s' % msg)
            
            try:
                self._setBrokers(uri)
            except Exception, msg:
                raise ValueError('invalid broker(s): %s' % msg)
            
        except ValueError, msg:
            raise ValueError('invalid uri: %s [%s]' % (self.uri, msg))
    
    def _setBrokers(self, uri):
        brackets = self._REGEX_BRACKETS.match(uri)
        uri = brackets.groupdict()['uri'] if brackets else uri
        brokers = [self._REGEX_URI.match(u).groupdict() for u in uri.split(',')]
        for broker in brokers:
            broker['port'] = int(broker['port'])
        self.brokers = brokers

    def _setOptions(self, options=None):
        _options = dict((k, o.default) for (k, o) in self._SUPPORTED_OPTIONS.iteritems())
        if options:
            _options.update((k, self._SUPPORTED_OPTIONS[k].parser(v)) for (k, _, v) in (o.partition('=') for o in options.split(',')))
        self.options = _options

