"""
"""
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

class StompConfig(object):
    """This configuration object contains those parameters which are common to both clients (sync and async) and are needed to establish a STOMP connection.

    :param uri: A failover URI as it is accepted by :class:`StompFailoverUri`.
    :param login: The login for the STOMP brokers.
    :param passcode: The passcode for the STOMP brokers.
    :param version: The highest STOMP protocol version this client may use. If :obj:`None`, **version** defaults to :attr:`StompSpec.DEFAULT_VERSION` (currently :obj:`'1.0'`, but this may change in upcoming stompest releases) 
    :param check: Decides whether the :class:`StompSession` object which is used to represent the STOMP sesion should be strict about the session's state: (e.g., whether to allow calling :meth:`StompSession.send` when disconnected).
    
    .. note :: Login and passcode have to be the same for all brokers becuse they are not part of the failover URI scheme.
    
    .. seealso :: :class:`StompFailoverProtocol`, :class:`StompFailoverUri`
    """
    def __init__(self, uri, login='', passcode='', version=None, check=True):
        self.uri = uri
        self.login = login
        self.passcode = passcode
        self.version = version
        self.check = check

class StompFailoverProtocol(object):
    """This object is a parser for the failover URI scheme used in stompest. Looping over this object, you can produce a series of tuples (broker, delay in s). When the URI does not allow further failover, a :class:`StompConnectTimeout` error is raised.
    
    :param uri: A failover URI.
    
    **Example:**
    
    >>> from stompest.protocol import StompFailoverProtocol
    >>> from stompest.error import StompConnectTimeout
    >>> failover = StompFailoverProtocol('failover:(tcp://remote1:61615,tcp://localhost:61616)?randomize=false,startupMaxReconnectAttempts=3,initialReconnectDelay=7,maxReconnectDelay=8,maxReconnectAttempts=0')
    >>> try:
    ...     for (broker, delay) in failover:
    ...         print 'broker: %s, delay: %f' % (broker, delay)                                                                       
    ... except StompConnectTimeout as e:
    ...     print 'timeout: %s' % e
    ... 
    broker: {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}, delay: 0.000000
    broker: {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}, delay: 0.007000
    broker: {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}, delay: 0.008000
    broker: {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}, delay: 0.008000
    timeout: Reconnect timeout: 3 attempts
    >>> try:
    ...     for (broker, delay) in failover:
    ...         print 'broker: %s, delay: %f' % (broker, delay)
    ... except StompConnectTimeout as e:
    ...     print 'timeout: %s' % e
    ... 
    broker: {'host': 'remote1', 'protocol': 'tcp', 'port': 61615}, delay: 0.000000
    timeout: Reconnect timeout: 0 attempts
    
    .. seealso :: :class:`StompFailoverUri`
    """
    def __init__(self, uri):
        self._failoverUri = StompFailoverUri(uri)
        self._maxReconnectAttempts = None
    
    def __iter__(self):
        self._reset()
        while True:
            for broker in self._brokers():
                yield broker, self._delay()
    
    def _brokers(self):
        failoverUri = self._failoverUri
        options = failoverUri.options
        brokers = list(failoverUri.brokers)
        if options['randomize']:
            random.shuffle(brokers)
        if options['priorityBackup']:
            brokers.sort(key=lambda b: b['host'] in failoverUri.LOCAL_HOST_NAMES, reverse=True)
        return brokers
    
    def _delay(self):
        options = self._failoverUri.options
        self._reconnectAttempts += 1
        if self._reconnectAttempts == 0:
            return 0
        if (self._maxReconnectAttempts != -1) and (self._reconnectAttempts > self._maxReconnectAttempts):
            raise StompConnectTimeout('Reconnect timeout: %d attempts'  % self._maxReconnectAttempts)
        delay = max(0, (min(self._reconnectDelay + (random.random() * options['reconnectDelayJitter']), options['maxReconnectDelay'])))
        self._reconnectDelay *= (options['backOffMultiplier'] if options['useExponentialBackOff'] else 1)
        return delay / 1000.0

    def _reset(self):
        options = self._failoverUri.options
        self._reconnectDelay = options['initialReconnectDelay']
        if self._maxReconnectAttempts is None:
            self._maxReconnectAttempts = options['startupMaxReconnectAttempts']
        else:
            self._maxReconnectAttempts = options['maxReconnectAttempts']
        self._reconnectAttempts = -1
        
class StompFailoverUri(object):
    """This object is a parser for the failover URI scheme used in stompest. The parsed parameters are available in the attributes :attr:`brokers` and :attr:`options`. The Failover transport syntax is very close to the one used in ActiveMQ. Its basic form is::
    
    'failover:(uri1,...,uriN)?transportOptions'
    
    or::
    
    'failover:uri1,...,uriN'
    
    :param uri: A failover URI.
    
    **Example:**
    
    >>> from stompest.protocol import StompFailoverUri    
    >>> uri = StompFailoverUri('failover:(tcp://remote1:61615,tcp://localhost:61616)?randomize=false,startupMaxReconnectAttempts=3,initialReconnectDelay=7,maxReconnectDelay=8,maxReconnectAttempts=0')
    >>> print uri.brokers
    [{'host': 'remote1', 'protocol': 'tcp', 'port': 61615}, {'host': 'localhost', 'protocol': 'tcp', 'port': 61616}]
    >>> print uri.options
    {'initialReconnectDelay': 7, 'maxReconnectDelay': 8, 'backOffMultiplier': 2.0, 'startupMaxReconnectAttempts': 3, 'priorityBackup': False, 'maxReconnectAttempts': 0, 'reconnectDelayJitter': 0, 'useExponentialBackOff': True, 'randomize': False}
    
    **Supported Options:**
    
    =============================  ========= ============= ================================================================
    option                         type      default       description
    =============================  ========= ============= ================================================================
    *initialReconnectDelay*        int       :obj:`10`     how long to wait before the first reconnect attempt (in ms)
    *maxReconnectDelay*            int       :obj:`30000`  the maximum amount of time we ever wait between reconnect attempts (in ms)
    *useExponentialBackOff*        bool      :obj:`True`   should an exponential backoff be used between reconnect attempts
    *backOffMultiplier*            float     :obj:`2.0`    the exponent used in the exponential backoff attempts
    *maxReconnectAttempts*         int       :obj:`-1`     :obj:`-1` means retry forever
                                                           :obj:`0` means don't retry (only try connection once but no retry)
                                                           :obj:`> 0` means the maximum number of reconnect attempts before an error is sent back to the client
    *startupMaxReconnectAttempts*  int       :obj:`0`      if not :obj:`0`, then this is the maximum number of reconnect attempts before an error is sent back to the client on the first attempt by the client to start a connection, once connected the *maxReconnectAttempts* option takes precedence
    *reconnectDelayJitter*         int       :obj:`0`      jitter in ms by which reconnect delay is blurred in order to avoid stampeding
    *randomize*                    bool      :obj:`True`   use a random algorithm to choose the the URI to use for reconnect from the list provided
    *priorityBackup*               bool      :obj:`False`  if set, prefer local connections to remote connections
    =============================  ========= ============= ================================================================
    
    .. seealso :: :class:`StompFailoverProtocol`, `failover transport <http://activemq.apache.org/failover-transport-reference.html>`_ of ActiveMQ.
    """
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
    _SUPPORTED_OPTIONS = {
        'initialReconnectDelay': _configurationOption(int, 10)
        , 'maxReconnectDelay': _configurationOption(int, 30000)
        , 'useExponentialBackOff': _configurationOption(_bool, True)
        , 'backOffMultiplier': _configurationOption(float, 2.0)
        , 'maxReconnectAttempts': _configurationOption(int, -1)
        , 'startupMaxReconnectAttempts': _configurationOption(int, 0)
        , 'reconnectDelayJitter': _configurationOption(int, 0)
        , 'randomize': _configurationOption(_bool, True)
        , 'priorityBackup': _configurationOption(_bool, False)
        #, 'backup': _configurationOption(_bool, False), # initialize and hold a second transport connection - to enable fast failover
        #, 'timeout': _configurationOption(int, -1), # enables timeout on send operations (in miliseconds) without interruption of reconnection process
        #, 'trackMessages': _configurationOption(_bool, False), # keep a cache of in-flight messages that will flushed to a broker on reconnect
        #, 'maxCacheSize': _configurationOption(int, 131072), # size in bytes for the cache, if trackMessages is enabled
        #, 'updateURIsSupported': _configurationOption(_bool, True), # determines whether the client should accept updates to its list of known URIs from the connected broker
    }
    
    def __init__(self, uri):
        self._parse(uri)
    
    def __repr__(self):
        return "StompFailoverUri('%s')" % self.uri
    
    def __str__(self):
        return self.uri
    
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
