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
class StompConfig(object):
    """This is a container for those configuration options which are common to both clients (sync and async) and are needed to establish a STOMP connection. All parameters are available as attributes with the same name of this object.

    :param uri: A failover URI as it is accepted by :class:`~.StompFailoverUri`.
    :param login: The login for the STOMP brokers.
    :param passcode: The passcode for the STOMP brokers.
    :param version: A valid STOMP protocol version, or :obj:`None` (equivalent to the :attr:`DEFAULT_VERSION` attribute of the :class:`~.StompSpec` class).
    :param check: Decides whether the :class:`~.StompSession` object which is used to represent the STOMP sesion should be strict about the session's state: (e.g., whether to allow calling the session's :meth:`~.StompSession.send` when disconnected).
    
    .. note :: Login and passcode have to be the same for all brokers because they are not part of the failover URI scheme.
    
    .. seealso :: The :class:`~.StompFailoverTransport` class which tells you which broker to use and how long you should wait to connect to it, the :class:`~.StompFailoverUri` which parses failover transport URIs.
    """
    def __init__(self, uri, login='', passcode='', version=None, check=True):
        self.uri = uri
        self.login = login
        self.passcode = passcode
        self.version = version
        self.check = check
