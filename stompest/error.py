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
class StompError(Exception):
    """Base class for STOMP errors
    """

class StompFrameError(StompError):
    """Raised for error parsing STOMP frames
    """

class StompProtocolError(StompError):
    """Raised for STOMP protocol errors
    """
    
class StompConnectionError(StompError):
    """Raised for nonexistent connection
    """

class StompConnectTimeout(StompConnectionError):
    """Raised for timeout waiting for connect response from broker
    """
