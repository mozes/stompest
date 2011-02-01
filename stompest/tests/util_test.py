"""
Copyright 2011 Mozes, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import unittest
from stompest.util import filterReservedHeaders

class UtilTest(unittest.TestCase):
    
    def test_filterReservedHeaders(self):
        origHdrs = {'message-id': 'delete me', 'timestamp': 'delete me too', 'foo': 'bar'}
        filteredHdrs = filterReservedHeaders(origHdrs)
        self.assertTrue('message-id' in origHdrs)
        self.assertTrue('foo' in origHdrs)
        self.assertFalse('message-id' in filteredHdrs)
        self.assertFalse('timestamp' in filteredHdrs)
        self.assertTrue('foo' in filteredHdrs)
        
if __name__ == '__main__':
    unittest.main()