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