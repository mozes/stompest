"""Util functions
"""
from copy import deepcopy

reservedHeaders = ['message-id', 'timestamp', 'expires', 'priority', 'destination']

def filterReservedHeaders(headers):
    filtered = headers.copy()
    for hdr in reservedHeaders:
        if hdr in filtered:
            del filtered[hdr]
    return filtered
    
def cloneStompMessageForErrorDest(msg):
    errMsg = deepcopy(msg)
    errMsg['headers'] = filterReservedHeaders(msg['headers'])
    errMsg['headers']['persistent'] = 'true'
    return errMsg

    