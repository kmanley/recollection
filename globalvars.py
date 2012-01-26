import binascii
import cPickle as pickle
_hexlify = binascii.hexlify
_unhexlify = binascii.unhexlify
_pickle_dumps = pickle.dumps
_pickle_loads = pickle.loads
def serialize(o):
    return _hexlify(_pickle_dumps(o, protocol=-1))
def deserialize(s):
    return _pickle_loads(_unhexlify(s))

SERVER = None
OBJMAP = {}
COMMITLIST = []
ROLLBACKLIST = []
_TXID = [0]

def SET_TXID(n):
    _TXID[0] = n

def GET_TXID():
    return _TXID[0]

def INCR_TXID():
    _TXID[0] += 1

class NOTSPECIFIED: pass

_COMMITLIST_APPEND = COMMITLIST.append
def COMMITLIST_APPEND(key, command, idx=NOTSPECIFIED, value=NOTSPECIFIED):
    _COMMITLIST_APPEND((_TXID[0], key, command, "" if idx==NOTSPECIFIED else idx, "" if value==NOTSPECIFIED else value))

_ROLLBACKLIST_APPEND = ROLLBACKLIST.append
def ROLLBACKLIST_APPEND(func, *args):
    _ROLLBACKLIST_APPEND((func, args))

def _key_from_obj(self):
    try:
        return OBJMAP[id(self)]
    except KeyError:
        raise SyntaxError("can't mutate embedded object with that syntax, try get('key', idx,...).func(...) instead")

#wrappedlist = None

