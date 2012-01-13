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
TXID = 0

def _key_from_obj(self):
    try:
        return OBJMAP[id(self)]
    except KeyError:
        raise SyntaxError("can't mutate embedded object with that syntax, try get('key', idx,...).func(...) instead")

#wrappedlist = None

