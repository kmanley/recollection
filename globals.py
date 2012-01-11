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

wrappedlist = None

from types import ListType

IDENTITY = lambda x : x

def _wrap_list(o):
    newobj = wrappedlist()
    for item in o:
        newobj._append(_wrap(item))
    return newobj

WRAPPERS = {}

def _create_wrappers():
    WRAPPERS.update({
        type(None) : IDENTITY,
        bool : IDENTITY,
        int : IDENTITY,
        long : IDENTITY,
        float : IDENTITY,
        str : IDENTITY,
        unicode : IDENTITY,
        ListType  : _wrap_list,
        # if we see a wrapped type, then either the user is doing something like:
        # put('x', [1,2,3])
        # put('y', [1, 2, get('x')])
        # (which we can't allow because embedded references can't be tracked/journaled, so
        #  instead we make a copy)
        # or we are in a rollback situation
        # e.g.
        # put('x', [1,2,3])
        # put('x', [10,20,30])
        # in this case the rollback calls _put which calls _wrap and we don't really need a copy but
        # there's no harm since rollbacks should be infrequent.
        wrappedlist : lambda o : o._copy()
    })
    return WRAPPERS

def _wrap(o):
    typ = type(o)
    try:
        wrapper = (WRAPPERS or _create_wrappers())[typ]
    except KeyError:
        raise TypeError("unsupported type '%s'" % typ)

    return wrapper(o)

def _key_from_obj(self):
    try:
        key = OBJMAP[id(self)]
    except KeyError:
        raise SyntaxError("can't mutate embedded object with that syntax, try get('key', idx,...).func(...) instead")
    else:
        #print "%s -> id %s -> %s" % (self, id(self), key)
        return key
