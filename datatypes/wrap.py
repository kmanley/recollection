from types import ListType

IDENTITY = lambda x : x
COPY = lambda o : o._copy()

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
        wrappedlist : COPY,
        })
    return WRAPPERS

def _wrap(o):
    typ = type(o)
    try:
        wrapper = (WRAPPERS or _create_wrappers())[typ]
    except KeyError:
        raise TypeError("unsupported type '%s'" % typ)

    return wrapper(o)

