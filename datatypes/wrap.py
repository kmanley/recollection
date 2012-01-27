from types import ListType

IDENTITY = lambda x : x
COPY = lambda o : o._copy()
UNPACK = lambda o : o._obj

wrappedlist = None # NOTE: replaced when listtype is loaded

def _wrap_list(o):
    #newobj = wrappedlist()
    # TODO: listcomp faster?
    #for item in o:
    #    newobj._append(_wrap(item))
    #return newobj
    return wrappedlist([_wrap(item) for item in o])

WRAPPERS = {}
UNWRAPPERS = {}

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

def _unwrap_list(o):
    return [_unwrap(item) for item in o]

def _create_unwrappers():
    UNWRAPPERS.update({
        type(None) : IDENTITY,
        bool : IDENTITY,
        int : IDENTITY,
        long : IDENTITY,
        float : IDENTITY,
        str : IDENTITY,
        unicode : IDENTITY,
        
        wrappedlist : _unwrap_list,
    })
    return UNWRAPPERS


def _unwrap(o):
    typ = type(o)
    try:
        unwrapper = (UNWRAPPERS or _create_unwrappers())[typ]
    except KeyError:
        raise TypeError("internal error: unexpected type for unwrapping '%s' >%s<" % (typ, o)) # TODO: o could be big, just print part of it in that case

    return unwrapper(o)

    
    
    
    
    