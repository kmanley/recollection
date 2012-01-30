import wrap
import globalvars
#import lib.proxy as proxy

_wrap = wrap._wrap
_key_from_obj = globalvars._key_from_obj

# rebind for performance; see ../rebind_perf/test.py for example
COMMITLIST_APPEND = globalvars.COMMITLIST_APPEND
ROLLBACKLIST_APPEND = globalvars.ROLLBACKLIST_APPEND

#TODO: the special methods can be called directly! So we either need to override __delitem__, etc.
#  to do journaling or raise NotImplemented

# '__add__' - ok, returns a new list
# '__class__' - ok, idempotent
# '__contains__', - ok, idempotent
# *'__delattr__', - TODO: wtf?
# *'__delitem__', - ok, overridden
# *'__delslice__', ok, overridden
# '__doc__', ok, idempotent
# '__eq__', ok, idempotent
# '__format__', ok, idempotent
# '__ge__', ok, idempotent
# '__getattribute__', ok, idempotent
# '__getitem__', ok, idempotent
# '__getslice__', ok, idempotent
# '__gt__', ok, idempotent
# '__hash__', ok, idempotent
# *'__iadd__', ok, overridden
# *'__imul__', ok, overridden
# '__init__', ok, creates a new obj
# '__iter__', ok, idempotent
# '__le__', ok, idempotent
# '__len__', ok, idempotent
# '__lt__', ok, idempotent
# '__mul__', ok, creates new obj
# '__ne__', ok, idempotent
# '__new__', ok, creates new obj
# '__reduce__', ok, used by pickle
# '__reduce_ex__', ok, used by pickle
# '__repr__', ok, idempotent
# '__reversed__', ok, idempotent
# '__rmul__',  ok, idempotent
# '__setattr__', ?
# *'__setitem__', ok, wrapped
# *'__setslice__', ok, wrapped
# '__sizeof__', ok, idempotent
# '__str__', ok, idempotent
# '__subclasshook__', ok, idempotent
# 'append', OK--wrapped
# 'count', ok, idempotent
# 'extend', OK--wrapped:
# 'index', ok, idempotent
# 'insert', OK--wrapped
# 'pop', OK--wrappped
# 'remove', TODO:
# 'reverse', OK-wrapped
# 'sort' TODO:

#reallist = list
#class wrappedlist(proxy.ListProxy):
class wrappedlist(list):
    def __delattr__(self, *args, **kwargs):
        # TODO: not sure in what context this is called (?)
        raise NotImplementedError()

    def __setattr__(self, *args, **kwargs):
        # TODO: not sure in what context this is called (?)
        raise NotImplementedError()
    
    def __delitem__(self, *args, **kwargs):
        # No point in implementing, it's the same as pop()
        raise NotImplementedError("please use list.pop(index) instead")

    def __delslice__(self, *args, **kwargs):
        # NOTE: we could implement but shorthand syntax isn't supported in eval(...) anyway
        raise NotImplementedError("please use one or more calls to list.pop(index) instead")

    def __setslice__(self, *args, **kwargs):
        # NOTE: we could implement but shorthand syntax isn't supported in eval(...) anyway
        raise NotImplementedError("please use set(key, idx, ..., obj) instead")

    def __iadd__(self, *args, **kwargs):
        # No point in implementing as this is the same as extend(...)
        raise NotImplementedError("please use list.extend(obj) or put(key, get(key) + obj) instead")

    def __imul__(self, *args, **kwargs):
        # NOTE: we could implement but shorthand syntax isn't supported in eval(...) anyway
        raise NotImplementedError("please use put(key, get(key) * other) instead")

    def __setitem__(self, index, val):
        val = _wrap(val) # TODO: I assume the call to global function is cheaper than call to mixin
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        prev = self[index]
        list.__setitem__(self, index, val)
        ROLLBACKLIST_APPEND(self.__setitem__, index, prev)
        COMMITLIST_APPEND(key, "SETITEM", index, val)

    def append(self, val):
        val = _wrap(val)
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        list.append(self, val)
        # NOTE: the idea here is that we want to avoid making a deep copy of the before value when possible.
        # The rollback function should be the cheapest way possible to restore the data back to its previous state.
        # Only make a copy of the previous value if there is no other way to achieve the same effect. It's not so
        # much that we want rollbacks to be fast, it's that we want the happy path to be fast, and deep copies are
        # expensive.
        ROLLBACKLIST_APPEND(self.pop)
        # NOTE: this is APPENDR because we will also need APPENDL, e.g. for deque
        COMMITLIST_APPEND(key, "APPENDR", None, val)

    def extend(self, val):
        val = _wrap(val)
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        prev_len = len(self)
        list.extend(self, val)
        val_len = len(self) - prev_len # we do it this way because val could be an iterator
        ROLLBACKLIST_APPEND(lambda self : [self.pop() for i in range(val_len)], self)
        # NOTE: this is EXTENDR because we will also need EXTENDL, e.g. for deque
        COMMITLIST_APPEND(key, "EXTENDR", None, val)

    def insert(self, index, val):
        val = _wrap(val)
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        list.insert(self, index, val)
        ROLLBACKLIST_APPEND(self.pop, index)
        COMMITLIST_APPEND(key, "INSERT", index, val)

    def pop(self, index=None):
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        if index is None:
            index = len(self) - 1
        val = list.pop(self, index) # TODO: what if this is a ref; do we need to do a copy? or deepcopy?
        ROLLBACKLIST_APPEND(self.insert, index, val)
        # NOTE: this POP pops an arbitrary index, we will also need POPR, POPL with no index for deque
        COMMITLIST_APPEND(key, "POP", index)
        return val

    def reverse(self):
        """
        Reverses the items of the list in place

        Algorithmic complexity: **O(n)** where n is the size of the list

        Journal output: <TXID>:<KEYPATH>:**REVERSE**\:\:

        Example::

            put('x', [1, 2, 3])
            get('x').reverse()
            get('x') # returns [3, 2, 1]
        """
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        list.reverse(self)
        ROLLBACKLIST_APPEND(self.reverse)
        COMMITLIST_APPEND(key, "REVERSE")

    def sort(self, *args, **kwargs):
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        prev = self._copy() # NOTE: shallow copy
        list.sort(self, *args, **kwargs)
        ROLLBACKLIST_APPEND(self._set, prev)
        # NOTE: we use PUT here instead of journaling SORT to avoid the problem of serializing the params to sort, e.g.
        # cmp, key, reverse...
        COMMITLIST_APPEND(key, "PUT", None, self) # TODO: or self._copy?

    def _copy(self):
        return wrappedlist(self[::]) # TODO: this is shallow copy, do we need deepcopy?

    def _set(self, val):
        list.__setslice__(self, 0, len(self), val)

    #def _append(self, val):
    #    list.append(self, val)
        

#wrappedlist = list
#list = reallist
#list = wrappedlist

#import globals
wrap.wrappedlist = wrappedlist