#reallist = list

import globals
from globals import _wrap, _key_from_obj, serialize

#TXID, COMMITLIST, ROLLBACKLIST,
#COMMITLIST_APPEND = COMMITLIST.append
#ROLLBACKLIST_APPEND = ROLLBACKLIST.append
#_wrap = None
#_key_from_obj = None
#COMMITLIST = None
#ROLLBACKLIST = None

class wrappedlist(list):
    #TODO: the special methods can be called directly! So we either need to override __delitem__, etc.
    #  to do journaling or raise NotImplemented
    """
     '__add__' - ok, returns a new list
     '__class__' - ok, idempotent
     '__contains__', - ok, idempotent
     *'__delattr__', - TODO: wtf?
     *'__delitem__', - ok, overridden
     *'__delslice__', ok, overridden
     '__doc__', ok, idempotent
     '__eq__', ok, idempotent
     '__format__', ok, idempotent
     '__ge__', ok, idempotent
     '__getattribute__', ok, idempotent
     '__getitem__', ok, idempotent
     '__getslice__', ok, idempotent
     '__gt__', ok, idempotent
     '__hash__', ok, idempotent
     *'__iadd__', ok, overridden
     *'__imul__', ok, overridden
     '__init__', ok, creates a new obj
     '__iter__', ok, idempotent
     '__le__', ok, idempotent
     '__len__', ok, idempotent
     '__lt__', ok, idempotent
     '__mul__', ok, creates new obj
     '__ne__', ok, idempotent
     '__new__', ok, creates new obj
     '__reduce__', ok, used by pickle
     '__reduce_ex__', ok, used by pickle
     '__repr__', ok, idempotent
     '__reversed__', ok, idempotent
     '__rmul__',  ok, idempotent
     '__setattr__', ?
     *'__setitem__', ok, wrapped
     *'__setslice__', ok, wrapped
     '__sizeof__', ok, idempotent
     '__str__', ok, idempotent
     '__subclasshook__', ok, idempotent
     'append', OK--wrapped
     'count', ok, idempotent
     'extend', OK--wrapped:
     'index', ok, idempotent
     'insert', OK--wrapped
     'pop', OK--wrappped
     'remove', TODO:
     'reverse', OK-wrapped
     'sort' TODO:
    """
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

    # TODO: consider implementing this so that e.g. random.shuffle(get('x')) works...
    def __setitem__(self, index, val):
        val = _wrap(val) # TODO: I assume the call to global function is cheaper than call to mixn
        key = _key_from_obj(self)
        prev = self[index]
        list.__setitem__(self, index, val)
        ROLLBACKLIST_APPEND((self.__setitem__, (index, prev)))
        globals.COMMITLIST.append((globals.TXID, key, "SETITEM", index, serialize(val)))

    def append(self, val):
        val = _wrap(val)
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        list.append(self, val)
        # NOTE: the idea here is that we want to avoid making a deep copy of the before value when possible.
        # The rollback function should be the cheapest way possible to restore the data back to its previous state.
        # Only make a copy of the previous value if there is no other way to achieve the same effect. It's not so
        # much that we want rollbacks to be fast, it's that we want the happy path to be fast, and deep copies are
        # expensive.
        ROLLBACKLIST_APPEND((self.pop, ()))
        # NOTE: this is APPENDR because we will also need APPENDL, e.g. for deque
        COMMITLIST_APPEND((globals.TXID, key, "APPENDR", "", serialize(val))) # TODO: get rid of all dotted qualifiers by rebinding

    def extend(self, val):
        val = _wrap(val)
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        prev_len = len(self)
        list.extend(self, val)
        val_len = len(self) - prev_len # we do it this way because val could be an iterator
        ROLLBACKLIST_APPEND((lambda self : [self.pop() for i in range(val_len)], (self,)))
        # NOTE: this is EXTENDR because we will also need EXTENDL, e.g. for deque
        COMMITLIST_APPEND((globals.TXID, key, "EXTENDR", "", serialize(val)))

    def insert(self, index, val):
        val = _wrap(val)
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        list.insert(self, index, val)
        ROLLBACKLIST_APPEND((self.pop, (index,)))
        COMMITLIST_APPEND((globals.TXID, key, "INSERT", index, serialize(val)))

    def pop(self, index=None):
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        if index is None:
            index = len(self) - 1
        val = list.pop(self, index) # TODO: what if this is a ref; do we need to do a copy? or deepcopy?
        ROLLBACKLIST_APPEND((self.insert, (index, val)))
        # NOTE: this POP pops an arbitrary index, we will also need POPR, POPL with no index for deque
        COMMITLIST_APPEND((globals.TXID, key, "POP", index, ""))
        return val

    def reverse(self):
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        list.reverse(self)
        ROLLBACKLIST_APPEND((self.reverse, ()))
        COMMITLIST_APPEND((globals.TXID, key, "REVERSE", "", ""))

    def sort(self):
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        prev = self[::] # NOTE: shallow copy
        list.sort(self)
        ROLLBACKLIST_APPEND((self._set, (prev,)))
        COMMITLIST_APPEND((globals.TXID, key, "SORT", "", ""))

    def _copy(self):
        return self[::] # TODO: this is shallow copy, do we need deepcopy?

    def _set(self, val):
        list.__setslice__(self, 0, len(self), val)

    def _append(self, val):
        list.append(self, val)

#list = wrappedlist

import globals
globals.wrappedlist = wrappedlist