import wrap
import globalvars
#import lib.proxy as proxy

_wrap = wrap._wrap
_key_from_obj = globalvars._key_from_obj

# rebind for performance; see ../rebind_perf/test.py for example
COMMITLIST_APPEND = globalvars.COMMITLIST_APPEND
ROLLBACKLIST_APPEND = globalvars.ROLLBACKLIST_APPEND

class NOTSET: pass

class wrappeddict(dict):
    def __delattr__(self, *args, **kwargs):
        # TODO: not sure in what context this is called (?)
        raise NotImplementedError()

    def __setattr__(self, *args, **kwargs):
        # TODO: not sure in what context this is called (?)
        raise NotImplementedError()
    
    def __delitem__(self, *args, **kwargs):
        # No point in implementing, it's the same as pop()
        raise NotImplementedError("please use dict.pop(index) instead")

    # TODO: note: virtually the same as wrappedlist.__setitem__, consider refactoring
    def __setitem__(self, idx, val):
        val = _wrap(val) 
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        prev = self.get(idx, NOTSET)
        dict.__setitem__(self, idx, val)
        if prev==NOTSET:
            # wasn't there before, so pop this value
            ROLLBACKLIST_APPEND(self.pop, idx)
        else:
            # was there before, set previous value
            ROLLBACKLIST_APPEND(self.__setitem__, idx, prev)
        COMMITLIST_APPEND(key, "SETITEM", idx, val)
        
    def clear(self):
        raise NotImplementedError("please use put(key, [idx, ...], {}) instead")

    def fromkeys(self):
        raise NotImplementedError("please use a dict comprehension instead")
        
    def pop(self, idx, default=NOTSET):
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        val = self.get(idx, NOTSET)
        if val == NOTSET:
            # d[idx] didn't exist, return default if passed otherwise keyerror
            if default != NOTSET:
                return default
            else:
                raise KeyError(idx)
        else:
            dict.pop(self, idx)
            ROLLBACKLIST_APPEND(self.__setitem__, idx, val)
            COMMITLIST_APPEND(key, "POP", idx)
            return val

    def popitem(self):
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        item = dict.popitem(self) 
        ROLLBACKLIST_APPEND(self.__setitem__, item[0], item[1])
        COMMITLIST_APPEND(key, "POP", item[0])
        return item
    
    def setdefault(self, idx, val):
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        prev = self.get(idx, NOTSET)
        if prev==NOTSET:
            return self.__setitem__(idx, val)
        else:
            return prev
                
    def update(self, other, **kwargs):
        key = _key_from_obj(self) # NOTE: must come before mutating superclass call; as this could raise an exception
        prev = self._copy() # NOTE: shallow copy
        dict.update(self, other, **kwargs)
        ROLLBACKLIST_APPEND(self._set, prev)
        COMMITLIST_APPEND(key, "PUT", None, self) # TODO: or self._copy? 
    
    def _copy(self):
        return wrappeddict(dict.copy(self)) # TODO: this is shallow copy, do we need deepcopy?
    
    def _set(self, items):
        dict.clear(self)
        dict.update(self, items)
    
wrap.wrappeddict = wrappeddict
