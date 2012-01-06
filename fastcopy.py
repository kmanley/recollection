"""
Conclusion: marshal is fastest, cpickle is about as fast as ujson as long as you use protocol=-1
Not everything can be marshalled

"""
import sys
import time
import copy
import cPickle as pickle
import marshal
from bitarray import bitarray
from blist import blist, sortedlist, sorteddict, sortedset
from collections import deque
import numpy
from numpy import array, matrix

deepcopy = copy.deepcopy
marshal_loads = marshal.loads
marshal_dumps = marshal.dumps
pickle_loads = pickle.loads
pickle_dumps = pickle.dumps
numpy_fromstring = numpy.fromstring

def identity(o):
    return o

def dopickle(o):
    return pickle_loads(pickle_dumps(o, protocol=-1))

def trymarshal(o):
    try:
        return marshal_loads(marshal_dumps(o))
    except:
        # marshal can fail if there is a cycle
        return pickle_loads(pickle_dumps(o, protocol=-1))

COPYFUNCS = {str : identity,
             unicode : identity,
             int : identity,
             long: identity,
             float: identity,
             tuple : identity,
             set: trymarshal,
             list: trymarshal, # NOT o[::], as that is a shallow copy, only deepcopy for strings
             dict: trymarshal,
             # numpy types
             array : deepcopy,
             matrix : deepcopy,
             # other 3rd party types
             bitarray : deepcopy,
             #blist : lambda o : o[::], NO: that's a shallow copy--use pickle
             #sortedlist : lambda o : o[::], NO: that's a shallow copy--use pickle
             # TODO: igraph, pandas
             }

def fast_copy(o):
    return COPYFUNCS.get(type(o), dopickle)(o)

