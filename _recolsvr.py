"""
TODO: disallow storing embedded references, e.g.
  put('x', [1,2,3])
  put('y', [100, 200, get('x')])
instead give a message to use copy(...)

TODO: think about. If you have a class foobar that is supported and you have a bunch of these pickled in the
sqlite db, then you change the name of the class, then try to load from db, you will get errors. Maybe have another
level of indirection or directly store class name and only serialize the underlying Python type, e.g. instead of
pickling wrappedlist store "wrappedlist" and pickle just a pure list.

TODO: should we offer an alternative persistence mode like REDIS, where we just serialize the whole DB every N secs?
TODO: offer a no-persistence mode where we don't even write to the journal.

TODO: bug
> put((1,2,3),'foo')
trying to use non-string key fails in db writer

TODO: bug
 (put(str(x),x*2) for x in range(1))
generator expression results in weird result

TODO: this fails
> put('x', [1,2,3, [10,20]])
{u'cps': 5422.00637, u'res': None}
>
> get('x')
{u'cps': 8851.51582, u'res': [1, 2, 3, [10, 20]]}
>
> get('x',3).append(30)
{u'cps': 9584.21617, u'res': None}
> get('x')
{u'cps': 8107.55247, u'res': [1, 2, 3, [10, 20, 30]]}

because the embedded list isn't a wrappedlist...it's like we need to walk the tree of objects and convert
everything, what a ballache...I really just want to be able to replace the metaclass of built in types...


# TODO: efficient way to copy datetimes
# TODO: integrate see? https://github.com/inky/see


TODO: how to handle this...

> put('x', [1,2,3])
None
> put('l1', [100,200,get('x')])  # embeds a reference to x
None
> put('l2', [100,200,get('x')])  # ditto
None
> get('l1')
[100, 200, [1, 2, 3]]   # shows embedded x
> get('l2')
[100, 200, [1, 2, 3]]   # ditto
> get('x').append(4)    # now, we change x
None
> get('l1')
[100, 200, [1, 2, 3, 4]]  # and affect both l1 and l2, but there's no way for this to appear in journal
> get('l2')
[100, 200, [1, 2, 3, 4]]
>
THIS SEEMS TO ARGUE FOR REQUIRING get(...) to return a deep copy
But that kills performance....can we somehow track the dependencies? If not, warn people not to do this because
it's stupid.



TODO: how to make journaling operations efficient, e.g.

in memory
key:    [1,2,3,4,5]

if we store this in db as
key:    <pickled [1,2,3,4,5]>
then we have no choice but to resave the entire key value, this could be expensive if value is 1M+ elements

if we store like this in db
key  index  value
key   0     1
key   1     2
key   2     3
key   3     4
key   4     5
then pop from beginning involves
   update table set index=index-1 where key=key
   delete from table where index=last
but this could involved 1 million rows in table for just this one key

Pop from end of list can simply be journaled as DEL key[-1]
Pop from begin












TODO:
  - keep track of changes in a transaction and apply them all at once using dict.update
  - if any deletes happen, mark them as deleted and clean them up in something similar to expiry handler
  - ssync, don't return until journal entry has been written
  - hsync, don't return until journal entry has been written and fsync'd
  - batch journal writes
  - journal reader which applies updates to sqlite db
  - delay server startup until all journal updates have been applied to sqlite db
  - rebuild in memory db from sqlite
  - consider capped collections, e.g. cappedlist(size) object?

3 Jan 12
Something weird happening with testing on my new box.
Initially high req/sec then trails off, even when I stub out everything and just return a hardcoded string.
This happens with both 32 and 64 bit builds
Is this a problem in 0MQ lib?
*** NO: turns out the problem is due to including pythoncom ***

C:\data\personal\code\recollection>c:\python27x32\python.exe recolsvr.py
INFO:root:pid: 5804
INFO:root:journal writer thread starting...
0.00 req/sec
18158.73 req/sec
18485.86 req/sec
18292.66 req/sec
1575.89 req/sec
1471.46 req/sec
1542.16 req/sec
1734.49 req/sec
"""


# TODO: have a look at other C-based JSON libs on python.org, e.g. YAJL, see which ones have python bindings
#  I know YAJL does
# TODO: try implementing in Cython and see if that's any faster
# TODO: master-slave, see here: http://zguide.zeromq.org/page:all#header-100 the section "getting a snapshot"
"""
create table master (key text not null,
                     i0 text null,
                     i1 text null,
                     i2 text null,
                     i3 text null,
                     i4 text null,
                     i5 text null,
                     i6 text null,
                     i7 text null,
                     i8 text null,
                     value text,
                     primary key (key, i0, i1, i2, i3, i4, i5, i6, i7, i8));
CREATE TABLE lastwrite (txid int not null, seekpos int not null);
insert into last values (0,0);

Notes: 
21 Dec - using a sqlite journal is way too slow; queue keeps growing
  Switching to disk based journal the queue only lagged behind around 300 rows even with 8K+ requests/sec
  Tried 5 clients each doing 100K sets, server was only using 40% CPU though CPU was pegged by the clients, got about 8K req/sec
  
  Tried by running around 100 perf clients on Adrian's machine and actually saw nearly 20K req/sec on the server, and the
  server still only used about 90% cpu (I could still easily use the repl). but then I started seeing the journal queue backup, e.g. 
    WARNING:root:db queue size: 1420774
    19065.87 requests/sec
    18548.88 requests/sec
    18362.87 requests/sec
    19696.77 requests/sec
    18407.58 requests/sec
    WARNING:root:db queue size: 1475175
    19425.42 requests/sec
    19306.29 requests/sec
    18244.90 requests/sec
    19038.45 requests/sec
    19520.94 requests/sec
    WARNING:root:db queue size: 1533165
    18360.65 requests/sec  
 TODO: need to write bigger blocks of data, not a single line at a time.
  
"""

"""
Philosophy-we are just providing access to known/standard Python types (and some 3rd party extensions), but we add:
 - concurrent access by multiple clients
 - serializaton of requests
 - configurable persistence/durability

Requirements for using external data structures:
1. must be pickleable
2. must be deep-copyable 

3rd party data structures
blist.sortedset      OK
bitarray             OK
numpy.array          OK
numpy.matrix         OK
Bio.trie             NO (not deep copyable)


REDIS equivalents
------------------------------------------------------------------------------------------------------------------------
APPEND key value                            get('key').append(value)
AUTH
BGREWRITEAOF
BGSAVE
BLPOP
BRPOP
BRPOPLPUSH
CONFIG GET
CONFIG RESETSTAT
DBSIZE                                      numkeys()
DEBUG OBJECT obj
DEBUG SEGFAULT
DECR key                                    decr('key'[,indexes])
DECRBY key decrement                        decr('key'[,indexes], by=decrement)
DEL key                                     erase('key')
DISCARD
ECHO msg                                    echo('msg')
EXEC
EXISTS key                                  exists('key')
EXPIRE key seconds                          expire('key', when) # when can be a datetime.datetime or time_t
EXPIREAT key timestamp                      expire('key', when) # when can be a datetime.datetime or time_t
FLUSHALL
FLUSHDB
GET key                                     get('key')
GETBIT key offset                           get('key')[offset]  #Note: works for any sequence type
GETRANGE key start end                      get('key')[start:end] # Note: stride also works
GETSET key value                            get('key'), put('key', newvalue)
HDEL key field                              *get('key').pop(field)
HEXISTS key field                           get('key').has_key(field)
HGET key field                              get('key')[field]
HGETALL key                                 get('key') # returns JSON
HINCRBY key field increment                 incr('key', 'field', by=increment)
HKEYS key                                   get('key').keys()
HLEN key                                    length('key')
HMGET key field [field...]                  get('key')[field], get('key')[field]
HMSET key field value [field value]         put('key', field, value), put('key', field2, value2), ...
                                            or
                                            *get('key').update({"field":value, "field":value})
HSET key field value                        put(key, field, value)
HSETNX key field value                      nop() if exists('key', field) else put('key', field value)
                                            or
                                            get('key').setdefault(field, value)
HVALS key                                   get('key').values()
INCR key                                    incr('key')
INCRBY key increment                        incr('key', by=increment)
INFO
KEYS pattern                                [k for k in keys() if ...]
LASTSAVE
LINDEX key index                            get('key', index)
LINSERT key before|after pivot value        get('key').insert(pivot, value)
LLEN key                                    length('key')
LPOP key                                    get('key').pop(0)
LPUSH key value [value...]                  get('key').append(value)
                                            or
                                            get('key').extend(value)
                                            or
                                            get('key').insert(pos, value)
LPUSHX key value                            get('key').insert(0, value) if exists('key') else nop()
LRANGE key start stop                       get('key')[start:stop] # Note: stride also works
LSET key index value                        put('key', index, value)
LTRIM key start stop                        put('key', get('key')[start:stop])
MGET key [key...]                           get('key'), get('key), ...
MONITOR
MOVE key db
MSET key value [key value]                  put('key', value), put('key', value)
MSETNX key value [key value]                nop() if (exists(key) or exists(key2)...) else put(key, value), put(key2, value2)...
MULTI
OBJECT subcommand ...
PERSIST key                                 persist('key')
PING                                        ping()
PSUBSCRIBE
PUBLISH
PUNSUBSCRIBE
QUIT
RANDOMKEY                                   randkey()
RENAME key newkey                           rename(key, newkey)
RENAMENX key newkey                         nop() if exists(key) else rename(key, newkey)
RPOP key                                    get('key').pop()
RPOPLPUSH source dest                       get('key2').insert(0, get('key1').pop())
RPUSH key value [value]                     get('key').append(value), ...
RPUSHNX                                     get('key').append(value) if exists('key') else nop()
SADD key member member                      get('key').add(member)
                                            or
                                            get('key').update([member, member])
SAVE
SCARD                                       length('key')
SDIFF key [key...]                          get('key1') - get('key2') ...
SDIFFSTORE dest key [key...]                put('dest', get('key1') - get('key2') ...)
SELECT
SET key value                               put('key', value)
SETBIT key offset value                     put('key', offset, value)
SETEX key seconds value                     put('key, value, expiry=when)
SETNX key value                             nop() if exists('key') else put('key', value)
SETRANGE key offset value                   put('key', get('key')[:offset] + value) # TODO: ...or something like that
SHUTDOWN
SINTER key1 [key2...]                       get('key1') & get('key2') ... -or- get('key1').intersection(get('key2'))
SINTERSTORE dest key1 [key2...]             put('dest', get('key1') & get('key2') ...)
SISMEMBER key member                        member in get('key')
SLAVEOF host port                           TODO:
SLOWLOG ...                                 TODO:
SMEMBERS key                                get('key') -or- list(get('key'))
SMOVE src dest member                       get('dest').add(member), get(src).remove(member)
SORT key [by pattern] [...]                 get('key').sorted() TODO: more complex examples
SPOP key                                    get('key').pop()

SRANDMEMBER x                               random.choice(get('x'))
SREM key member [member2...]                get('key').remove(member), ... -or- .get('key').difference_update((member, member2,...))
STRLEN key                                  length('key')
SUBSCRIBE                                   TODO:
SUNION key [key2...]                        get('key') | get('key2') ...
SUNIONSTORE dest key [key2...]              put('dest', get('key') | get('key2') ...)
SYNC
TTL key                                     ttl('key')
TYPE key                                    kind('key', ...)
UNSUBSCRIBE [channel [channel2...]]]
UNWATCH
WATCH key [key2...]
ZADD key score member [score member...]     get('key').set(score, member) # where value at key is a scoreboard()
ZCARD key                                   length('key')
ZCOUNT key min max                          len(get('key').range(min, max))
ZINCRBY key increment member                get('key').incr(member, increment)
ZINTERSTORE dest numkeys key [key...]       put('dest', get('key1').intersection(get('key2'), ...binop...))
ZRANGE key start stop [withscores]          get('key')[start:stop]
ZRANGEBYSCORE key min max [withscores]      get('key').range(min, max)
ZRANK key member                            get('key').rank(member)
ZREM key member [member...]                 get('key').pop(member), ...
ZREMRANGEBYRANK key start stop              get('key').remove(get('x')[start:stop])
ZREMRANGEBYSCORE key min max                get('key').remove(get('x').range(min, max))
ZREVRANGE key start stop [withscores]       get('key')[start:stop][::-1]
ZREVRANGEBYSCORE key min max [withscores]   get('key').range(min, max)[::-1]
ZREVRANK key member                         get('key').revrank(member)
ZSCORE key member                           get('key').score(member) # where value at key is a scoreboard
ZUNIONSTORE dest numkeys key [key...]       put('dest', get('key1').union(get('key2'), ...binop...))
   [weights]
EVAL


SETNX key value                             nop() if exists('key') else put('key', 'value')

Functions without an equivalent in REDIS
expirations()   - get a list of all pending expirations



SortedSets : use blist.sortedset

REDIS   <->     RECOL data types mapping
------------------------------------------------------------------------------------------------------------------------
string          string
list            list
hash            dict
set             set
sortedset       ?? use sortedset?
bitset          bitarray
                heap/priority queue
                decimal
                datetime
                deque
                scoreboard
                numpy array/matrix
                pandas time series
                graph (e.g. igraph)

ADVANTAGES OVER REDIS
Extra data types:
    date, datetime
    numpy array/matrix
Polymorphic; Fewer commands to remember; leverages methods on individual data structure types; more orthogonal
Arbitrarily nested data structures, e.g. set('a', [1,2,[10,20,[100,200]]])
Using Python syntax in queries: nested function calls, return tuples of many commands, listcomps, math functions etc.
Works the same on every platform (win32 is a first class citizen)
Uses standard JSON serialization; convenient for web apps; save JSON directly to db (internally it is stored as equivalent Python data)
Control synchronisation via explicit ssync(), hsync() commands
Fewer server roundtrips, e.g. put('key', {"foo":"bar", "baz":"bip}) vs. hset("key", "foo", "bar" then hset "key" "baz" "bip"
Slicing
It doesn't crash (https://github.com/antirez/redis/issues/243)
I can make it run in either embeddable or server mode (TODO: think about API when embedding)


RECOL antipatterns
-------------------------
No!                 Yes!                why?
len(get('key'))     length('key')       get(...) triggers journaling if the returned value is nonscalar

"""


# TODO: make another module the main entry point to avoid pyc compilation every time
import re, os, zmq, time, ujson, traceback, types, threading, sqlite3, copy, random, math
import tailer
import decimal, datetime, numpy, heapq, bisect, blist, operator
import pydoc
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

import cStringIO as StringIO
# TODO:? too dangerous? import networkx as nx
import gc
# TODO: add stddev, other stats functions
#from blist import sortedset
from collections import deque
#from Bio.trie import trie
import win32api # TODO:
# NOTE: DO NOT IMPORT PYTHONCOM, there is a weird interaction with zmq that kills performance, see
# https://github.com/zeromq/pyzmq/issues/163
#import pythoncom # TODO: just for CreateGuid--need a better solution that is cross-platform
#from collections import deque
import Queue # TODO: don't expose to user code
#from bitarray import bitarray as bitarray_impl
from bitarray import bitarray
from fastcopy import fast_copy
import logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO) # TODO: configuration

ujson_loads = ujson.loads
ujson_dumps = ujson.dumps

# TODO: implement a score class; this wraps sortedset and also stores member names in a dict with their score, so it's
# easy/fast to remove a member from scoreboard.

JOURNALFILE = r"c:\temp\journal.txt"
CONSOLE_EVENTS = {0 : "CTRL_C_EVENT",
                  1 : "CTRL_BREAK_EVENT",
                  2 : "CTRL_CLOSE_EVENT",
                  5 : "CTRL_LOGOFF_EVENT",
                  6 : "CTRL_SHUTDOWN_EVENT"}

class NOTSET : pass
class QUITSENTINEL : pass
class FLUSHSENTINEL : pass

INT_TYPES = set((int, long))
STRING_TYPES = set((str, unicode))
SCALAR_TYPES = set((type(None), type(True), type(1), type(1.0), type(""), type(u"")))
DATETIME_TYPE = datetime.datetime
SMALLEST_LEXICAL_CHAR = ''
LARGEST_LEXICAL_CHAR = '\xff'

journal_queue = Queue.Queue()

class Error(Exception):
    pass

# TODO: implement __slots__
class scoreboard(object):
    def __init__(self, it=()): # it is iterable of (score, member)
        self._set = blist.sortedset(it)  # set of (score, member), ...
        self._map = {y:x for x,y in self._set}

    def __len__(self):
        return len(self._set)

    def __getitem__(self, items):
        return self._set.__getitem__(items)

    def __contains__(self, member):
        return (member in self._map)

    # Note that difference, symmetric_difference (xor) don't make sense in this context since by definition
    # the member doesn't exist in both, therefore you can't apply binop
    def intersection(self, other, binop=operator.add):
        return scoreboard([(binop(self._map[m], other._map[m]), m) for m in (self._map.viewkeys() & other._map.viewkeys())])

    def union(self, other, binop=operator.add):
        return scoreboard([(binop(self._map.get(m,0), other._map.get(m,0)), m) for m in (self._map.viewkeys() | other._map.viewkeys())])

    def iterkeys(self):
        return self._map.iterkeys()

    def keys(self):
        # NOTE: we return a set here so we can use the |, &, -, ^ operators on the result
        retval = set(self.iterkeys())
        return retval
    members = keys

    def set(self, score, member):
        prev_score = self._map.get(member, None)
        if prev_score != None:
            self._set.discard((prev_score, member))
        item = (score, member)
        self._set.add(item)
        self._map[member] = score
        return item

    def pop(self, member):
        score = self._map.get(member)
        if score != None:
            return self.remove([(score, member)])

    def remove(self, items):
        for item in items:
            self._map.pop(item[1])
            self._set.remove(item)
        return len(items)

    def score(self, member):
        return self._map.get(member, None)

    def rank(self, member):
        score = self._map.get(member)
        if score is None:
            return None
        return self._set.index((score, member))

    def revrank(self, member):
        rank = self.rank(member)
        if rank != None:
            return len(self._set) - 1 - rank

    def range(self, minscore, maxscore):
        """
        Return data with minscore <= score <= maxscore
        """
        start = self._set.bisect_left((minscore, SMALLEST_LEXICAL_CHAR))
        stop = self._set.bisect_right((maxscore, LARGEST_LEXICAL_CHAR))
        return self._set[start:stop]

    def incr(self, member, by=1):
        prev_score = self._map.get(member, None)
        if prev_score == None:
            return
        return self.set(prev_score+by, member)

    def toDict(self):
        return {"type" : "scoreboard", "repr" : list(self._set)}


class JournalWriterThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def process_tx(self, tx):
        #print("processing journal item %s" % item)
        #time.sleep(0.1)
        #self.curs.execute("insert into journal values(null, '%s')" % item)
        # TODO: need to batch these to improve performance
        #self.journal.write("insert into journal values(null, '%s')\n" % item)
        msg = "".join(["%d:%s:%s:%s:%s\n" % (txid, repr(key), cmd, args, val) for txid, key, cmd, args, val in tx])
        #print("begin write journal")
        #print msg[:-1]
        #for item in tx:
        #    key, cmd, val = item
        #    print("journal entry: %s, %s, %s" % (key, cmd, val))
        #print("end write journal")
        self.journal.write(msg)
        self.journal.flush() # TODO: remove, just for debugging
        #self.journal.write("%s,%s,%s" % (item[0], item[1], base64.encodestring(item[1])))
        self.ctr += 1
        #if self.ctr % self.BATCH_SIZE == 0:
        #    self.db.commit()

    def run(self):
        self.journal = open(JOURNALFILE, "ab") # TODO: location
        self.BATCH_SIZE = 1000
        self.ctr = 0
        QSIZE_WARNING_LIMIT = 200
        last_warning_time = time.time() 
        #log.info("controller dbthread starting...")
        log.info("journal writer thread starting...") 
        try:
            while True:
                try:
                    qsize = journal_queue.qsize()
                    if (qsize >= QSIZE_WARNING_LIMIT) and (time.time() - last_warning_time > 5):
                        log.warning("db queue size: %d" % qsize)
                        last_warning_time = time.time()
                    tx = journal_queue.get(True, 2.0)
                except Queue.Empty:
                    pass
                else:
                    # TODO: make sure no real data can be in queue after the quit sentinel
                    if tx == QUITSENTINEL:
                        log.info("journal writer thread got exit sentinel")
                        return
                    else:
                        try: 
                            self.process_tx(tx)
                        finally:
                            journal_queue.task_done()
        finally:
            #self.db.commit()
            log.info("journal writer thread stopping")            


# TODO: need to wrap objects in safe proxies, to avoid mutation side effects such as
# SET('/foo', set([1,2,3])
# GET('/foo').pop() # this is a GET but there is a mutation side effect! 
# OOORRRR, do we? Maybe we should just keep track of any key that is either GET or SET and
# write all of them to the journal, since operations done on objects returned by GET could have
# had side effects...This could nicely sidestep the issue

def _key_from_obj(self):
    try:
        key = OBJMAP[id(self)]
    except KeyError:
        raise Error("can't mutate embedded object with that syntax, try get('key', idx,...).func(...) instead")
    else:
        #print "%s -> id %s -> %s" % (self, id(self), key)
        return key

reallist = list
class wrappedlist(reallist):
    """
     '__add__' - ok, returns a new list
     '__class__' - ok, idempotent
     '__contains__', - ok, idempotent
     '__delattr__', - ok, del can't be used in eval expression
     '__delitem__', - ok, del can't be used in eval expression
     '__delslice__', ok, del can't be used in eval expression
     '__doc__', ok, idempotent
     '__eq__', ok, idempotent
     '__format__', ok, idempotent
     '__ge__', ok, idempotent
     '__getattribute__', ok, idempotent
     '__getitem__', ok, idempotent
     '__getslice__', ok, idempotent
     '__gt__', ok, idempotent
     '__hash__', ok, idempotent
     '__iadd__', ok, can't be used in eval, e.g. get('x') += 3
     '__imul__', ok, can't be used in eval, e.g. get('x') *=3
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
     '__setitem__', ok, x[y] = z can't be called with expression syntax
     '__setslice__', ok, x[y:z] = a can't be called with expression syntax
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
     'reverse', TODO:
     'sort' TODO:
    """
    def append(self, val):
        val = _wrap(val)
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        reallist.append(self, val)
        # NOTE: the idea here is that we want to avoid making a deep copy of the before value when possible.
        # The rollback function should be the cheapest way possible to restore the data back to its previous state.
        # Only make a copy of the previous value if there is no other way to achieve the same effect. It's not so
        # much that we want rollbacks to be fast, it's that we want the happy path to be fast, and deep copies are
        # expensive.
        ROLLBACKLIST.append((self.pop, ()))
        COMMITLIST.append((TXID, key, "APPEND", "", serialize(val))) # TODO: get rid of all dotted qualifiers by rebinding

    def extend(self, val):
        val = _wrap(val)
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        prev_len = len(self)
        reallist.extend(self, val)
        val_len = len(self) - prev_len # we do it this way because val could be an iterator
        ROLLBACKLIST.append((lambda self : [self.pop() for i in range(val_len)], (self,)))
        COMMITLIST.append((TXID, key, "EXTEND", "", serialize(val)))

    def insert(self, index, val):
        val = _wrap(val)
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        reallist.insert(self, index, val)
        ROLLBACKLIST.append((self.pop, (index,)))
        COMMITLIST.append((TXID, key, "INSERT", index, serialize(val)))

    def pop(self, index=None):
        key = _key_from_obj(self) # IMPORTANT: must come before superclass call
        if index is None:
            index = len(self) - 1
        val = reallist.pop(self, index) # TODO: what if this is a ref; do we need to do a deepcopy?
        ROLLBACKLIST.append((self.insert, (index, val)))
        COMMITLIST.append((TXID, key, "POP", index, ""))
        return val

    def _append(self, val):
        reallist.append(self, val)


list = wrappedlist

# toDict methods for types that don't have a direct representation in JSON
realset = set
class fakeset(realset):
    def toDict(self):
        return {"type" : "set", "repr" : sorted(self)}
set = fakeset

realset = set
class fakeset(realset):
    def toDict(self):
        return {"type" : "set", "repr" : sorted(self)}
set = fakeset

realdate = datetime.date
class fakedate(realdate):
    def toDict(self):
        return {"type" : "datetime.date", "repr" : self.isoformat()}
setattr(datetime, "date", fakedate)

realblist = blist.blist
class fakeblist(realblist):
    def toDict(self):
        return {"type" : "blist.blist", "repr" : list(self)}
setattr(blist, "blist", fakeblist)

realdatetime = datetime.datetime
realtime = datetime.time

bitarray.toDict = lambda self : {"type": "bitarray", "repr" : self.to01()}
#set.toDict = lambda self : {"type":"set", "repr" : sorted(self)}
#blist.sortedlist.toDict = lambda self : {"type":"blist.sortedlist", "repr": list(self)}
#blist.blist.toDict = lambda self : {"type":"blist.blist", "repr": list(self)}
blist.sortedlist.toDict = lambda self : {"type":"blist.sortedlist", "repr": list(self)}
blist.sortedset.toDict = lambda self : {"type":"blist.sortedset", "repr": list(self)}
#datetime.datetime.toDict = lambda self : {"type" : "datetime.datetime", "repr" : self.isoformat()}
#datetime.date.toDict = lambda self : {"type" : "datetime.date", "repr" : self.isoformat()}
#datetime.time.toDict = lambda self : {"type" : "datetime.date", "repr" : str(self)}

class XPerSec:
    def __init__(self, things):
        self.things = things
        self.start = time.clock()
        self.count = 0

    def incr(self):
        now = time.clock()
        elapsed = now - self.start
        if elapsed > 1.0:
            print "%.2f %s/sec" % (self.count/elapsed, self.things)
            self.count = 1
            self.start = now
        else:
            self.count += 1

TXID = 0
D = {}
OBJMAP = {}
COMMITLIST = []
ROLLBACKLIST = []
VARS = {}
TEMPVAR = "_"
TEMPVAR0= "_0"
TEMPVAR1 = "_1"
TEMPVAR2 = "_2"
EXPIRY_LIST = "_expiry_list"
EXPIRY_XREF = "_expiry_xref"
D[EXPIRY_LIST] = blist.sortedlist() # TODO: work out how this gets initialized
D[EXPIRY_XREF] = {} # TODO: work out how this gets initialized

TXD = {}

IDENTITY = lambda x : x
#XFORM_JSON = {} # TODO: {bitarray : lambda x : x.to01(),
#              #set : lambda x : list(x)}

debug_writes = 0

#def help(o):
    # TODO: pydoc is funky
    #io = StringIO.StringIO()
    #h = pydoc.Helper(output=io)
    #h.help(o)
#    h = pydoc.TextDoc()
#    return h.docmodule(o)

# TODO: need to work out serialization problems with ujson and datetime.date/datetime/time
#def today():
#    return datetime.date.today()
#def now():
#    return datetime.datetime.now()

def nop():
    pass

def ping():
    return

# TODO: needs more thought, e.g. how do you retrieve the value; should it stay constant for tx, etc...
#def guid():
#    return str(pythoncom.CreateGuid())[1:-1]

def crash():
    raise Error("forced error")

def j2p(s):
    # Converts from JSON to Python
    return ujson_loads(s)

def p2j(s):
    return ujson_dumps(s)

def echo(value):
    return value

# TODO: obviated by info?
#def numkeys():
#    # NOTE: TODO: this includes internal keys starting with _, e.g. for managing expirations
#    return len(D)

def keys():
    return sorted(D.keys())

def randkey(iterate=False):
    choice = random.randrange(len(D))
    # TODO: instantiating keys() could be very expensive--consider iteration?
#    if iterate:
#        it = D.iterkeys()
#        value = it.next()
#        while choice > 0:
#            value = it.next();
#            
#        while choice > 0:
#        while choice > 0:
#            
#    else:
    return D.keys()[choice] 

# TODO: can this be made to work with arbitrary number of indexers?
def erase(key, index=None):
    # TODO: convert to update TXD
    if index != None:
        del D[key][index]
    else:
        del D[key]
    # TODO: write to journal
    
def exists(key, *idxs):
    try:
        _get(key, *idxs) # NOT get(), as we don't want to trigger journal
    except (TypeError, AttributeError, KeyError):
        return False
    else:
        return True

#WRAPPERS = {reallist : list}

# NOTE: knowledgeable users can use this instead of get(...) when they know they
# will not be performing a mutating action on the returned object.
# TODO: consider making get(...) never journal. If user knows they are doing a mutating
# action make them use mut('key',...) The rationale is that if we have to figure out whether
# the action is mutating it will slow down the server
def _get(key, *idxs, **kwargs):
    obj = D.get(key, kwargs.get("default", None))
    for idx in idxs:
        obj = obj[idx]
    #obj = WRAPPERS[type(obj)](obj)
    OBJMAP[id(obj)] = (key,) + idxs
    return obj

def length(key, *idxs, **kwargs):
    return len(_get(key, *idxs, **kwargs))

get = _get

def copy(key, *idxs, **kwargs):
    return fast_copy(_get(key, *idxs, **kwargs))

#def get(key, *idxs, **kwargs):
#    return fast_copy(_get(key, *idxs, **kwargs))

def kind(key, *idxs):
    return _get(key, *idxs).__class__.__name__

def expirations():
    return D[EXPIRY_LIST] # can't affect db, so don't trigger journaling by using get

def expire(key, when):
    if type(when) == DATETIME_TYPE:
        when = time.mktime(when.timetuple())
    expiry_list = get(EXPIRY_LIST) # triggers journaling
    expiry_xref = get(EXPIRY_XREF) # triggers journaling
    existing = expiry_xref.get(key)
    if existing:
        # This key already has an expiry specified. It only makes sense to have a single expiry for
        # each key, so we get rid of the old one
        expiry_list.discard(existing)
    value = (when, key)
    expiry_list.add(value)
    expiry_xref[key] = value

def persist(key):
    existing  = D[EXPIRY_XREF].get(key)
    if existing:
        expiry_list = get(EXPIRY_LIST) # NOTE: triggers journal
        expiry_xref = get(EXPIRY_XREF) # NOTE: triggers journal
        expiry_list.discard(existing)
        expiry_xref.pop(key)
        return True
    else:
        return False

def ttl(key):
    existing  = D[EXPIRY_XREF].get(key)
    if existing:
        when, key = existing
        return when - time.time()
    else:
        return None

def putnx(key, value):
    # TODO: journaling; only journal if it didn't exist!
    return D.setdefault(key, value)

#UNWRAPPED_TYPES = set((int, long, str, unicode))

def _wrap_list(o):
    newobj = list()
    for item in o:
        newobj._append(_wrap(item))
    return newobj

WRAPPERS = {type(None) : IDENTITY,
            int : IDENTITY,
            long : IDENTITY,
            float : IDENTITY,
            str : IDENTITY,
            unicode : IDENTITY,
            reallist : _wrap_list,
            wrappedlist : IDENTITY, # TODO: remember to put all wrapped types here too, e.g. put('x', [1,2,3]), get('x').append([10,20,30]), get('x').pop().append(20)
            }

def _wrap(o):
    typ = type(o)
    try:
        wrapper = WRAPPERS[typ]
    except KeyError:
        raise Error("unsupported type '%s'" % typ)

    return wrapper(o)

#    if typ == reallist:
#        newobj = list()
#        for item in o:
#            newobj._append(_wrap(item))
#        return newobj
#    elif typ in UNWRAPPED_TYPES:
#        return o
#    else:
#        raise Error("unsupported type '%s'" % typ)

def _put(key, *args, **kwargs):
    idxs = args[:-1]
    val = args[-1]
    val = _wrap(val)
    if idxs:
        obj = D[key]
        for idx in idxs[:-1]:
            obj = obj[idx]
        prev = obj[idxs[-1]]
        obj[idxs[-1]] = val
    else:
        try:
            prev = D[key]
        except KeyError:
            prev = None
        D[key] = val
    expiry = kwargs.get("expiry")
    if expiry:
        expire(key, expiry)
    else:
        pass # TODO: if key previously existed and was set to expire, but we don't specify expire here,
        # then need to clear the expiry
    global debug_writes
    debug_writes += 1 # TODO:
    return (key,)+idxs, prev, val

def put(key, *args, **kwargs):
    fullkey, prev, val = _put(key, *args, **kwargs)
    # TODO: if item didn't exist before, then rollback should indicate to DEL the key, not set to None!
    ROLLBACKLIST.append((_put, fullkey + (prev,))) # TODO: what about rolling back key that had expiry?
    COMMITLIST.append((TXID, fullkey, "SET", "", serialize(val))) # # TODO: get rid of all dotted qualifiers by rebinding

def incr(key, *args, **kwargs):
    value = get(key, *args, **kwargs) + kwargs.get("by",1)
    put(key, *(args+(value,)))
    return value

def info():
    # TODO: other useful info
    return {"num_keys" : len(D), }

def decr(key, *args, **kwargs):
    kwargs["by"] = -kwargs.get("by", 1)
    return incr(key, *args, **kwargs)
    
def rename(oldkey, newkey):
    D[newkey] = D[oldkey]
    del D[oldkey]
    # TODO: write to journal

def dump():
    return D
    #return {key : XFORM_JSON.get(type(value), IDENTITY)(value) for key, value in D.iteritems()}

def tmp(value=NOTSET):
    return var(TEMPVAR, value)

def tmp0(value=NOTSET):
    return var(TEMPVAR0, value)

def tmp1(value=NOTSET):
    return var(TEMPVAR1, value)

def tmp2(value=NOTSET):
    return var(TEMPVAR2, value)

def var(name, value=NOTSET):
    # set or get value of temp var
    if value == NOTSET:
        return VARS[name]
    else:
        VARS[name] = value
        return value

def mutate(key):
    pass # TODO: return object from D (not copied) to allow mutation, then somehow do a put(key, obj)
         # at the end of the tx


EVAL_GLOBALS = {"__builtins" : None}
#make a list of safe functions
EVAL_LOCALS = { "datetime":datetime,
                "list" : list,
                "math" : math,
                # commands
                "copy" : copy,
                "crash" : crash,
                "decr" : decr,
                "dump" : dump,
                "echo" : echo,
                "exists" : exists,
                "get" : get,
                "incr" : incr,
                "j2p" : j2p,
                "kind" : kind,
                "length" : length,
                "nop" : nop,
                "p2j" : p2j,
                "ping" : ping,
                "put" : put,
              }

from itertools import islice
# from http://stackoverflow.com/questions/260273/most-efficient-way-to-search-the-last-x-lines-of-a-file-in-python
def reversed_lines(file):
    "Generate the lines of file in reverse order."
    tail = []           # Tail of the line whose head is not yet read.
    for block in reversed_blocks(file):
        # A line is a list of strings to avoid quadratic concatenation.
        # (And trying to avoid 1-element lists would complicate the code.)
        linelists = [[line] for line in block.splitlines()]
        linelists[-1].extend(tail)
        for linelist in reversed(linelists[1:]):
            yield ''.join(linelist)
        tail = linelists[0]
    if tail: yield ''.join(tail)

# from http://stackoverflow.com/questions/260273/most-efficient-way-to-search-the-last-x-lines-of-a-file-in-python
def reversed_blocks(file, blocksize=4096):
    "Generate blocks of file's contents in reverse order."
    file.seek(0, os.SEEK_END)
    here = file.tell()
    while 0 < here:
        delta = min(blocksize, here)
        file.seek(here - delta, os.SEEK_SET)
        yield file.read(delta)
        here -= delta

class Server:
    def __init__(self):
        self.D = {}
        self.exit_requested = False
        self.journal_writer_thread = JournalWriterThread()
        self.zmq_context = zmq.Context()

    def console_ctrl_handler(self, event):
        """
        Return True if we handled the event, False to continue
        passing event to upstream handlers
        """
        print ("console event %s (%s)" % (CONSOLE_EVENTS.get(event, "UNKNOWN"), event))
        self.exit_requested = True
        self.zmq_context.term()
        return True

    def get_last_txid(self):
        global TXID
        print "searching %s for latest txid" % JOURNALFILE
        try:
            fp = open(JOURNALFILE, "rb")
        except IOError, e:
            print str(e)
            TXID = 0
        else:
            try:
                try:
                    line = tailer.tail(fp, 1)[0]
                except IndexError:
                    TXID = 0
                else:
                    parts = line.split(":")
                    TXID = long(parts[0])
            finally:
                fp.close()
        finally:
            print "last txid was %s" % TXID

    def load_from_db(self):
        self.db = sqlite3.connect("recollection.dat") # TODO:
        self.curs = self.db.cursor()
        # TODO: rename cols i1-i8, we have 9 currently and only want 8
        self.curs.execute("select * from master order by key, i0, i1, i2, i3, i4, i5, i6, i7, i8;")
        # TODO: need lazy iterator fetch here
        rows = 0
        start = time.time()
        for row in self.curs.fetchall():
            rows += 1
            key, i0, i1, i2, i3, i4, i5, i6, i7, i8, val = row
            if i0 == None:
                D[key] = deserialize(val)
            else:
                raise NotImplementedError()
            #print row
        print "loaded %d rows in %.2f secs" % (rows, time.time()-start)

    def do_expiry(self, now=None):
        if not now:
            now = time.time()
        while True:
            try:
                when, key = D[EXPIRY_LIST][0]
            except IndexError:
                # nothing in the expiration list
                return
            else:
                if when < now:
                    #print ("expiring key '%s'" % key) # TODO: remove
                    erase(EXPIRY_LIST, 0)
                    erase(EXPIRY_XREF, key)
                    erase(key)
                else:
                    break

    def run(self):
        
        log.info("pid: %s" % os.getpid())

        # TODO: windows specific
        win32api.SetConsoleCtrlHandler(self.console_ctrl_handler, 1)

        self.get_last_txid()

        self.load_from_db()

        self.journal_writer_thread.start()
        
        #counter = XPerSec("requests")
        
        socket = self.zmq_context.socket(zmq.REP)
        socket.bind("tcp://*:5555") # TODO: parameterize ipaddr and port

        #result = {"res":None}
        reqs = 0
        start = time.time()
        while not self.exit_requested:
            try:
                command = socket.recv();
            except zmq.ZMQError, z:
                if str(z) == "Context was terminated": # TODO: brittle
                    break
            #counter.incr()
            #start = time.clock()
            now = time.time()
            execstart = time.clock()
            self.do_expiry(now)
            global VARS, OBJMAP, COMMITLIST, ROLLBACKLIST, TXID
            TXID += 1
            VARS = {}
            OBJMAP = {} # TODO: what's faster, d.clear() or d={}?
            COMMITLIST = []
            ROLLBACKLIST = []
            try:
                #result = ujson_dumps(eval(command)) # TODO: make safe by controlling locals/globals
                result = eval(command, EVAL_GLOBALS, EVAL_LOCALS)
                result = ujson.dumps({"res":result, "cps": 1. / (time.clock() - execstart)})

                #result = ujson.dumps(eval('get("foo")'))
                #result = ujson.dumps(get("foo"))
                #result = '"foo"'
                #xform = XFORM_JSON.get(type(res))
                #if xform:
                #    res = xform(res)
                #cooked = XFORM_JSON.get(type(raw), IDENTITY)(raw)
                #print "cooked: %s (%s)" % (cooked, type(cooked))
                #result = {"res" : res}
            except Exception, e:
                #errbuff.truncate()
                print "START ROLLBACK:" # TODO:
                for item in reversed(ROLLBACKLIST):
                    print item
                    func, args = item
                    func(*args)
                print "END ROLLBACK:" # TODO:
                traceback.print_exc()
                result = ujson_dumps({"err" : "%s: %s" % (e.__class__.__name__, str(e))})
            else:
                if COMMITLIST:
                    COMMITLIST.append((TXID, "", "COMMIT", "", "")) # TODO: get rid of all dotted qualifiers by rebinding
                    journal_queue.put(COMMITLIST)
            finally:
                #finish = time.clock()
                elapsed = now - start
                if elapsed > 2: # TODO: parameterize
                    print "%.2f req/sec" % (reqs/elapsed)
                    reqs = 0
                    start = now
                else:
                    reqs += 1
                #result["ms"] = (time.clock() - start) * 1000.
            #print "got request", request
            #try:
            #    json = ujson.dumps(result)
            #except Exception, e:
            #    #del result["res"]
            #    result["err"] = "Failed to serialize result: %s" % str(e)
            #    json = ujson.dumps(result)
            # TODO: NOTE: need to handle the fact that context could be terminated here as well--
            # actually -- we don't want it to happen here but currently it can!
            """
            Traceback (most recent call last):
              File "recolsvr.py", line 397, in <module>
                server.run()
              File "recolsvr.py", line 388, in run
                socket.send(json)
              File "socket.pyx", line 553, in zmq.core.socket.Socket.send (zmq\core\socket.c:5758)
              File "socket.pyx", line 600, in zmq.core.socket.Socket.send (zmq\core\socket.c:5530)
              File "socket.pyx", line 167, in zmq.core.socket._send_copy (zmq\core\socket.c:2039)
            zmq.core.error.ZMQError: Context was terminated
            """
            socket.send(result)
        journal_queue.put(QUITSENTINEL)            
        log.info("waiting for journal writer thread to catch up")
        self.journal_writer_thread.join()
        print "did %d writes, verify journal contains same number!" % debug_writes

def main():
    #gc.disable() # TODO:?
    server = Server()
    server.run()

def profile_main():
	pass
    # import hotshot, hotshot.stats
    # prof = hotshot.Profile("recolsvr.prof")
    # prof.runcall(main)
    # prof.close()
    # stats = hotshot.stats.load("recolsvr.prof")
    # stats.sort_stats("time", "calls")
    # stats.print_stats(50)

if __name__ == "__main__":
	main()

