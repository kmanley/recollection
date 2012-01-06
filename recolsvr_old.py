# TODO: try implementing in Cython and see if that's any faster
"""
create table master (key text, idx text, value text, primary key (key, idx));
CREATE TABLE journal (id integer primary key, sql text);
CREATE TABLE last (id integer);
insert into last values (0);

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
HDEL key field                              get('key').pop(field)
HEXISTS key field                           get('key').has_key(field)
HGET key field                              get('key')[field]
HGETALL key                                 get('key') # returns JSON
HINCRBY key field increment                 incr('key', 'field', by=increment)
HKEYS key                                   get('key').keys()
HLEN key                                    length('key')
HMGET key field [field...]                  get('key')[field], get('key')[field]
HMSET key field value [field value]         put('key', field, value), put('key', field2, value2), ...
                                            or
                                            get('key').update({"field":value, "field":value})
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
ZINTERSTORE dest numkeys key [key...]
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
ZUNIONSTORE dest numkeys key [key...]
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
                deque
                score
                numpy array/matrix
                pandas time series
                graph (e.g. igraph)

ADVANTAGES OVER REDIS
Extra data types:
    date, datetime
    numpy array/matrix
Fewer commands to remember; leverages methods on individual data structure types; more orthogonal
Arbitrarily nested data structures, e.g. set('a', [1,2,[10,20,[100,200]]])
Using Python syntax in queries: nested function calls, return tuples of many commands, listcomps, math functions etc.
Works the same on every platform (win32 is a first class citizen)
Uses standard JSON serialization
Control synchronisation via explicit ssync(), hsync() commands
Fewer server roundtrips, e.g. put('key', {"foo":"bar", "baz":"bip}) vs. hset("key", "foo", "bar" then hset "key" "baz" "bip"
Slicing
It doesn't crash (https://github.com/antirez/redis/issues/243)

RECOL antipatterns
-------------------------
No!                 Yes!                why?
len(get('key'))     length('key')       get(...) triggers journaling if the returned value is nonscalar

"""


# TODO: make another module the main entry point to avoid pyc compilation every time
import re, os, zmq, time, ujson, traceback, types, threading, sqlite3, copy, random, math, decimal, datetime, numpy, heapq, bisect, blist
# TODO: add stddev, other stats functions
#from blist import sortedset
from collections import deque
#from Bio.trie import trie
import win32api
#from collections import deque
import Queue # TODO: don't expose to user code
#from bitarray import bitarray as bitarray_impl
from bitarray import bitarray
import logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO) # TODO: configuration

# TODO: implement a score class; this wraps sortedset and also stores member names in a dict with their score, so it's
# easy/fast to remove a member from scoreboard.

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

# TODO: implement __slots__
class scoreboard(object):
    def __init__(self, it=()): # it is iterable of (score, member)
        self._set = blist.sortedset(it)  # set of (score, member), ...
        self._map = {} # map of member : score
        for score, member in it:
            self._map[member] = score

    def __len__(self):
        return len(self._set)

    def __getitem__(self, items):
        return self._set.__getitem__(items)

    def __contains__(self, member):
        return (member in self._map)

    def __or__(self, other):
        return scoreboard([(0, member) for member in (self._map.viewkeys() | other._map.viewkeys())])

    def __and__(self, other):
        return scoreboard([(0, member) for member in (self._map.viewkeys() & other._map.viewkeys())])

    def __sub__(self, other):
        return scoreboard([(0, member) for member in (self._map.viewkeys() - other._map.viewkeys())])

    def __xor__(self, other):
        return scoreboard([(0, member) for member in (self._map.viewkeys() ^ other._map.viewkeys())])

    def iterkeys(self):
        return self._map.iterkeys()

    def keys(self):
        return list(self.iterkeys())
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

    def revrange(self, minscore, maxscore):
        return sorted(self.range)

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

    def process_item(self, item):
        #print("processing journal item %s" % item)
        #time.sleep(0.1)
        #self.curs.execute("insert into journal values(null, '%s')" % item)
        # TODO: need to batch these to improve performance
        self.journal.write("insert into journal values(null, '%s')\n" % item)
        self.ctr += 1
        #if self.ctr % self.BATCH_SIZE == 0:
        #    self.db.commit()

    def run(self):
        self.journal = open(r"c:\temp\journal.txt", "w")
        #self.db = sqlite3.connect("recollection.dat") # TODO:
        #self.curs = self.db.cursor()
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
                    item = journal_queue.get(True, 2.0)
                except Queue.Empty:
                    pass
                else:
                    # TODO: make sure no real data can be in queue after the quit sentinel
                    if item == QUITSENTINEL:
                        log.info("journal writer thread got exit sentinel")
                        return
                    else:
                        try: 
                            self.process_item(item)
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

realdatetime = datetime.datetime
realtime = datetime.time


bitarray.toDict = lambda self : {"type": "bitarray", "repr" : self.to01()}
#set.toDict = lambda self : {"type":"set", "repr" : sorted(self)}
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

D = {}
EXPIRY_LIST = "_expiry_list"
EXPIRY_XREF = "_expiry_xref"
D[EXPIRY_LIST] = blist.sortedlist() # TODO: work out how this gets initialized
D[EXPIRY_XREF] = {} # TODO: work out how this gets initialized

TXD = {}

IDENTITY = lambda x : x
#XFORM_JSON = {} # TODO: {bitarray : lambda x : x.to01(),
#              #set : lambda x : list(x)}

debug_writes = 0

def help():
    return "TODO:" # TODO:

# TODO: need to work out serialization problems with ujson and datetime.date/datetime/time
#def today():
#    return datetime.date.today()
#def now():
#    return datetime.datetime.now()

def nop():
    pass

def ping():
    return

def crash():
    raise Exception("forced error")

def decode(s):
    # Converts from JSON to Python
    return ujson.loads(s)

def encode(s):
    return ujson.dumps(s)

def echo(value):
    return value

def numkeys():
    # NOTE: TODO: this includes internal keys starting with _, e.g. for managing expirations
    return len(D)

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

# NOTE: knowledgeable users can use this instead of get(...) when they know they
# will not be performing a mutating action on the returned object
def _get(key, *idxs, **kwargs):
    obj = D.get(key, kwargs.get("default", None))
    for idx in idxs:
        obj = obj[idx]
    return obj

def length(key, *idxs, **kwargs):
    return len(_get(key, *idxs, **kwargs))

def get(key, *idxs, **kwargs):
    obj = _get(key, *idxs, **kwargs)
    if type(obj) not in SCALAR_TYPES:
        pass
        # TODO: need to track this; could be mutated...e.g. get('a').append('foo')
    return obj

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

def put(key, *args, **kwargs):
    idxs = args[:-1]
    val = args[-1]
    if idxs:
        obj = D[key]
        for idx in idxs[:-1]:
            obj = obj[idx]
        obj[idxs[-1]] = val        
    else:
        D[key] = val
    expiry = kwargs.get("expiry")
    if expiry:
        expire(key, expiry)
    global debug_writes
    debug_writes += 1 # TODO:
    journal_queue.put("foobar")

def incr(key, *args, **kwargs):
    value = get(key, *args, **kwargs) + kwargs.get("by",1)
    put(key, *(args+(value,)))
    return value

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

    def do_expiry(self):
        now = time.time()
        while True:
            try:
                when, key = D[EXPIRY_LIST][0]
            except IndexError:
                # nothing in the expiration list
                return
            else:
                if when < now:
                    print ("expiring key '%s'" % key) # TODO: remove
                    erase(EXPIRY_LIST, 0)
                    erase(EXPIRY_XREF, key)
                    erase(key)
                else:
                    break

    def run(self):
        
        log.info("pid: %s" % os.getpid())
        
        # TODO: windows specific
        win32api.SetConsoleCtrlHandler(self.console_ctrl_handler, 1)
        
        self.journal_writer_thread.start()
        
        counter = XPerSec("requests")            
        
        socket = self.zmq_context.socket(zmq.REP)
        socket.bind("tcp://*:5555") # TODO: parameterize ipaddr and port
    
        while not self.exit_requested:
            try:
                command = socket.recv();
            except zmq.ZMQError, z:
                if str(z) == "Context was terminated": # TODO: brittle
                    break
            counter.incr()
            start = time.clock()
            self.do_expiry()
            try:
                res = eval(command) # TODO: make safe by controlling locals/globals
                #xform = XFORM_JSON.get(type(res))
                #if xform:
                #    res = xform(res)
                #cooked = XFORM_JSON.get(type(raw), IDENTITY)(raw)
                #print "cooked: %s (%s)" % (cooked, type(cooked))
                result = {"res" : res}
            except Exception, e:
                #errbuff.truncate()
                traceback.print_exc()
                result = {"err" : "%s: %s" % (e.__class__.__name__, str(e))}
            finally:
                result["ms"] = (time.clock() - start) * 1000.
            #print "got request", request
            try:
                json = ujson.dumps(result)
            except Exception, e:
                del result["res"]
                result["err"] = "Failed to serialize result: %s" % str(e)
                json = ujson.dumps(result)
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
            socket.send(json) 
        journal_queue.put(QUITSENTINEL)            
        log.info("waiting for journal writer thread to catch up")
        self.journal_writer_thread.join()
        print "did %d writes, verify journal contains same number!" % debug_writes

def main():
    server = Server()
    server.run()

if __name__ == "__main__":
    main()


