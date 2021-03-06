"""
So my latest thinking is that maybe we use leveldb/nessdb or some such as the backing store, 
make the client lib multithreaded, do away with our own journal writer/reader, implement
p2p/chord/kademlia for auto-sharding/scaling, and also slaveof-style failover. Also continue to support a rich api
of datatypes, but perhaps use the mutator-style, e.g. append(key, [idxs,...], value) instead 
of get(key, [idxs,...]).append(value)
TODO: what is http://telehash.org/

TODO: should I just use leveldb as storage, then we don't need txlog/sqlite/etc
http://code.google.com/p/leveldb/
So the project could be to put a server in front of this, implement the p2p stuff, etc...
The binary values would be pickles.
Another advantage is that data is sorted by key--allows range queries, e.g. get me all keys that start with People
Another advantage of this is you can use a dataset larger than memory, which REDIS can't do
(TODO: well, seems someone is doing this already https://github.com/srinikom/leveldb-server
take a look at this code because it also demonstrates multithreaded zeromq from python
TODO: consider using leveldb for grid/procman, compare performance
)
Also compare to nessdb
https://github.com/shuttler/nessDB

TODO: use snappy to compress data in the sqlite db

Also take a look at leveldb
http://code.google.com/p/leveldb/


# TODO: look into SQLLite Studio
http://sqlitestudio.one.pl

# TODO: consider using Spread Toolkit
http://www.spread.org/SpreadOverview.html
for communication between recolsvr nodes in a cluster setting, this may make failover/etc easier
Reliable multicast from any number of senders to lots of receivers, etc.
http://www.savarese.com/software/libssrcspread/

http://stackoverflow.com/questions/620877/what-algorithms-there-are-for-failover-in-a-distributed-system
http://en.wikipedia.org/wiki/Chord_%28peer-to-peer%29
http://en.wikipedia.org/wiki/Distributed_hash_table
http://en.wikipedia.org/wiki/Tapestry_%28DHT%29
http://current.cs.ucsb.edu/projects/chimera/
http://ants.etse.urv.es/wiki/Chord
http://en.wikipedia.org/wiki/Kademlia
http://en.wikipedia.org/wiki/Pastry_%28DHT%29
http://ants.etse.urv.es/wiki/Pastry
http://pdos.csail.mit.edu/chord/#downloads
http://kadc.sourceforge.net/
http://www.freepastry.org/ - has a Java implementation
http://khashmir.sourceforge.net/
http://www.heim-d.uni-sb.de/~heikowu/SharkyPy/ - implementation of Kademlia
http://entangled.sourceforge.net/
https://github.com/gardaud/pyChord
http://twistedmatrix.com/trac/browser/sandbox/chord.py
http://lysol.github.com/congress/ # kademlia-like dht lib in python ** TODO: may be able to adapt this
http://blog.notdot.net/2009/11/Implementing-a-DHT-in-Go-part-1 (and part 2)
http://www.bittorrent.org/beps/bep_0005.html
http://www.linuxjournal.com/article/6797

TODO: read http://stackoverflow.com/questions/7700562/dht-bittorrent-vs-kademlia-vs-clones-python

Chimera seems to be a successor to Tapestry (?)
Implementation of Chimera in C - this is a good example of what I envision, an overlay network that can make callbacks
into client code and which can route app-layer messages in addition to message required by the DHT. 
http://current.cs.ucsb.edu/projects/chimera/download.html
See the docs folder in that download for more info

Kademlia seems to be a successor to Chord...(?)

grep pypi for dht, etc.
grep github for dht, chord, etc.
Kademlia looks interesting, node leave/failure is simply no action...
http://everythingisdata.wordpress.com/2009/11/03/chord-a-scalable-peer-to-peer-lookup-service-for-internet-applications/
TODO: can one of these C/C++ overlay networks like Chimera or Tapestry somehow be integrated?
TODO: separate project to implement Chord algorithm on top of ZeroMQ and allow this to be used as a library
by other code including recol
So the cluster vision is:
 - nodes can come and go at any time thanks to chord stabilization
 - when a client has to retrieve a key from the kv store, it uses the chord algorithm to get it from the node where it resides
 - when a client has to write a key to the kv store, it uses the chord algorithm to put it in the node where it belongs
 - clients can talk to any node in the cluster, when client starts you give it the ip of a single node and from 
     that node we can get a list of other nodes; these are returned to the client and it can round-robin requests
 - individual node redundancy via slaveof, can consider failover as a separate issue
 
Considerations for DHT
 - no longer have isolation, clients can be talking to nodes at same time, e.g. for 
     put(key1, get(key2) + get(key3))
   key1, key2, and key3 could map to 3 different machines. the value at key3 could be changed
   after key2 is read but before key3 is read. But maybe that doesn't matter.
   
- more concerning, queries like
    put(key1, 100), put(key2, 200) 
   are no longer atomic if key1 & key2 map to different nodes. But maybe this is just a documented limitation of the API
   
- TODO: important: furthermore how would this work?
    put(key1, [1,2,3,4,5])
    get(key1).append(6)
    suppose key1 is stored on node100, but the query is handled by node200
    node200 gets key1 from node100 and has a local handle on [1,2,3,4,5]
    node200 appends 6, now the value is [1,2,3,4,5,6]
    but this value is on node200 not node100
    It seems what we actually want to do is forward the whole get(key1).append(6) action to the node that holds key1, but how?
    Maybe we need to figure out all the operations our data structures have in common and make them top level methods instead,
    e.g. 
    get(key1,...).append(6)      becomes    append(key1, ..., 6)
    
    then we'd no longer include the mutable methods in datatypes\listtype, etc. 
    
    this only needs to be done for the mutable methods
    append
    extend
    insert
    setitem
    pop
    popleft
    popright 
    reverse
    sort
    

 

Also look at wackamole (but it doesn't seem to run on Windows(?))
http://www.backhand.org/wackamole/

The tech term to google is "group messaging"

"Broadcast is not what you want. Since there could and probably will be devices attached to this network 
which don't care about your message, you should use Multicast. Unlike broadcast messages, which must be 
sent to and processed by every client on the network, Multicast messages are delivered only to interested 
clients (ie those which have some intention to receive this particular type of message and act on it).
If you later scale this system up so that it needs to be routed over a large network, multicast can scale 
to that, whereas broadcast won't, so you gain a scalability benefit which you might appreciate later. 
Meanwhile you eliminate unnecessary overhead in switches and other devices that don't need to see these 
"something changed" messages."

"Should I be setting any particular configuration for multicast support on
windows host? Are there any ms applications which support multicast and could
be used for testing?
No. To send: just send UDP or raw IP to 224.x.x.x. To receive: assign a
group address (same value of 224.x.x.x) by IP_ADD_MEMBERSHIP, then do the usual
recvfrom() on it."
(Also see http://www.huque.com/software/mctester/)

What about ICE? http://www.zeroc.com/index.html

http://zookeeper.apache.org/doc/r3.4.2/zookeeperOver.html#Implementation
Look for Python bindings

TODO: Master/slave binary star pattern: see here for ZeroMQ
http://zguide.zeromq.org/page:all

TODO: I think the clustering stuff can all be done with ZeroMQ, see some of the advanced patterns in the
guide such as the worked Inter-broker routing example.
Also see A Shared Key-Value Cache (Clone Pattern), this sounds a lot like what REDIS SLAVEOF does

"Here, then, is the Clustered Hashmap Protocol, which "defines a cluster-wide key-value hashmap, and mechanisms 
for sharing this across a set of clients. CHP allows clients to work with subtrees of the hashmap, to update values, 
and to define ephemeral values."
http://rfc.zeromq.org/spec:12


Also could use RabbitMQ or some such to keep track of cluster...


Another idea
- could run recolsvr in PyPy and use the ctypes ZeroMQ bindings (but would need alternative implementations
    of the other data types like bitarray, etc in Python instead of C)
- TODO: do an experiment to see if this is actually faster (?)

# TODO: sqlite performance--look into pragmas
# synchronous - controls fsync
# cache_size
# journal_mode = look into 'wal' mode it seems like it's a good fit for our usage
#   hmmm...but I don't seem to be able to set this.
# http://stackoverflow.com/questions/1711631/how-do-i-improve-the-performance-of-sqlite
# http://web.utk.edu/~jplyon/sqlite/SQLite_optimization_FAQ.html

# TODO: mxBase includes mxBeeBase which is an on disk B+ tree including 
mxBeeBase comes with two readily useable on-disk dictionary implementations: BeeDict and BeeStringDict.
NO: showstopper--win64 not supported 

# TODO: consider TreeDict data structure
http://www.stat.washington.edu/~hoytak/code/treedict/index.html

# TODO: 
consider that for a numpy array stored at <key> a common operation would be
put(<key>, x, y, value) so the database should support 2 subkeys

# TODO: consider geospatial data
and geo-indexing, e.g. somehow index what's close to what
http://en.wikipedia.org/wiki/Geohash
http://pypi.python.org/pypi/Geohash/
http://www.mongodb.org/display/DOCS/Geospatial+Indexing


Consider using HDF5 instead of sqlite, either via HDF5.py or PyTables or some other wrapper (?)
Also see metakit (http://equi4.com/metakit/)
Also see Berkeley DB recno table
"Overall, BerkeleyDB can be extremely fast - I recently designed a built a data analysis platform 
for an employer that was capable of doing 40k inserts per second on an 8 core x86 system (while at 
the same time doing thousands of reads per second) with a dataset in the 30G range. This was with 
full transactional protection." 

NOTE: can find binaries for 2.7 even 64 bit for many packages here
http://www.lfd.uci.edu/~gohlke/pythonlibs/
including iGraph TODO: include iGraph data structures as types in recol?
TODO: include PySparse also seen at http://www.lfd.uci.edu/~gohlke/pythonlibs/? what other cool types?

Stats with some data
loaded 1,100,011 rows in 20.88 secs

# TODO: take a look at the new io module, buffered streams etc.

For sharding/clustering, consider openpgm with zeromq
it's tricky to setup, see here:
lists.zeromq.org/pipermail/zeromq-dev/2011-April/010715.html

TODO:
pyInstaller is cross-platform and very powerful, with many third-party packages (matplotlib, numpy, PyQT4, ...)
specially supported "out of the box", support for eggs, code-signing on Windows (and a couple other Windows-only goodies,
optional binary packing... the works!-) The one big issue: the last "released" version, 1.3, is ages-old -- you
absolutely must install the SVN trunk version, svn co http://svn.pyinstaller.org/trunk pyinstaller (or the 1.4 pre-release,
but I haven't tested that one). A fair summary of its capabilities as of 6 months ago is here (in English, despite the
Italian URL;-).


Linux TODO: use time.time() instead of time.clock()
watch out for divide by zero error

# TODO: using logging instead of print

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
                     value text not null,
                     primary key (key));
CREATE TABLE lastwrite (txid int not null, seekpos int not null);
insert into lastwrite values (0,0);

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


REDIS equivalents (REDIS-RECOL Rosetta Stone)
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
list            list or blist
hash            dict
set             set
sortedset       ?? use sortedset?
bitset          bitarray
                heap/priority queue
                complex
                decimal
                datetime.date
                datetime.time
                datetime.datetime
                deque
                frozenset
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
This means you can query the stored JSON (well, it's stored as Python) directly, which you can't do in REDIS:
    # Store other data as serialized JSON object, but we won't be able to query the value itself
    2 SET "people:francois" "{'name': 'Francois Beausoleil', 'email': 'francois@teksol.info', 'year-of-birth': 1973, 'tags': ['friendly', 'ruby', 'coder', 'father']}"

Control synchronisation via explicit ssync(), hsync() commands
Fewer server roundtrips, e.g. put('key', {"foo":"bar", "baz":"bip}) vs. hset("key", "foo", "bar" then hset "key" "baz" "bip"
Slicing
It doesn't crash (https://github.com/antirez/redis/issues/243)
I can make it run in either embeddable or server mode (TODO: think about API when embedding)

RECOL antipatterns
-------------------------
"""


# TODO: make another module the main entry point to avoid pyc compilation every time
import sys, re, os, zmq, time, ujson, traceback, types, threading, sqlite3, copy, random, math
import tailer
import decimal, datetime, numpy, heapq, bisect, blist, operator
import pydoc
import hashlib

import cStringIO as StringIO
# TODO:? too dangerous? import networkx as nx
import gc
# TODO: add stddev, other stats functions
#from blist import sortedset
from collections import deque
#from Bio.trie import trie
import platform
PLATFORM = platform.system().lower()
WINDOWS = False
LINUX = False
if PLATFORM == "windows":
    WINDOWS = True
    import win32api # TODO:
elif PLATFORM == "linux":
    LINUX = True
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

FSYNC_QUEUE = Queue.Queue(1)
JOURNAL_QUEUE = Queue.Queue()

# TODO: implement __slots__
# TODO rename to ranklist? or index? as this can be used for indexes too
# TODO: if it's going to be used as an index, then we shouldn't limit "score" to be an int, it should be any comparable thing
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
        self.ctr = 0
        self.need_fsync = False

    def process_tx(self, tx):
        ts = str(datetime.datetime.utcnow())
        msg = "".join(["%s|%d|%s|%s|%s|%s\n" % (ts, txid, repr(key), cmd, args, serialize(_unwrap(val))) for txid, key, cmd, args, val in tx])
        self.journal.write(msg)
        self.need_fsync = True
        # TODO: currently we fsync on demand (client calls fsync()) or when the queue is idle, but if queue is 
        # very busy we also need to fsync every n (configurable) seconds, or every m (configurable) writes, call self.fsync()
        self.ctr += 1
        
    def fsync(self):
        if self.need_fsync:
            #log.debug("fsync") # TODO:
            self.journal.flush()
            os.fsync(self.journal.fileno())
            self.need_fsync = False

    def run(self):
        self.journal = open(JOURNALFILE, "ab") # TODO: location
        self.BATCH_SIZE = 1000
        self.ctr = 0
        QSIZE_WARNING_LIMIT = 200 # TODO: get from config
        last_warning_time = time.time() 
        #log.info("controller dbthread starting...")
        log.info("journal writer thread starting...") 
        try:
            while True:
                qsize = JOURNAL_QUEUE.qsize()
                #log.info("QSIZE %s" % qsize) # TODO:
                if (qsize >= QSIZE_WARNING_LIMIT) and (time.time() - last_warning_time > 5):
                    log.warning("journal queue size: %d" % qsize)
                    last_warning_time = time.time()
                try:
                    qitem = JOURNAL_QUEUE.get(True, 2.0) # TODO: make wait time configurable
                except Queue.Empty:
                    log.debug("JOURNAL_QUEUE EMPTY")
                    self.fsync()
                    time.sleep(0.001)
                else:
                    log.debug("JOURNAL_QUEUE POP %s" % (repr(qitem),)) # TODO:
                    # TODO: make sure no real data can be in queue after the quit sentinel
                    if qitem == QUITSENTINEL:
                        log.info("journal writer thread got exit sentinel")
                        return
                    elif qitem == FSYNC_QUEUE:
                        self.fsync()
                        qitem.put(1) # signal main thread
                    else:
                        try: 
                            self.process_tx(qitem)
                        finally:
                            JOURNAL_QUEUE.task_done()
        finally:
            #self.db.commit()
            log.info("journal writer thread stopping")            


# TODO: need to wrap objects in safe proxies, to avoid mutation side effects such as

#reallist = list
#from datatypes.listtype import wrappedlist
#list = wrappedlist

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
            log.info("%.2f %s/sec" % (self.count/elapsed, self.things)) # TODO: logging
            self.count = 1
            self.start = now
        else:
            self.count += 1

from datatypes.wrap import _wrap, _unwrap
import globalvars

GLOBAL_OBJMAP = globalvars.OBJMAP
GLOBAL_ROLLBACKLIST = globalvars.ROLLBACKLIST
GLOBAL_COMMITLIST = globalvars.COMMITLIST
COMMITLIST_APPEND = globalvars.COMMITLIST_APPEND
ROLLBACKLIST_APPEND = globalvars.ROLLBACKLIST_APPEND

GLOBAL_SET_TXID = globalvars.SET_TXID
GLOBAL_GET_TXID = globalvars.GET_TXID
GLOBAL_INCR_TXID = globalvars.INCR_TXID

GLOBAL_SET_TXID(0)

from globalvars import serialize, deserialize
#globals.TXID = 0

D = {}
EXECSTART = 0
VARS = {}
TEMPVAR = "_"
TEMPVAR0= "_0"
TEMPVAR1 = "_1"
TEMPVAR2 = "_2"
EXPIRY_LIST = "_expiry_list"
EXPIRY_XREF = "_expiry_xref"
D[EXPIRY_LIST] = blist.sortedlist() # TODO: work out how this gets initialized, needs to be wrapped for journaling..IMPORTANT!
D[EXPIRY_XREF] = {} # TODO: work out how this gets initialized

TXD = {}

#IDENTITY = lambda x : x
#XFORM_JSON = {} # TODO: {bitarray : lambda x : x.to01(),
#              #set : lambda x : list(x)}

debug_writes = 0

class Preformatted(str):
    pass

def help(what=None):
    global EVAL_LOCALS
    if what==None:
        return EVAL_LOCALS.keys()
    else:
        return Preformatted((getattr(what, "__doc__", None) or "No help available").strip())

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
    raise RuntimeError("forced error")

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

# TODO: get rid of keys, instead support iterkeys, itervalues, iteritems or some such
def keys():
    return sorted(D.keys())

# TODO: do we want to expose this?
def iterkeys():
    return D.iterkeys()

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
        # TODO: catch errors here and just return None
        del D[key][index]
    else:
        try:
            del D[key]
        except KeyError:
            pass
    # TODO: write to journal!!

def exists(key, *idxs):
    try:
        _get(key, *idxs) 
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
    obj = D.get(key, kwargs.get("default", NOTSET))
    if obj==NOTSET:
        raise KeyError(key)
    for idx in idxs:
        obj = obj[idx]
    #obj = WRAPPERS[type(obj)](obj)
    globalvars.OBJMAP[id(obj)] = (key,) + idxs
    return obj

get = _get

def mget(*keys, **kwargs):
    # TODO: currently if just one key is not found the whole operation returns KeyError; would be
    # better if we return the ones we can
    return [(get(key, kwargs=kwargs) if type(key) in STRING_TYPES else \
             get(key[0], *tuple(key[1:]), **kwargs)) for key in keys]

"""
TODO: think about ways we can automatically index objects

e.g. if object {"name" : "Kevin", "age" : 30, "city" : "Seattle"}

is at key /people/1

then have some operation, e.g. index("/people/1") that creates indexes for all attributes
TODO: would be good to also be able to index all people, but this would require searching keysubstrings.

I suppose we could have a convention where at the top level the key must point to a collection of objects, not just one, e.g.
at key "people" we have

[ {"id":100, "name":"Bill", "age":20, "city":"Seattle"},
  {"id":200, "name":"John", "age":25, "city":"New York"},
  {"id":300, "name":"Bob",  "age":20,  "city":"Seattle"}]

(may need to have a convention where there's an ID attribute that is unique)

then we can index("people") and this will create:

at key _index:people:     name:  {"Bill":0}, {"John":1}
at key _index:people:age:   {20:0}, {

TODO: be able to pass lambdas to the indexer to refine how each attribute is indexed

"""

def page(data, pagenum, pagesize=50):
    """
    Example:
    >>> put('x', [[x,x+1,x+2] for x in range(7)])
    {u'cps': 3580.65882, u'res': None}
    >>>
    >>> page(get('x'), 1, 5)
    {u'cps': 7575.42964,
     u'res': {u'data': [[0, 1, 2], [1, 2, 3], [2, 3, 4], [3, 4, 5], [4, 5, 6]],
              u'endidx': 5,
              u'len': 7,
              u'numpages': 2,
              u'page': 1,
              u'pagesize': 5,
              u'startidx': 0}}
    >>> page(get('x'), 2, 5)
    {u'cps': 8079.80539,
     u'res': {u'data': [[5, 6, 7], [6, 7, 8]],
              u'endidx': 10,
              u'len': 7,
              u'numpages': 2,
              u'page': 2,
              u'pagesize': 5,
              u'startidx': 5}}
    """
    # NOTE: pagenum is 1-based, not 0-based
    # NOTE: you can pass pagenum=-1 for the last page, pagenum=-2 for 2nd to last page, etc.
    datalen  = len(data)
    numpages = int(math.ceil(datalen / float(pagesize)))
    if pagenum < 0:
        pagenum = numpages + pagenum + 1
    startidx = ((pagenum-1) * pagesize)
    endidx = startidx + pagesize
    return {"len" : datalen,
            "numpages" : numpages,
            "page" : pagenum,
            "pagesize" : pagesize,
            "startidx" : startidx,
            "endidx" : endidx,
            "data" : data[startidx:endidx]}

def length(key, *idxs, **kwargs):
    return len(_get(key, *idxs, **kwargs))

def copy(key, *idxs, **kwargs):
    return fast_copy(_get(key, *idxs, **kwargs))

def doc(obj):
    return getattr(obj, "__doc__", None)

def kind(key, *idxs):
    return _get(key, *idxs).__class__.__name__

def expirations():
    return D[EXPIRY_LIST] # can't affect db, so don't trigger journaling by using get

def expire(key, when):
    if type(when) == DATETIME_TYPE:
        when = time.mktime(when.timetuple())
    expiry_list = get(EXPIRY_LIST) 
    expiry_xref = get(EXPIRY_XREF) 
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
        expiry_list = get(EXPIRY_LIST) 
        expiry_xref = get(EXPIRY_XREF) 
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

# putnx is not needed, you can simply say 
#    exists('key') or put('key', value)
#def putnx(key, value):
#    # TODO: journaling; only journal if it didn't exist!
#    return D.setdefault(key, value)

#from datatypes.wraptype import _wrap
# TODO: raise exception if more than key + 8 subkeys, as this won't fit in current db schema
def _put(key, *args, **kwargs):
    idxs = args[:-1]
    val = args[-1]
    val = _wrap(val)
    if idxs:
        obj = D[key]
        for idx in idxs[:-1]:
            obj = obj[idx]
        globalvars.OBJMAP[id(obj)] = (key,) + idxs[:-1] # TODO: idxs[:-1] repeated, idxs[-1] repeated, could optimize
        try:
            prev = obj[idxs[-1]]
        except KeyError:
            prev = None
        obj[idxs[-1]] = val
    else:
        try:
            prev = D[key]
        except KeyError:
            prev = None # TODO: no, this should be some kind of sentintel (TOMBSTONE) that means to delete the item during rollback
        D[key] = val
    expiry = kwargs.get("expiry")
    if expiry:
        expire(key, expiry)
    else:
        pass # TODO: if key previously existed and was set to expire, but we don't specify expire here,
        # then need to clear the expiry
    global debug_writes
    debug_writes += 1 # TODO:
    #return (key,)+idxs, prev, val
    return prev, val

def put(key, *args, **kwargs):
    """
    This is the docstring for put TODO:
    """
    #fullkey, prev, val = _put(key, *args, **kwargs)
    prev, val = _put(key, *args, **kwargs)
    # NOTE: put only needs to add to commitlist/rollbacklist if we have subkeys, otherwise we rely on the 
    # implementation of obj.__setitem__ to do it
    if len(args) == 1:
        # TODO: if prev==TOMBSTONE, then rollback should DEL the key, not set to TOMBSTONE
        ROLLBACKLIST_APPEND(_put, *(key, prev)) # TODO: what about rolling back key that had expiry?
        COMMITLIST_APPEND((key,), "PUT", None, val) 

def incr(key, *args, **kwargs):
    value = get(key, *args, **kwargs) + kwargs.get("by",1)
    put(key, *(args+(value,)))
    return value

# TODO: def sync(hard=0): pass

def info():
    # TODO: uptime, txid, journal queue length, other useful info, length of prepared expression cache
    # TODO: would it be better just to have individual functions for all these things?
    return {"pid" : os.getpid(),
            "num_keys" : len(D)}
    
def profile(res):
    secs = (time.clock() - EXECSTART)
    msecs = secs * 1000.0
    tps = 1.0 / secs
    return {"res" : res, "msecs" : msecs, "tps" : tps}

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

FSYNCFLAG = 0   
def fsync(_=None):
    global FSYNCFLAG
    FSYNCFLAG = 1
    return _
    
PREPARED_QUERIES = {}

# TODO: need to prevent PREPARED_QUERIES from growing without bound
#def prepare(query, key=None):
#    if key==None:
#        key = hashlib.sha256(query).hexdigest()
#    PREPARED_QUERIES[key] = compile(query, "<string>", "eval")
#    return key

def shutdown():
    globalvars.SERVER.exit_requested = True

from datatypes.listtype import wrappedlist
from datatypes.dicttype import wrappeddict

ALLOWED_BUILTINS = {
    'abs':abs,
    'all':all,
    'any':any,
    #'apply':
    'basestring':basestring,
    'bin':bin,
    'bool':bool,
    #'buffer':buffer, # TODO: support this?
    #'bytearray':bytearray, # TODO:support this?
    'bytes':bytes,
    'callable':callable,
    'chr':chr,
    #'classmethod':
    'cmp':cmp,
    'coerce':coerce,
    #'compile':
    'complex':complex,
    #'copyright':
    #'credits':
    #'delattr':
    #'dict':wrappeddict,
    'dir':dir,
    'divmod':divmod,
    'enumerate':enumerate,
    'False':False,
    #'eval':
    #'execfile':
    #'exit':
    #'file':
    'filter':filter,
    'float':float,
    'format':format,
    'frozenset':frozenset,
    #'getattr':
    #'globals':
    'hasattr':hasattr,
    'hash':hash,
    #'help':help,
    'hex':hex,
    'id':id,
    #'input':
    'int':int,
    'intern':intern,
    'isinstance':isinstance,
    'issubclass':issubclass,
    'iter':iter,
    'len':len,
    #'license':
    'list':wrappedlist,
    #'locals':
    'long':long,
    'map':map,
    'max':max,
    #'memoryview':
    'min':min,
    'next':next,
    'None':None,
    #'object':
    'oct':oct,
    #'open':
    'ord':ord,
    'pow':pow,
    #'print':
    #'property':
    #'quit':
    'range':range,
    #'raw_input':
    'reduce':reduce,
    #'reload':
    'repr':repr,
    'reversed':reversed,
    'round':round,
    'set':set,
    #'setattr':
    #'slice':
    'sorted':sorted,
    #'staticmethod':
    'str':str,
    'sum':sum,
    #'super':
    'True' : True,
    'tuple':tuple,
    'type':type,
    'unichr':unichr,
    'unicode':unicode,
    #'vars':
    'xrange':xrange,
    'zip':zip
}

# TODO: put back some globals e.g. id, abs, ...
EVAL_GLOBALS = {
    "__builtins__" : ALLOWED_BUILTINS
}


#make a list of safe functions
EVAL_LOCALS = { # modules
                "datetime":datetime, # TODO: what's the difference having this in locals vs. globals?
                #"list" : list,
                "math" : math,
                "random" : random,
                # commands
                "copy" : copy,
                "crash" : crash,
                "decr" : decr,
                "doc" : doc,
                "dump" : dump,
                "echo" : echo,
                "erase" : erase,
                "exists" : exists,
                "expire" : expire,
                "expirations" : expirations,
                "fsync" : fsync,
                "get" : get,
                "help" : help,
                "incr" : incr,
                "info" : info,
                "iterkeys" : iterkeys,
                "j2p" : j2p,
                "keys" : keys,
                "kind" : kind,
                "length" : length,
                "mget" : mget,
                "nop" : nop,
                "p2j" : p2j,
                "page" : page,
                "persist" : persist,
                "ping" : ping,
                #"prepare" : prepare,
                "profile" : profile,
                "put" : put,
                "randkey" : randkey,
                "rename" : rename,
                "shutdown" : shutdown,
                "ttl" : ttl,
                }

def _tryint(i):
    try:
        return int(i)
    except:
        return i

class Server:
    def __init__(self):
        self.D = {}
        self.exit_requested = False
        self.journal_writer_thread = JournalWriterThread()
        self.zmq_context = zmq.Context()
        self.db = sqlite3.connect("recollection.dat") # TODO:
        self.curs = self.db.cursor()

    def console_ctrl_handler(self, event):
        """
        Return True if we handled the event, False to continue
        passing event to upstream handlers
        """
        log.info("console event %s (%s)" % (CONSOLE_EVENTS.get(event, "UNKNOWN"), event))
        self.exit_requested = True
        self.zmq_context.term()
        return True

    def get_last_txid(self):
        # TODO: need to think about finding the last COMMITTED tx. What if log is corrupted? What if we find
        # an uncommitted tx?
        log.info("searching %s for latest txid" % JOURNALFILE)
        try:
            fp = open(JOURNALFILE, "rb")
        except IOError, e:
            log.warning(str(e))
            GLOBAL_SET_TXID(0)
        else:
            try:
                try:
                    line = tailer.tail(fp, 1)[0]
                except IndexError:
                    GLOBAL_SET_TXID(0)
                else:
                    parts = line.split(":")
                    GLOBAL_SET_TXID(long(parts[0]))
            finally:
                fp.close()
        finally:
            log.info("last txid from journal was %s" % GLOBAL_GET_TXID())

    def wait_for_db(self):
        while not self.exit_requested:
            dbtxid = self.curs.execute("select txid from lastwrite;").fetchone()[0]
            if dbtxid == GLOBAL_GET_TXID():
                log.info("db is in sync with journal")
                break
            elif dbtxid > GLOBAL_GET_TXID():
                log.critical("internal error: db txid %s > journal txid %s" % (dbtxid, GLOBAL_GET_TXID()))
                sys.exit(1)
            else:
                log.info("last db txid is %s, last journal txid is %s (%s txs behind)" % (dbtxid, GLOBAL_GET_TXID(), GLOBAL_GET_TXID()-dbtxid))
                log.info("waiting for db to catch up...(please ensure db writer is running)")
                time.sleep(2.0)

    def load_from_db(self):
        self.curs.execute("select * from master order by key;")
        # TODO: need lazy iterator fetch here
        rowcount = 0
        start = time.time()
        print "start of fetchall..." # TODO:
        rows = self.curs.fetchall() # TODO: replace with lazy/fetchmany thing
        print "end of fetchall..." # TODO:
        for row in rows:
            rowcount += 1
            key, val = row
            val = deserialize(val)
            try:
                _put(key, val)
            except Exception:
                log.exception("failed to deserialize row: %s" % repr(row))
            #print row
        log.info("loaded %d rows in %.2f secs" % (rowcount, time.time()-start))

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
        globalvars.SERVER = self
        log.info("pid: %s" % os.getpid())

        # TODO: windows specific
        if WINDOWS:
            win32api.SetConsoleCtrlHandler(self.console_ctrl_handler, 1)

        self.get_last_txid()

        self.wait_for_db()
        if self.exit_requested:
            return

        self.load_from_db()

        self.journal_writer_thread.start()

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
            now = time.time()
            global VARS, EXECSTART, FSYNCFLAG
            VARS = {}
            EXECSTART = time.clock()
            FSYNCFLAG = 0
            #execstart = time.clock()
            self.do_expiry(now)
            GLOBAL_INCR_TXID()
            
            GLOBAL_OBJMAP.clear()
            GLOBAL_COMMITLIST.__imul__(0) # this is the only way I found to clear a list in place
            GLOBAL_ROLLBACKLIST.__imul__(0) # TODO: get rid of dotted qualifiers
            try:
                #if command[0] == "*": # dereference operator, get it? TODO: is startswith faster?
                #    command = PREPARED_QUERIES[command[1:]]
                # TODO: if length of command is too long, maybe don't use it directly as key? 
                #  needs more thought...maybe go back to the explicit prepare(...) function. Another
                #  advantage of explicit prep is user can specify a short key for a long query so less
                #  net traffic
                compiled = PREPARED_QUERIES.get(command)
                if compiled is None:
                    compiled = compile(command, "<string>", "eval")
                    PREPARED_QUERIES[command] = compiled
                result = eval(compiled, EVAL_GLOBALS, EVAL_LOCALS)
                #elapsed = (time.clock() - execstart) or 0.0000000001 # TODO:
                result = ujson.dumps(result) # TODO: don't need Preformatted any more
                #if type(result) == Preformatted:
                    #result = ujson.dumps(result)
                #else:
                    #result = ujson.dumps({"res":result, "cps": 1. / elapsed})
            except Exception, e:
                # TODO: handle error during rollback--log which keys may be out of sync but keep going
                if GLOBAL_ROLLBACKLIST:
                    log.info("START ROLLBACK:") # TODO:
                    for item in reversed(GLOBAL_ROLLBACKLIST):
                        log.info(item)
                        func, args = item
                        func(*args)
                    log.info("END ROLLBACK:") # TODO:
                #traceback.print_exc()
                result = ujson_dumps({"err" : "%s: %s" % (e.__class__.__name__, str(e))})
            else:
                if GLOBAL_COMMITLIST:
                    COMMITLIST_APPEND("", "COMMIT")
                    log.debug("PUSH %s" % repr(GLOBAL_COMMITLIST)) # TODO:
                    JOURNAL_QUEUE.put(GLOBAL_COMMITLIST[::]) # TODO: NOTE: shallow copy; is that right?
                # NOTE: the following should NOT be indented under if GLOBAL_COMMITLIST, because an fsync()
                # may appear as a command by itself, in which case we still want it to have an effect 
                if FSYNCFLAG:
                    JOURNAL_QUEUE.put(FSYNC_QUEUE)
                    FSYNC_QUEUE.get() # block until journal thread does the fsync
                    FSYNC_QUEUE.task_done()
            finally:
                elapsed = now - start
                if elapsed > 2: # TODO: parameterize
                    log.info("%.2f req/sec @ txid %s" % (reqs/elapsed, GLOBAL_GET_TXID()))
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
        JOURNAL_QUEUE.put(QUITSENTINEL)            
        log.info("waiting for journal writer thread to catch up")
        self.journal_writer_thread.join()
        log.info("did %d writes, verify journal contains same number!" % debug_writes) # TODO: remove

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
