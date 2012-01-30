import sys, recolcli, time, random
ITERS = 200000

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

hits = 0
misses = 0
ctr = XPerSec("reqs")
c = recolcli.Client("grappa", 5555)
for i in range(0, ITERS):
    if i % 1000 == 0:
        print "%d of %d (%d hits, %d misses)" % (i, ITERS, hits, misses)

    if i % 2 == 0:
        #q = "fsync(put('%s', %s))" % (i, i)
        q = "put('%s', %s)" % (i, i)
        #print "query: %s" % q
        result = c.query(q)
        #print "result: %s" % repr(result)
        if type(result) == dict:
            print result
            sys.exit(1) 
    #    if random.randrange(100) > 1000: # < 10:
    #        expiry = time.time() + 2
    #        c.query("put('%s', %s, expiry=%s)" % (i, i, expiry))
    #    else:
    #        c.query("put('%s', %s)" % (i, i))
    else:
        result = c.query("get('%s')" % (i-1))
        if type(result) == dict:
            misses += 1
            print result
            sys.exit(1)
        else:
            hits += 1
    #if i % 100 == 0:
    #    expiry = time.time() + 2
    #    c.query("put('%s', %s, expiry=%s)" % (i, i, expiry))
    #else:
    #    c.query("put('%s', %s)" % (i, i))

    ctr.incr()

#c.query("get('%s')" % i)

