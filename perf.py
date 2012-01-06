import recolcli, time, random
ITERS = 50000000

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

ctr = XPerSec("reqs")
c = recolcli.Client("127.0.0.1", 5555)
for i in range(ITERS):
    if i % 1000 == 0:
      print "%d of %d" % (i, ITERS)
    if random.randrange(100) > 50:
        if random.randrange(100) > 1000: # < 10:
            expiry = time.time() + 2
            c.query("put('%s', %s, expiry=%s)" % (i, i, expiry))
        else:
            c.query("put('%s', %s)" % (i, i))
    else:
        c.query("get('%s')" % i)
    #if i % 100 == 0:
    #    expiry = time.time() + 2
    #    c.query("put('%s', %s, expiry=%s)" % (i, i, expiry))
    #else:
    #    c.query("put('%s', %s)" % (i, i))

    ctr.incr()

  #c.query("get('%s')" % i)

