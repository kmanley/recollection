import os, sqlite3, time, tailer
import binascii
unhexlify = binascii.unhexlify
import cPickle as pickle
pickle_loads = pickle.loads

def deserialize(s):
    return pickle_loads(unhexlify(s))

JOURNALFILE = r"c:\temp\journal.txt"

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

class DBWriter(object):
    def __init__(self):
        self.ctr = XPerSec("txs")
        self.conn = sqlite3.connect("recollection.dat")
        self.curs = self.conn.cursor()
        self.txid = 0
        self.seekpos = 0
        self.exitrequested = False
        self.last_commit = time.time()
        self.sync()

    def sync(self):
        result = self.query("select txid, seekpos from lastwrite;")[0]
        self.txid = result[0]
        self.seekpos = result[1]
        print "last txid: %d, last seekpos: %d" % (self.txid, self.seekpos)

    def query(self, sql):
        self.curs.execute(sql)
        return self.curs.fetchall()

    def query1(self, sql):
        self.curs.execute(sql)
        return self.curs.fetchall()[0][0]

    def commit(self):
        print "commit"
        self.conn.commit()

    def process_line(self, line):
        #print line
        txid, key, cmd, data = line.split(":")
        self.txid = long(txid)
        key = eval(key)
        keylen = len(key)
        #data = deserialize(data)
        # TODO: use map of functions to dispatch the following
        if cmd == "COMMIT":
            # TODO: when server is idle the last tx written to journal will not be committed--needs a fix
            #self.commit() # TODO: need to batch up; too slow
            now = time.time()
            if (now - self.last_commit) > 5.0: # TODO: hardcoded
                self.commit()
                self.last_commit = now
            self.ctr.incr()
        elif cmd == "SET":
            if keylen == 1:
                self.curs.execute("delete from master where key=? and i0 is not null", (key[0],))
                self.curs.execute("replace into master (key, value) values (?,?)", (key[0], data))
            elif keylen == 2:
                self.curs.execute("delete from master where key=? and i1 is not null", (key[0],))
                self.curs.execute("replace into master (key, i0, value) values (?,?, ?)", (key[0], key[1], data))
            else:
                # TODO:
                raise NotImplementedError()
        #self.curs.execute("update lastwrite set txid=?, seekpos=?", ())

    def run(self):
        while True:
            if not os.path.exists(JOURNALFILE):
                print "waiting for %s to appear..." % JOURNALFILE
                time.sleep(2.0)
            else:
                break
        with open(JOURNALFILE, "rb") as fp:
            fp.seek(self.seekpos)
            t = tailer.Tailer(fp, end=False)
            while not self.exitrequested:
                try:
                    for line in t.follow():
                        self.process_line(line)
                except KeyboardInterrupt:
                    self.exitrequested = True # TODO: use consolectrlhandler instead
        self.commit()

        #counter = XPerSec("writes")
        #ctr = 0
        #while True:
        #    #print "inserting %s" % ctr
        #    curs.execute("insert into journal values (null, '%s')" % ("command_%s" % ctr))
        #    counter.incr()
        #    if ctr % batchsize == 0:
        #        conn.commit()
        #    ctr += 1

if __name__ == "__main__":
    w = DBWriter()
    w.run()




