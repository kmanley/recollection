import lib.tailer as tailer
import os, sqlite3, time
import binascii
unhexlify = binascii.unhexlify
hexlify = binascii.hexlify
import cPickle as pickle
pickle_loads = pickle.loads
pickle_dumps = pickle.dumps

import logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.INFO) # TODO: configuration

import platform
PLATFORM = platform.system().lower()
WINDOWS = False
LINUX = False
if PLATFORM == "windows":
    WINDOWS = True
    import win32api # TODO:
elif PLATFORM == "linux":
    LINUX = True

CONSOLE_EVENTS = {0 : "CTRL_C_EVENT",
                  1 : "CTRL_BREAK_EVENT",
                  2 : "CTRL_CLOSE_EVENT",
                  5 : "CTRL_LOGOFF_EVENT",
                  6 : "CTRL_SHUTDOWN_EVENT"}

def deserialize(s):
    return pickle_loads(unhexlify(s))

def serialize(o):
    return hexlify(pickle_dumps(o))

JOURNALFILE = r"c:\temp\journal.txt" # TODO: config

class XPerSec:
    def __init__(self, things):
        self.things = things
        self.start = time.clock()
        self.count = 0

    def incr(self):
        now = time.clock()
        elapsed = now - self.start
        if elapsed > 1.0:
            log.info("%.2f %s/sec" % (self.count/elapsed, self.things))
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
        self.last_commit_time = 0
        self.exit_requested = False
        self.last_committed_txid = -1
        self.sync()
        
    def console_ctrl_handler(self, event):
        """
        Return True if we handled the event, False to continue
        passing event to upstream handlers
        """
        log.info("console event %s (%s)" % (CONSOLE_EVENTS.get(event, "UNKNOWN"), event))
        self.exit_requested = True
        return True

    def sync(self):
        result = self.query("select txid, seekpos from lastwrite;")[0]
        self.txid = result[0]
        self.seekpos = result[1]
        log.info("last txid: %d, last seekpos: %d" % (self.txid, self.seekpos))

    def query(self, sql):
        self.curs.execute(sql)
        return self.curs.fetchall()

    def query1(self, sql):
        self.curs.execute(sql)
        return self.curs.fetchall()[0][0]

    def commit(self, txid, seekpos):
        log.info("db commit at txid %s, seekpos %s" % (txid, seekpos))
        self.curs.execute("update lastwrite set txid=?, seekpos=?", (txid, seekpos))
        self.conn.commit()
        self.last_committed_txid = txid
        
    def get_obj_from_db(self, key):
        # TODO: introduce an in-memory cache
        sql = "select value from master where key=? "
        ands = " and ".join("i%s=?" % x for x in range(len(key[1:])))
        if ands:
            sql = sql + " and " + ands 
        log.info(sql) # TODO:
        serialized = self.curs.execute(sql, key).fetchall()
        assert len(serialized) == 1 # TODO:
        serialized = serialized[0][0]
        obj = deserialize(serialized)
        return obj
    
    def put_obj_in_db(self, key, value):
        keylen = len(key)
        #qmarks = ",".join("?" * (keylen+1))
        #sidxs = ",".join(["i%s" % x for x in range(keylen-1)])
        #if sidxs:
        #    sidxs = sidxs + ","
        #sql = "insert into master (key, %s value) values (%s)" % (sidxs, qmarks)
        
        #where = 
        #sql = "update master set value=? where key=? and i0=? and i1=? and i2=? and i3=? and i4=? and i5=? and i6=? and i7=?"
        
        
        sql = "replace into master (key, i0, i1, i2, i3, i4, i5, i6, i7, value) values (?,?,?,?,?,?,?,?,?,?)"
        params = key + (('',) * (9-keylen)) + (value,)
        
        #params = key + (value,)
        #params = key + ((None,)*9) + (value,)  
        log.info("%s, %s" % (sql, repr(params))) # TODO: 
        result = self.curs.execute(sql, params)
        print "rowcount: %s" % self.curs.rowcount
        #if self.curs.rowcount == 0:
        #    sql = "insert into master (value, key, i0, i1, i2, i3, i4, i5, i6, i7) values (?,?,?,?,?,?,?,?,?,?)"
        #log.info("%s, %s" % (sql, repr(params))) # TODO: 
        #result = self.curs.execute(sql, params)
        #print "rowcount: %s" % self.curs.rowcount
        
    def handle_APPENDR(self, fp, txid, key, idx, value):
        value = deserialize(value)
        obj = self.get_obj_from_db(key)
        obj.append(value)
        self.put_obj_in_db(key, serialize(obj))
        
    def handle_SETITEM(self, fp, txid, key, idx, value):
        idx = int(idx)
        #value = deserialize(value)
        # NOTE: no, we don't want to reconstitute the original object 
        #obj = self.get_obj_from_db(key)
        #obj[idx] = value
        #self.put_obj_in_db(key, serialize(obj))
        self.handle_PUT(fp, txid, key + (idx,), None, value)

    def handle_COMMIT(self, fp, txid, key, idx, value):
        self.txid = txid
        log.debug("commit %s" % txid)
        now = time.time()
        if (now - self.last_commit_time) > 5.0: # TODO: hardcoded
            self.commit(self.txid, fp.tell())
            self.last_commit = now
        self.ctr.incr()
        
    def handle_PUT(self, fp, txid, key, idx, value):
        keylen = len(key)
        # delete anything specified at a more granular level since we are overwriting
        sql = "delete from master where key=? and i%s <> ''" % (keylen-1)
        log.info(sql) # TODO:
        self.curs.execute(sql, (key[0],))
        self.put_obj_in_db(key, value)
        

    def process_line(self, fp, line):
        log.info(line) # TODO:
        txid, key, cmd, idx, value = line.split(":")
        self.txid = long(txid)
        key = eval(key)
        func = getattr(self, "handle_%s" % cmd)
        func(fp, txid, key, idx, value)

    def wait_for_journal(self):
        while True:
            if not os.path.exists(JOURNALFILE):
                log.info("waiting for %s to appear..." % JOURNALFILE)
                time.sleep(2.0)
            else:
                break

    def run(self):
        log.info("pid: %s" % os.getpid())
        
        try:
            self.wait_for_journal()
        except KeyboardInterrupt:
            return
        
        # TODO: windows specific
        if WINDOWS:
            win32api.SetConsoleCtrlHandler(self.console_ctrl_handler, 1)
            
        with open(JOURNALFILE, "rb") as fp:
            fp.seek(self.seekpos)
            t = tailer.Tailer(fp, end=False)
            for line in t.follow():
                if self.exit_requested:
                    break
                
                if line == tailer.Idle:
                    if self.last_committed_txid != self.txid:
                        self.commit(self.txid, fp.tell())
                    time.sleep(1.0) # TODO: config
                else:
                    self.process_line(fp, line.strip())

        log.info("exiting")


if __name__ == "__main__":
    w = DBWriter()
    w.run()




