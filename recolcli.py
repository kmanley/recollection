import zmq, time, ujson, pprint
import cStringIO as StringIO
STRINGTYPES = (str, unicode)

class Client(object):
    def __init__(self, hostname="127.0.0.1", port=5555):
        self.hostname = hostname
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect ("tcp://%s:%s" % (self.hostname, self.port))
        
    def query(self, cmd):
        self.socket.send(cmd)
        result = ujson.loads(self.socket.recv())
        return result
    
    def _val(self, s):
        if type(s) in STRINGTYPES:
            return "'%s'" % s
        else:
            return str(s)
    
    def get(self, key, *idxs, **kwargs):
        cmd = StringIO.StringIO()
        cmd.write("get('%s'" % key)
        for idx in idxs:
            cmd.write(",%s" % self._val(idx))                
        for key in kwargs:
            cmd.write(",%s=%s" % (key, self._val(kwargs[key])))
        cmd.write(")")
        print cmd.getvalue()
        return self.query(cmd.getvalue())
