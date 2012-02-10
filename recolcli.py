import zmq, time, ujson, pprint
#import cStringIO as StringIO
import StringIO

class CmdBuilder(object):
    def __init__(self, s):
        #print "__init__: %s" % s
        self.buff = StringIO.StringIO()
        self.buff.write(s)
        
    def __getitem__(self, *args):
        #print "got here! %s" % type(self.buff)
        self.buff.write("[" + ", ".join([repr(arg) for arg in args]) + "]")
        return CmdBuilder(self.buff.getvalue())

    def __call__(self, *args, **kwargs):
        #print "got here! %s" % type(self.buff)
        self.buff.write("(" + ", ".join([repr(arg) for arg in args]))
        if kwargs:
            self.buff.write(",")
            self.buff.write(",".join(["%s=%s" % (k,repr(v)) for k,v in kwargs.items()]))
        self.buff.write(")")
        return CmdBuilder(self.buff.getvalue())
    
    def __repr__(self):
        return self.buff.getvalue()


class Client(object):
    def __init__(self, hostname="127.0.0.1", port=5555):
        self.hostname = hostname
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect ("tcp://%s:%s" % (self.hostname, self.port))
        
    def query(self, cmd):
        #print "query: %s" % cmd # TODO:
        self.socket.send(cmd)
        result = ujson.loads(self.socket.recv())
        return result
    
    def __call__(self, *args):
        if len(args) == 1:
            cmd = repr(args[0])
        else:
            cmd = repr(args)
        return self.query(cmd)
    
    def __getattribute__(self, name):
        if name in set(["hostname", "port", "context", "socket", "query"]): # TODO: precompute set
            return object.__getattribute__(self, name)
        buff = StringIO.StringIO()
        buff.write(name)
        return CmdBuilder(buff.getvalue())
    

#def unittest():
#    c = Client("grappa")
#    c.query(c.put("foo", [1,2,3]), c.profile(c.get("foo")))
    
    
    