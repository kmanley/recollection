import os, sys, tempfile, subprocess
sys.path.append("..")
import recolcli

subproc = None
class GARBAGE: pass

# TODO: when we have more tests, this setup/teardown should only happen once.
# Or, maybe not. Maybe each test should have its own pristine "environment"
def start_server():
    stdin = tempfile.TemporaryFile()
    stdout = tempfile.TemporaryFile()
    os.chdir("..")
    global subproc
    subproc = subprocess.Popen("run.cmd", shell=False, stdout=stdout, stderr=stdout, stdin=stdin)
    os.chdir(".\\test")
    print "subproc pid: %s" % subproc.pid

def wait_for_shutdown():
    print "waiting..."
    subproc.wait()
    print "done waiting"

class RecolTester(object):
    def __init__(self):
        self._c = recolcli.Client()

    def setup(self):
        # make sure each test starts without the x key
        self.query("erase('x')")

    def query(self, q):
        result = self._c.query(q)
        if result.has_key("err"):
            raise AssertionError("got unexpected error %s from query %s" % (result, q))
        return result

    def asserteq(self, query, expected):
        r = self._c.query(query)
        if r.get("res", GARBAGE) != expected:
            raise AssertionError("in %s, expected res='%s'" % (r, expected))

    def asserterr(self, query, expected):
        assert type(expected) in (str, unicode)
        assert len(expected) > 0
        r = self._c.query(query)
        if r.get("err", "").find(expected) < 0:
            raise AssertionError("in %s, expected err containing '%s'" % (r, expected))

