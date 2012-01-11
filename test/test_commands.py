import os, sys, tempfile
import subprocess
from recoltest import RecolTester, start_server, wait_for_shutdown

setup = start_server
teardown = wait_for_shutdown

class TestCommands(RecolTester):
    # TODO: put the test_put methods in the individual datatype tests instead
    """
    "copy" : copy,
    "decr" : decr,
    "doc" : doc,
    "dump" : dump,
    "echo" : echo,
    "erase" : erase,
    "exists" : exists,
    "get" : get,
    "incr" : incr,
    "j2p" : j2p,
    "kind" : kind,
    "length" : length,
    "nop" : nop,
    "page" : page,
    "p2j" : p2j,
    "ping" : ping,
    """

    def test_incr(self):
        self.query("put('x', [1,2,3])")
        self.asserterr("incr('x')", "TypeError")
        self.query("incr('x', 0)")
        self.asserteq("get('x')", [2,2,3])

    def test_multiple_commands(self):
        pass

    def test_crash(self):
        self.asserterr("crash()", "forced error")

    # TODO: test rollback


    def testlast_shutdown(self):
        self.asserteq("shutdown()", None)
