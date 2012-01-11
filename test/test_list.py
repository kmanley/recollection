import os, sys, tempfile
import subprocess
from recoltest import RecolTester, start_server, wait_for_shutdown

setup = start_server
teardown = wait_for_shutdown

class TestList(RecolTester):
    def test_put(self):
        self.query("put('x', range(5))")
        self.asserteq("get('x')", range(5))
        self.asserteq("get('x', 0)", 0)
        self.asserteq("get('x', -1)", 4)

    def test_put2(self):
        data = [1,2,3, [100, 200, [1000, 2000, 3000]]]
        self.query("put('x', %s)" % repr(data))
        self.asserteq("get('x')", data)
        self.asserteq("get('x', 2)", 3)
        self.asserteq("get('x', 3, 0)", 100)
        self.asserteq("get('x', 3, 2, 1)", 2000)

    def test_kind(self):
        self.query("put('x', [1,2,3])")
        self.asserteq("kind('x')", "wrappedlist")

    def test_kind2(self):
        self.query("put('x', list([1,2,3]))")
        self.asserteq("kind('x')", "wrappedlist")

    def test_add(self):
        self.query("put('x', [0,1,2,3,4])")
        self.asserteq("get('x')+[10]", [0,1,2,3,4,10])

    def test_contains(self):
        self.query("put('x', range(10))")
        self.asserteq("3 in get('x')", True)
        self.asserteq("3 not in get('x')", False)
        self.asserteq("30 in get('x')", False)
        self.asserteq("30 not in get('x')", True)

    # TODO: need to address __delattr__
    #def test_delattr(self):
    #    self.asserteq("get('x').__delattr__('foo')", False)

    def test_delitem(self):
        self.query("put('x', [1,2,3])")
        self.asserterr("get('x').__delitem__(1)", "NotImplementedError")

    def test_delslice(self):
        self.query("put('x', [1,2,3])")
        self.asserterr("get('x').__delslice__(1,3)", "NotImplementedError")

    def test_eq(self):
        self.query("put('x', [1,2,3])")
        self.asserteq("get('x') == [1,2,3]", True)

    def test_ge(self):
        self.query("put('x', [1,2,3])")
        self.asserteq("get('x') >= [1,2,3]", True)
        self.asserteq("get('x') >= [1,2,2]", True)
        self.asserteq("get('x') >= [1,2,4]", False)

    #def test_getattribute(self):
    #    pass #TODO:

    def test_getitem(self):
        self.query("put('x', [1,2,3])")
        self.asserteq("get('x', 1)", 2)
        self.asserteq("get('x')[1]", 2)
        self.asserteq("get('x').__getitem__(1)", 2)

    def test_getslice(self):
        self.query("put('x', [1,2,3])")
        self.asserteq("get('x', 1)", 2)
        self.asserteq("get('x')[1]", 2)
        self.asserteq("get('x').__getitem__(1)", 2)

    def test_gt(self):
        self.query("put('x', [1,2,3])")
        self.asserteq("get('x') > [1,2,3]", False)
        self.asserteq("get('x') > [1,2,2]", True)
        self.asserteq("get('x') > [1,2,4]", False)

    def test_iadd(self):
        self.query("put('x', [1,2,3])")
        self.asserterr("get('x') += [4]", "SyntaxError")
        self.asserterr("get('x').__iadd__([4])", "NotImplementedError")

    def test_imul(self):
        self.query("put('x', [1,2,3])")
        self.asserterr("get('x') *= 2", "SyntaxError")
        self.asserterr("get('x').__imul__(2)", "NotImplementedError")

    def test_iter(self):
        self.query("put('x', range(10))")
        self.asserteq("[x for x in iter(get('x'))]", range(10))

    def test_le(self):
        self.query("put('x', [1,2,3,4,5])")
        self.asserteq("get('x') <= [1,2,3,4,5]", True)
        self.asserteq("get('x') <= [1,2,3,5,6]", True)
        self.asserteq("get('x') <= [1,2,2,2,2]", False)

    # TODO: all the other list methods

    def test_ne(self):
        self.query("put('x', [1,2,3])")
        self.asserteq("get('x') != [1,2,3,4,5]", True)

    def testlast_shutdown(self):
        self.asserteq("shutdown()", None)
