"""
NOTE: adapted from Michael Thornton's pytailer
http://code.google.com/p/pytailer/
http://pypi.python.org/pypi/tailer/0.2.1
"""

# $Id: __init__.py 3 2008-01-29 18:39:09Z msthornton $

import re
import time

class Idle:
    pass

class Tailer(object):
    """\
    Implements tailing and heading functionality like GNU tail and head
    commands.
    """
    line_terminators = ('\n',)

    def __init__(self, file):
        self.file = file

    def seek(self, pos, whence=0):
        self.file.seek(pos, whence)

    def follow(self):
        """\
        Iterator generator that returns lines as data is added to the file.

        Based on: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/157035
        """
        while 1:
            where = self.file.tell()
            line = self.file.readline()
            if line:
                if line.endswith("\n"):
                    yield line
                else:
                    # partial line...this sometimes happens when we're at the end of the file
                    self.seek(where)
            else:
                self.seek(where)
                yield Idle

    def __iter__(self):
        return self.follow()

    def close(self):
        self.file.close()


