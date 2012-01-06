import recolcli
import pythoncom

def create_guid():
    return str(pythoncom.CreateGuid())[1:-1]

class mapper(object):
    """
    Example session:

    Python 2.7.2 (default, Jun 12 2011, 14:24:46) [MSC v.1500 64 bit (AMD64)] on win32
    Type "help", "copyright", "credits" or "license" for more information.
    >>>
    >>> from mapper import *
    >>> m = mapper()
    >>> m.get_guid("bridget", 12345)
    '7557E692-BA26-498A-9A1F-B161A400CDF4'

    >>> m.get_luid("bridget", '7557E692-BA26-498A-9A1F-B161A400CDF4')
    12345

    >>> m.map_luid_to_guid("front_arena", 100, '7557E692-BA26-498A-9A1F-B161A400CDF4')
    >>> m.get_guid("front_arena", 100)
    u'7557E692-BA26-498A-9A1F-B161A400CDF4'

    >>> m.get_luid("front_arena", '7557E692-BA26-498A-9A1F-B161A400CDF4')
    100

    >>> m.get_luids('7557E692-BA26-498A-9A1F-B161A400CDF4')
    {u'bridget': 12345, u'front_arena': 100}

    >>> m.get_subsystems()
    [u'front_arena', u'bridget']

    """
    def __init__(self, hostname="grappa", port=5555):
        self.db = recolcli.Client(hostname, port)

    def get_subsystems(self):
        """
        Returns a list of all subsystems
        """
        result = self.db.query("list(get('subsystems'))")
        return result

    def get_guid(self, subsystem, luid):
        """
        Given a subsystem and luid, return the guid.
        If the guid doesn't exist, create it first.
        """
        result = self.db.query("get('luid:%s:%s')" % (subsystem, luid))
        if result is None:
            # doesn't exist, create it
            guid = create_guid()
            self.db.query("put('guid:%s', {})" % guid)
            self.map_luid_to_guid(subsystem, luid, guid)
            return guid
        else:
            return result

    def map_luid_to_guid(self, subsystem, luid, guid):
        """
        Map a luid to a guid that already exists
        """
        self.db.query("putnx('subsystems', set()).add('%s'), put('luid:%s:%s', '%s'), put('guid:%s', '%s', %s)" % \
                               (subsystem, subsystem, luid, guid, guid, subsystem, repr(luid)))

    def get_luid(self, subsystem, guid):
        """
        Given a guid, map the id to a local id for the
        given subsystem.
        """
        result = self.db.query("get('guid:%s', '%s')" % (guid, subsystem))
        return result

    def get_luids(self, guid):
        """
        Given a guid, return a list of all (subsystem, luid) pairs
        """
        result = self.db.query("get('guid:%s')" % guid)
        return result

