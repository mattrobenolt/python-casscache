try:
    import unittest
except ImportError:
    import unittest2 as unittest

import sys
from casscache import Client
from cassandra.cluster import Cluster

TEST_KEYSPACE = 'casscache_test'
TEST_COLUMNFAMILY = 'casscache'


def setUpModule():
    cluster = Cluster()
    session = cluster.connect()
    try:
        session.execute("""
            CREATE KEYSPACE %s
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """ % TEST_KEYSPACE)
    except Exception:
        pass

    try:
        session.execute("""
            CREATE COLUMNFAMILY %s.%s (
                key varchar PRIMARY KEY,
                value blob,
                flags int
            )
        """ % (TEST_KEYSPACE, TEST_COLUMNFAMILY))
    except Exception:
        pass
    cluster.shutdown()


def tearDownModule():
    cluster = Cluster()
    session = cluster.connect()
    session.execute("DROP KEYSPACE %s" % TEST_KEYSPACE)
    cluster.shutdown()


class CasscacheTests(unittest.TestCase):
    def setUp(self):
        self.client = Client(['127.0.0.1:9042'], keyspace=TEST_KEYSPACE, columnfamily=TEST_COLUMNFAMILY)

    def tearDown(self):
        self.client.disconnect_all()
        cluster = Cluster()
        session = cluster.connect()
        session.execute("TRUNCATE %s.%s" % (TEST_KEYSPACE, TEST_COLUMNFAMILY))
        cluster.shutdown()

    def test_get_not_found(self):
        self.assertIsNone(self.client.get('lol'))

    def test_set(self):
        self.assertEqual(self.client.set('lol', 'derp'), 1)

    def test_get_found(self):
        self.client.set('lol', 'derp')
        self.assertEqual(self.client.get('lol'), 'derp')

    def test_set_get_multi(self):
        keys = map(lambda n: 'lol%d' % n, range(0, 5))
        mapping = dict((key, 'derp') for key in keys)
        self.client.set_multi(mapping)
        self.assertEqual(self.client.get_multi(keys), mapping)

    def test_set_prefix(self):
        self.client.set_multi({'lol': 'derp'}, key_prefix='!')
        self.assertEqual(self.client.get('!lol'), 'derp')

    def test_get_prefix(self):
        self.client.set('!lol', 'derp')
        self.assertEqual(self.client.get_multi(['lol'], key_prefix='!'), {'lol': 'derp'})

    def test_delete(self):
        self.client.set('lol', 'derp')
        self.client.delete('lol')
        self.assertIsNone(self.client.get('lol'))

    def test_delete_multi(self):
        self.client.set('lol', 'derp')
        self.client.set('lol2', 'derp')
        self.client.delete_multi(['lol', 'lol2'])
        self.assertEqual(self.client.get_multi(['lol', 'lol2']), {})

    def test_marshal_int(self):
        self.client.set('lol', 1)
        res = self.client.get('lol')
        self.assertIsInstance(res, int)
        self.assertEqual(res, 1)

    @unittest.skipIf(sys.version_info[0] == 3)
    def test_marshal_long(self):
        self.client.set('lol', 1L)
        res = self.client.get('lol')
        self.assertIsInstance(res, long)
        self.assertEqual(res, 1L)

    def test_marshal_pickle(self):
        self.client.set('lol', [1, 2])
        self.assertEqual(self.client.get('lol'), [1, 2])

    def test_flush_all(self):
        self.client.set('lol', 'derp')
        self.client.flush_all()
        self.assertIsNone(self.client.get('lol'))

    def test_set_and_expire(self):
        self.client.set('lol', 'derp', -1)
        self.assertIsNone(self.client.get('lol'))

if __name__ == '__main__':
    unittest.main()
