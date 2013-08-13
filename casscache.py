"""
casscache
~~~~~~~~~

:copyright: (c) 2013 by Matt Robenolt, see AUTHORS for more details.
:license: BSD, see LICENSE for more details.
"""

try:
    VERSION = __import__('pkg_resources') \
        .get_distribution('casscache').version
except Exception:
    VERSION = 'unknown'

from cassandra.cluster import Cluster, Session

try:
    from cassandra.io.libevreactor import LibevConnection
    ConnectionClass = LibevConnection
except ImportError:
    from cassandra.io.asyncorereactor import AsyncoreConnection
    ConnectionClass = AsyncoreConnection


if not hasattr(Session, 'execute_many'):
    def _execute_many(self, queries, trace=False):
        """
        Executes many queries in parallel and synchronously waits for the responses.
        """
        futures = [self.execute_async(query, trace=trace) for query in queries]

        for future in futures:
            try:
                yield future.result()
            except Exception:
                yield None

    Session.execute_many = _execute_many


class Client(object):

    _FLAG_PICKLE = 1 << 0
    _FLAG_INTEGER = 1 << 1
    _FLAG_LONG = 1 << 2
    _FLAG_COMPRESSED = 1 << 3

    def __init__(self, servers, keyspace, columnfamily, **kwargs):
        hosts, port = set(), '9042'
        for server in servers:
            host, port = server.split(':', 1)
            hosts.add(host)

        self._cluster = Cluster(hosts, port=int(port), **kwargs)
        self._cluster.connection_class = ConnectionClass
        self._session = self._cluster.connect()

        self.keyspace = keyspace
        self.columnfamily = columnfamily
        self._session.set_keyspace(self.keyspace)

        # Prepare all of the necessary statements beforehand
        self._GET = self._session.prepare("SELECT value, flags FROM %s WHERE key = ? LIMIT 1" % self.columnfamily)
        self._SET = self._session.prepare("INSERT INTO %s (key, value, flags) VALUES (?, ?, ?)" % self.columnfamily)
        self._DELETE = self._session.prepare("DELETE FROM %s WHERE key = ?" % self.columnfamily)
        # Cannot be prepared with a dynamic TTL pre C* 2.0
        # See https://issues.apache.org/jira/browse/CASSANDRA-4450
        self._SET_TTL = "INSERT INTO %s (key, value, flags) VALUES (?, ?, ?) USING TTL %%d" % self.columnfamily

    def get(self, key):
        statement = self._GET
        return self._handle_row(self._session.execute(statement.bind((key,))))

    def _prefix_keys(self, keys, key_prefix):
        if not key_prefix:
            return keys
        return [key_prefix + key for key in keys]

    def get_multi(self, keys, key_prefix=''):
        statement = self._GET
        result = {}
        prefixed_keys = self._prefix_keys(keys, key_prefix)
        for idx, value in enumerate(map(self._handle_row,
                                        self._session.execute_many((statement.bind((key,))
                                        for key in prefixed_keys)))):
            if value is not None:
                result[keys[idx]] = value
        return result

    def set(self, key, val, time=0, min_compress_len=0):
        statement = self._get_set_statement(time)
        self._session.execute(statement.bind((key, val, 0)))
        return True

    def set_multi(self, mapping, time=0, key_prefix='', min_compress_len=0):
        statement = self._get_set_statement(time)
        prefixed_keys = self._prefix_keys(mapping.keys(), key_prefix)
        list(self._session.execute_many((statement.bind((prefixed_keys[idx], value, 0)) for idx, value in enumerate(mapping.values()))))
        return 0

    def delete(self, key, time=0):
        statement = self._DELETE
        self._session.execute(statement.bind((key,)))
        return 1

    def delete_multi(self, keys, time=0, key_prefix=''):
        statement = self._DELETE
        prefixed_keys = self._prefix_keys(keys, key_prefix)
        list(self._session.execute_many((statement.bind((key,)) for key in prefixed_keys)))
        return 1

    def disconnect_all(self):
        self._cluster.shutdown()

    def _get_set_statement(self, time=0):
        if time == 0:
            return self._SET
        return self._session.prepare(self._SET_TTL % time)

    def _handle_row(self, rows):
        try:
            return rows[0].value
        except (IndexError, TypeError):
            return None

    def get_stats(self, *args, **kwargs):
        return []

    def get_slabs(self, *args, **kwargs):
        return []

    def incr(self, *args, **kwargs):
        raise NotImplementedError

    def decr(self, *args, **kwargs):
        raise NotImplementedError

    def add(self, *args, **kwargs):
        raise NotImplementedError

    def append(self, *args, **kwargs):
        raise NotImplementedError

    def prepend(self, *args, **kwargs):
        raise NotImplementedError

    def replace(self, *args, **kwargs):
        raise NotImplementedError

    def cas(self, *args, **kwargs):
        raise NotImplementedError

    def gets(self, *args, **kwargs):
        raise NotImplementedError
