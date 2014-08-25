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

try:
    import cPickle as pickle
except ImportError:
    import pickle  # noqa

from cassandra.cluster import Cluster, Session
from cassandra.protocol import SyntaxException


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

    def __init__(self, servers, keyspace, columnfamily, **kwargs):
        hosts, port = set(), '9042'
        for server in servers:
            host, port = server.split(':', 1)
            hosts.add(host)

        self._cluster = Cluster(hosts, port=int(port), **kwargs)
        self._cluster.protocol_version = 1
        self._session = self._cluster.connect()

        self.keyspace = keyspace
        self.columnfamily = columnfamily
        self._session.set_keyspace(self.keyspace)

        # Prepare all of the necessary statements beforehand
        self._GET = self._session.prepare("SELECT value, flags FROM %s WHERE key = ? LIMIT 1" % self.columnfamily)
        self._SET = self._session.prepare("INSERT INTO %s (key, value, flags) VALUES (?, ?, ?)" % self.columnfamily)
        self._DELETE = self._session.prepare("DELETE FROM %s WHERE key = ?" % self.columnfamily)
        try:
            self._SET_TTL = self._session.prepare("INSERT INTO %s (key, value, flags) VALUES (?, ?, ?) USING TTL ?" % self.columnfamily)
            self._can_prepare_ttl = True
        except SyntaxException:
            # Cannot be prepared with a dynamic TTL pre C* 2.0
            # See https://issues.apache.org/jira/browse/CASSANDRA-4450
            self._SET_TTL = "INSERT INTO %s (key, value, flags) VALUES (?, ?, ?) USING TTL %%d" % self.columnfamily
            self._can_prepare_ttl = False

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
        if time == 0:
            statement = self._SET
            self._session.execute(statement.bind((key,) + self._val_to_store_info(val)))
        elif time > 0:
            statement = self._SET_TTL
            if self._can_prepare_ttl:
                self._session.execute(statement.bind((key,) + self._val_to_store_info(val) + (time,)))
            else:
                statement = self._session.prepare(self._SET_TTL % time)
                self._session.execute(statement.bind((key,) + self._val_to_store_info(val)))
        return True

    def set_multi(self, mapping, time=0, key_prefix='', min_compress_len=0):
        statement = self._get_set_statement(time)
        if statement is not None:
            prefixed_keys = self._prefix_keys(mapping.keys(), key_prefix)
            list(self._session.execute_many((
                statement.bind((prefixed_keys[idx],) + self._val_to_store_info(value))
                for idx, value in enumerate(mapping.values())
            )))
        return 0

    def delete(self, key, time=0):
        statement = self._DELETE
        self._session.execute(statement.bind((key,)))
        return 1

    def delete_multi(self, keys, time=0, key_prefix=''):
        statement = self._DELETE
        prefixed_keys = self._prefix_keys(keys, key_prefix)
        list(self._session.execute_many((
            statement.bind((key,))
            for key in prefixed_keys
        )))
        return 1

    def flush_all(self):
        self._session.execute("TRUNCATE %s" % self.columnfamily)

    def disconnect_all(self):
        self._cluster.shutdown()

    def _get_set_statement(self, time=0):
        if time < 0:
            return None
        if time == 0:
            return self._SET
        return

    def _handle_row(self, rows):
        try:
            row = rows[0]
            val, flags = row.value, row.flags
            if flags == 0:
                # Either a bare string or a compressed string now decompressed...
                return val
            elif flags & Client._FLAG_INTEGER:
                return int(val)
            elif flags & Client._FLAG_LONG:
                return long(val)
            elif flags & Client._FLAG_PICKLE:
                return pickle.loads(val)
            return None
        except Exception:
            return None

    def _val_to_store_info(self, val):
        """
        Transform val to a storable representation,
        returning a tuple of the flags, the length of the new value, and the new value itself.
        """
        if isinstance(val, str):
            return val, 0
        elif isinstance(val, int):
            return "%d" % val, Client._FLAG_INTEGER
        elif isinstance(val, long):
            return "%d" % val, Client._FLAG_LONG
        return pickle.dumps(val, protocol=pickle.HIGHEST_PROTOCOL), Client._FLAG_PICKLE

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
