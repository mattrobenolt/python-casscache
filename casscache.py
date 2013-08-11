from cassandra.cluster import Cluster, Session

try:
    from cassandra.io.libevreactor import LibevConnection
    ConnectionClass = LibevConnection
except ImportError:
    from cassandra.io.asyncorereactor import AsyncoreConnection
    ConnectionClass = AsyncoreConnection


def _execute_many(self, queries, trace=False):
    """
    Executes many queries in parallel and synchronously waits for the responses.
    """
    futures = (self.execute_async(query, trace=trace) for query in queries)

    for future in futures:
        try:
            yield future.result()
        except Exception:
            yield None

Session.execute_many = _execute_many


class Client(object):
    def __init__(self, servers, **kwargs):
        hosts, port = set(), '9042'
        for server in servers:
            host, port = server.split(':', 1)
            hosts.add(host)

        self._cluster = Cluster(hosts, port=int(port), **kwargs)
        self._cluster.connection_class = ConnectionClass
        self._session = self._cluster.connect()

        self.keyspace = "cache"
        self.column_family = "memcached"

        self._session.set_keyspace(self.keyspace)

        # Prepare all of the necessary statements beforehand
        self._GET = self._session.prepare("SELECT value, flags FROM %s WHERE key = ? LIMIT 1" % self.column_family)
        self._SET = self._session.prepare("INSERT INTO %s (key, value, flags) VALUES (?, ?, ?)" % self.column_family)
        # Cannot be prepared with a dynamic TTL pre C* 2.0
        # See https://issues.apache.org/jira/browse/CASSANDRA-4450
        self._SET_TTL = "INSERT INTO %s (key, value, flags) VALUES (?, ?, ?) USING TTL %%d" % self.column_family

    def get(self, key):
        statement = self._GET
        return self._handle_row(self._session.execute(statement.bind((key,))))

    def get_multi(self, keys):
        statement = self._GET
        result = {}
        for idx, value in enumerate(map(self._handle_row, self._session.execute_many((statement.bind((key,)) for key in keys)))):
            if value is not None:
                result[keys[idx]] = value
        return result

    def set(self, key, value, ttl=0):
        if ttl == 0:
            statement = self._SET
        else:
            statement = self._session.prepare(self._SET_TTL % ttl)
        self._session.execute(statement.bind((key, value, 0)))
        return True

    def set_multi(self, keys):
        pass

    def delete(self, key):
        query = "DELETE FROM %s WHERE key = ?" % self.column_family

    def delete_multi(self, keys):
        query = "DELETE FROM %s WHERE key IN ?" % self.column_family

    def disconnect_all(self):
        self._cluster.shutdown()

    def get_stats(self, *args, **kwargs):
        """ No support for this in C* """
        return []

    def get_slabs(self, *args, **kwargs):
        return []

    def flush_all(self):
        query = "TRUNCATE %s" % self.column_family
        self._session.execute(query)

    def _handle_row(self, rows):
        try:
            return rows[0].value
        except (IndexError, TypeError):
            return None
