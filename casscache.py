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

    def get_multi(self, keys):
        statement = self._GET
        result = {}
        for idx, value in enumerate(map(self._handle_row, self._session.execute_many((statement.bind((key,)) for key in keys)))):
            if value is not None:
                result[keys[idx]] = value
        return result

    def set(self, key, value, ttl=0):
        statement = self._get_set_statement(ttl)
        self._session.execute(statement.bind((key, value, 0)))
        return True

    def set_multi(self, pairs, ttl=0):
        statement = self._get_set_statement(ttl)
        list(self._session.execute_many((statement.bind((key, value, 0)) for key, value in pairs.iteritems())))
        return 0

    def delete(self, key, time=0):
        statement = self._DELETE
        self._session.execute(statement.bind((key,)))
        return 1

    def delete_multi(self, keys, time=0):
        statement = self._DELETE
        list(self._session.execute_many((statement.bind((key,)) for key in keys)))
        return 1

    def disconnect_all(self):
        self._cluster.shutdown()

    def get_stats(self, *args, **kwargs):
        """ No support for this in C* """
        return []

    def get_slabs(self, *args, **kwargs):
        return []

    def incr(self, key, delta=1):
        return None

    def decr(self, key, delta=1):
        return None

    def flush_all(self):
        query = "TRUNCATE %s" % self.columnfamily
        self._session.execute(query)

    def _get_set_statement(self, ttl=0):
        if ttl == 0:
            return self._SET
        return self._session.prepare(self._SET_TTL % ttl)

    def _handle_row(self, rows):
        try:
            return rows[0].value
        except (IndexError, TypeError):
            return None
