from cassandra.cluster import Cluster
from cassandra.query import ValueSequence


class Client(object):
    def __init__(self, servers, **kwargs):
        hosts, port = set(), '9042'
        for server in servers:
            host, port = server.split(':', 1)
            hosts.add(host)

        self._cluster = Cluster(hosts, port=int(port), **kwargs)
        self._session = self._cluster.connect()

        self.keyspace = "cache"
        self.column_family = "memcached"

        self._session.set_keyspace(self.keyspace)

        # Prepare all of the necessary statements beforehand
        self._GET_STATEMENT = self._session.prepare("SELECT value, flags FROM %s WHERE key = ? LIMIT 1" % self.column_family)
        self._SET_STATEMENT = self._session.prepare("INSERT INTO %s (key, value, flags) VALUES (?, ?, ?)" % self.column_family)

    def get(self, key):
        statement = self._GET_STATEMENT
        try:
            return self._session.execute(statement.bind((key,)))[0].value
        except IndexError:
            return None


    def get_multi(self, keys):
        pass

    def set(self, key, value, ttl=0):
        statement = self._SET_STATEMENT
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
