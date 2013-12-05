# coding=utf-8

from bulbs.neo4jserver.graph import Graph, Config
from pymongo.connection import Connection
import psycopg2

DB_META_LIVE = "db_meta_live"
DB_META_EDIT = "db_meta_edit"
DB_USERS = "db_users"
DB_LOGS = "db_logs"
DB_PROCESSORS = "db_processors"
DB_AGGREGATION = "db_aggregation"
DB_CRAWLER = "db_crawler"
DB_ADMIN = "db_admin"
DB_APPS = "db_apps"
DB_LOCATION = "db_location"


class DBConn(object):
    """
    Provides DB connections
    """

    _db_settings = None
    _db_user = None
    _db_pass = None
    _conns = None

    def __init__(self, db_settings, db_user, db_pass):
        """
        db_settings: Dictionary
        db_user: String
        db_pass: String
        """
        self._db_settings = db_settings
        self._db_user = db_user
        self._db_pass = db_pass
        self._conns = {}

    def _get_conn(self, db_code):
        if not db_code in self._conns:
            self._conns[db_code] = self._open_conn(db_code)

        return self._conns[db_code]

    def _open_conn(self, db_code):
        if db_code == DB_LOCATION or db_code == DB_AGGREGATION:
            return self._open_postgresql_conn(db_code)
        elif db_code == DB_META_LIVE or db_code == DB_META_EDIT:
            return self._open_neo4j_conn(db_code)

        return self._open_mongodb_conn(db_code)

    def _open_mongodb_conn(self, db_code):
        if self._db_user == None:
            return Connection(host=self._db_settings[db_code]["host"], port=self._db_settings[db_code]["port"])[self._db_settings[db_code]["name"]]

        return Connection(host="mongodb://%s:%s@%s:%s/%s" % (self._db_user, self._db_pass, self._db_settings[db_code]["host"], self._db_settings[db_code]["port"], self._db_settings[db_code]["name"]))

    def _open_postgresql_conn(self, db_code):
        db = self._db_settings[db_code] 
        conn = psycopg2.connect(host=db["host"], port=db["port"], database=db["name"], user=db["user"], password=db["pass"])
        return conn

    def _open_neo4j_conn(self, db_code):
        db = self._db_settings[db_code]
        config = Config(db["uri"])
        conn = Graph(config)
        return conn

    def get_conn_meta_live(self):
        return self._get_conn(DB_META_LIVE)

    def get_conn_meta_edit(self):
        return self._get_conn(DB_META_EDIT)

    def get_conn_users(self):
        return self._get_conn(DB_USERS)

    def get_conn_logs(self):
        return self._get_conn(DB_LOGS)

    def get_conn_processors(self):
        return self._get_conn(DB_PROCESSORS)

    def get_conn_aggregation(self):
        return self._get_conn(DB_AGGREGATION)

    def get_conn_crawler(self):
        return self._get_conn(DB_CRAWLER)

    def get_conn_admin(self):
        return self._get_conn(DB_ADMIN)

    def get_conn_apps(self):
        return self._get_conn(DB_APPS)

    def get_conn_location(self):
        return self._get_conn(DB_LOCATION)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "DB conn settings. User: '%s'\nDB_META_LIVE: %s\nDB_META_EDIT: %s\nDB_USERS: %s\nDB_LOGS: %s\nDB_PROCESSORS: %s\nDB_CRAWLER: %s\nDB_ADMIN: %s" \
            % (self._db_user, self._db_settings[DB_META_LIVE], self._db_settings[DB_META_EDIT], self._db_settings[DB_USERS], self._db_settings[DB_LOGS], self._db_settings[DB_PROCESSORS], self._db_settings[DB_CRAWLER], self._db_settings[DB_ADMIN])
