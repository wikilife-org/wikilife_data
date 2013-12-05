# coding=utf-8

#===================================
#   TESTS SETTINGS
#===================================

DB_SETTINGS_DEFAULT = {
    "host": "localhost",
    "port": 27017,
}

DB_SETTINGS = {
    "db_users"        : {"host": "localhost", "port": 27017, "name": "wikilife_users"},
    "db_logs"         : {"host": "localhost", "port": 27017, "name": "wikilife_logs"},
    "db_processors"   : {"host": "localhost", "port": 27017, "name": "wikilife_processors"},
    "db_crawler"      : {"host": "localhost", "port": 27017, "name": "wikilife_crawler"},
    "db_admin"        : {"host": "localhost", "port": 27017, "name": "wikilife_admin"},
    "db_location"     : {"name": "wikilife_location", "port": 5432, "user": "postgres", "pass": 123456}
}
DB_SETTINGS["db_meta_live"]["uri"] = "http://localhost:7474/db/data/"