# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO
from wikilife_utils.date_utils import DateUtils
from wikilife_utils.hasher import Hasher


class AppDAO(BaseDAO):
    """
    {
    "_id": ObjectId(""),
    "createUTC": ISODate("2011-06-08T23:31:31.491Z"),
    "updateUTC": ISODate("2011-06-08T23:31:31.491Z"),
    "id": ,
    "name": "abc",
    "callbackUrl": "abc",
    "clientId": "abc"
    "clientSecret": "abc"
    "developerId": "abc"
    }
    """

    _collection = None

    def _initialize(self):
        self._collection = self.get_db().apps
        self._collection.ensure_index("id", unique=True)
        self._collection.ensure_index("name", unique=True)
        self._collection.ensure_index("clientId", unique=True)
        self._collection.ensure_index("developerId")

    def get_app_by_id(self, app_id):
        return self._collection.find_one({"id": app_id})

    def get_app_by_name(self, name):
        return self._collection.find_one({"name": name})

    def get_app_by_client_id(self, client_id):
        return self._collection.find_one({"clientId": client_id})

    def get_apps_by_developer_id(self, developer_id):
        return list(self._collection.find({"developerId": developer_id}))

    def insert_app(self, name, callback_url, developer_id, client_id, client_secret):
        app_id = self._generate_app_id()
        app = {}
        app["createUTC"] = DateUtils.get_datetime_utc()
        app["updateUTC"] = None
        app["id"] = app_id
        app["callbackUrl"] = callback_url
        app["name"] = name
        app["clientId"] = client_id
        app["clientSecret"] = client_secret
        app["developerId"] = developer_id
        self._collection.insert(app)

        return app_id

    def update_app(self, app):
        self._collection.save(app)

    def delete_app(self, app_id, developer_id):
        self._collection.remove({"id": app_id, "developerId": developer_id})

    def _generate_app_id(self):
        app_id = Hasher.create_pseudo_unique(16)

        while(self.get_app_by_id(app_id) != None):
            app_id = Hasher.create_pseudo_unique(16)

        return app_id
