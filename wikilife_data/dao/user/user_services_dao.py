# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO
from wikilife_utils.date_utils import DateUtils
from pymongo import ASCENDING


class UserServicesDAO(BaseDAO):
    """
    Model:
       {
        "user_id":"abc123",
        "id": "abc123",
        "name": "abc123",
        "auth_code": "abc123",
        "access_token": "abc123",
        "account": "abc123", 
        "create_utc": ISODate("2012-10-15T12:00:00.000Z"),
        "hash": hash
        }
    """

    _collection = None

    def _initialize(self):
        self._collection = self.get_db().user_services 
        self._collection.ensure_index([("user_id", ASCENDING), ("id", ASCENDING)], unique=True)
        self._collection.ensure_index("hash", unique=True)

    def get_user_service(self, user_id, id):
        """
        user_id: wikilife user id
        id: service id
        """
        return self._collection.find_one({"user_id": user_id, "id": id})

    def get_user_service_by_hash(self, hash):
        """
        hash: String
        """
        return self._collection.find_one({"hash": hash})

    def get_user_services(self, user_id):
        """
        user_id: wikilife user id
        """
        return list(self._collection.find({"user_id": user_id}))

    def insert_user_service(self, user_id, id, name, auth_code, access_token, account, hash):
        """
        user_id: wikilife user id
        id: service id
        name: service name
        auth_code: service integrator authorization code
        access_token: service integrator access token
        account: service integrator account
        hash: String
        """
        create_utc = DateUtils.get_datetime_utc()
        self._collection.insert({"user_id": user_id, "id": id, "name": name, "auth_code": auth_code, "access_token": access_token, "account": account, "hash": hash, "create_utc": create_utc})

    def delete_user_service(self, user_id, id):
        """
        user_id: wikilife user id
        id: service id
        """
        self._collection.remove({"user_id": user_id, "id": id})
