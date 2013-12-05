# coding=utf-8

from pymongo import DESCENDING
from wikilife_data.dao.base_dao import BaseDAO


class UserTokenDAO(BaseDAO):
    _collection = None

    def _ensure_indexes(self):
        self._collection = self.get_db().user_token
        self._collection.ensure_index([("userId", DESCENDING), ("token", DESCENDING)], unique=True)
        
    def delete_token(self, user_id, token):
        self._collection.remove({"userId":user_id, "token":token})
        
    def insert_token(self, user_id, token, creation_date_utc):
        self._collection.insert({"userId":user_id, "token":token, "createUTC": creation_date_utc})
         
    def get_token(self, user_id, token):
        user_token = self._collection.find_one({"userId":user_id,"token":token})
        return user_token