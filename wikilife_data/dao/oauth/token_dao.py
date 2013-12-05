# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO
from wikilife_utils.date_utils import DateUtils


class TokenDAO(BaseDAO):
    """
    {
    "_id": ObjectId(""),
    "createUTC": ISODate("2011-06-08T23:31:31.491Z"),
    "userId": "abc", #internal wk user id
    "clientId": "abc", #app id
    "token": "abc",
    "code": "abc"
    }
    """

    _collection = None

    def _initialize(self):
        self._collection = self.get_db().tokens
        self._collection.ensure_index("token", unique=True)

    def get_user_id_for_token(self, token):
        mo = self._collection.find_one({"token": token})

        if mo != None:
            return mo["userId"]

        return None
    
    """
    def get_token_for_user_id(self, user_id):
        mo = self._collection.find_one({"userId": user_id})

        if mo != None:
            return mo["token"]

        return None
    """

    def get_user_id_for_code(self, code):
        mo = self._collection.find_one({"code": code})

        if mo != None:
            return mo["userId"]

        return None

    def get_token_for_code(self, code, client_id):
        mo = self._collection.find_one({"code": code, "clientId": client_id})

        if mo != None:
            return mo["token"]

        return None

    def blank_code(self, code, client_id):
        self._collection.update({"code": code, "clientId": client_id}, {"$set": {"code": None}})

    def insert_token(self, user_id, client_id, token, code):
        token_obj = {}
        token_obj["createUTC"] = DateUtils.get_datetime_utc()
        token_obj["updateUTC"] = None
        token_obj["userId"] = user_id
        token_obj["code"] = code
        token_obj["token"] = token
        token_obj["clientId"] = client_id
        self._collection.insert(token_obj)

    def delete_token(self, user_id, client_id):
        self._collection.remove({"userId": user_id, "clientId": client_id})
