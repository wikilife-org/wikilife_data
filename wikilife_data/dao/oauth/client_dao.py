# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO


class ClientDAO(BaseDAO):
    """
    {
    "_id": ObjectId(""),
    "createUTC": ISODate("2011-06-08T23:31:31.491Z"),
    "clientId": "abc", #internal wk user id
    "name": "abc",
    "secret": "abc"
    }
    """

    _collection = None

    def _initialize(self):
        self._collection = self.get_db().clients
