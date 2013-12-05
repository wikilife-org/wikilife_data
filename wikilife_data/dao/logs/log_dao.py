# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO
from wikilife_data.sequence import Sequence
from pymongo import DESCENDING, ASCENDING

CREATE_UTC_FIELD = "createUTC"


class LogDAOException(Exception):
    pass


class LogDAO(BaseDAO):
    """
    Log model V4:

        {
        "_id": ObjectId(""),
        "version": "4",
        "id": 123,
        "origId": 0,
        "oper": "i",
        "createUTC": ISODate("2011-06-08T23:31:31.491Z"),
        "update": ISODate("2011-06-08T23:31:31.491Z"),
        "source": "client.iphone",
        "userId":"QWERTY",
        "start": ISODate("2011-06-08T23:31:31.491Z"),
        "end": ISODate("2011-06-08T23:31:31.491Z"),
        "text": <string>,
        "nodes": [
            {
                "nodeId":<integer>,
                "metricId":<integer>,
                "value":<integer|string>
            }
        ]
        }
    """

    _collection = None
    _sequence = None

    def _initialize(self):
        self._collection = self._db.logs
        self._sequence = Sequence(self._db, "logs")
        self._collection.ensure_index(CREATE_UTC_FIELD)
        self._collection.ensure_index("id", unique=True)
        self._collection.ensure_index("userId")
        self._collection.ensure_index("nodes.nodeId")

    def get_log_by_id(self, log_id):
        log = self._collection.find_one({"id": log_id})
        return log

    def get_user_logs_by_node_id(self, user_id, node_id):
        """
        Returns pymongo cursor
        """
        return self._db.logs.find({"userId": user_id, "nodes.nodeId": node_id})

    def get_last_user_log_by_node_id(self, user_id, node_id):
        """
        Returns dict
        """
        cursor = self.get_user_logs_by_node_id(user_id, node_id).sort("start", DESCENDING)

        try:
            last_user_log = cursor.next()
        except StopIteration:
            last_user_log = None

        return last_user_log

    def get_first_log(self):
        return self._collection.find().sort(CREATE_UTC_FIELD, ASCENDING).limit(1).next()

    def get_logs_count(self):
        return self._collection.count()

    def get_logs_by_create_datetime_utc_range_asc(self, create_utc_from, create_utc_to):
        """
        This method is intended for processor initialization
        create_utc_from: Date
        create_utc_to: Date
        Returns Pymongo cursor
        """
        where = {}
        where[CREATE_UTC_FIELD] = {"$gte": create_utc_from, "$lt": create_utc_to}
        return self._collection.find(where).sort(CREATE_UTC_FIELD, ASCENDING)

    def add_log(self, log):
        log_id = self._sequence.next_value()
        log["id"] = int(log_id)
        log["version"] = "4"
        self._collection.insert(log)
        return log_id

    def update_user_id(self, current_user_id, new_user_id):
        self._collection.update({"userId": current_user_id}, {"$set": {"userId": new_user_id}}, multi=True, upsert=False)
    
    #TODO category not existing anymore
    def delete_profile_logs(self, user_id):
        self._collection.remove({"userId": user_id, "category": "profile"})
