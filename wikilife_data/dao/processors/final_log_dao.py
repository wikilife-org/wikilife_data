# coding=utf-8

from datetime import datetime
from pymongo import DESCENDING, ASCENDING
from wikilife_data.dao.base_dao import BaseDAO
from wikilife_utils.date_utils import DateUtils


CREATE_UTC_FIELD = "createUTC"


class FinalLogDAOException(Exception):
    pass


class FinalLogDAO(BaseDAO):
    """
    Model:
    {   "_id": 123,
        "version": "4", 
        "oper": str, 
        "createUTC": ISODate("2011-06-09T18:38:52.398Z"),
        "userId":"QWERTY",
        "text": "Running  60 minutes",
        "start": ISODate("2011-06-08T23:31:31.491Z"),
        "end": ISODate("2011-06-08T23:31:31.491Z"),
        "source":"client.iphone",
        "nodes": [
            {"nodeId": 1000, "metricId": 100, "value": 60.0}
        ], 
    }
    """
    

    _collection = None
    _sequence = None

    def _initialize(self):
        self._collection = self.get_db().final_logs
        self._collection.ensure_index(CREATE_UTC_FIELD)

    def get_final_log_by_id(self, log_id):
        log = self._collection.find_one({"_id": log_id})
        return log

    def get_first_final_log(self):
        cursor = self._collection.find().sort(CREATE_UTC_FIELD, ASCENDING).limit(1)

        if cursor.count() > 0:
            return self._collection.find().sort(CREATE_UTC_FIELD, ASCENDING).limit(1).next()
        else:
            return None

    def get_final_logs_count(self):
        return self._collection.find().count()

    def get_final_logs_by_user(self, user_id):
        return self._collection.find({"userId": user_id})

    def _get_final_logs_by_create_datetime_utc_range_desc(self, create_datetime_utc_from, create_datetime_utc_to, order_direction):
        """
        This method is intended for processor initialization
        create_datetime_utc_from: String
        create_datetime_utc_to: String
        Returns Pymongo cursor
        """
        where = {}
        where[CREATE_UTC_FIELD] = {"$gte": create_datetime_utc_from, "$lt": create_datetime_utc_to}
        return self._collection.find(where).sort(CREATE_UTC_FIELD, order_direction)

    def get_final_logs_by_create_datetime_utc_range_desc(self, create_datetime_utc_from, create_datetime_utc_to):
        return self._get_final_logs_by_create_datetime_utc_range_desc(create_datetime_utc_from, create_datetime_utc_to, DESCENDING)

    def get_final_logs_by_create_datetime_utc_range_asc(self, create_datetime_utc_from, create_datetime_utc_to):
        return self._get_final_logs_by_create_datetime_utc_range_desc(create_datetime_utc_from, create_datetime_utc_to, ASCENDING)

    def get_final_logs_count_by_category(self, category, create_from=None, create_to=None):
        where = {"category": category}

        if create_from and create_to:
            where[CREATE_UTC_FIELD] = {"$gte": create_from, "$lt": create_to}
        elif create_from:
            where[CREATE_UTC_FIELD] = {"$gte": create_from}

        return self._collection.find(where).count()

    def insert_final_log(self, fl_log_id, oper, create_utc, update, user_id, text, start, end, source, location, nodes):
        mo = {
            "_id": fl_log_id,
            "oper": oper,
            "version": "4",
            "createUTC": create_utc,
            "update": update,
            "userId": user_id,
            "text": text,
            "start": start,
            "end": end,
            "source": source,
            "location": location,
            "nodes": nodes
        }

        self._collection.insert(mo)
        return mo

    def delete_final_log(self, final_log_id):
        self._collection.remove({"_id": final_log_id})

    def update_user_id(self, current_user_id, new_user_id):
        self._collection.update({"userId": current_user_id}, {"$set": {"userId": new_user_id}}, multi=True, upsert=False)
    
    """
    def delete_profile_final_logs(self, user_id):
        self._collection.remove({"userId": user_id, "category": "profile"})
    """

    def get_logs_in_range(self, fields, from_date, to_date):
        if to_date is None:
            to_date = DateUtils.get_datetime_utc()

        from_date = datetime.fromtimestamp(int(from_date))

        where = {}
        where[CREATE_UTC_FIELD] = {"$gte": from_date, "$lt": to_date}

        return list(self._collection.find(where, fields).limit(20).sort(CREATE_UTC_FIELD, ASCENDING))
    
    """
    def get_latest_logs(self, fields, amount):
        logs = list(self._collection.find(spec={"$and": [{"category": {"$ne": "profile"}}, {"category": {"$ne": "journal"}}]}, fields=fields).limit(20).sort(CREATE_UTC_FIELD, DESCENDING))
        logs.reverse()
        return logs
    """

    def count_final_logs(self, create_datetime_utc_from=None, create_datetime_utc_to=None):
        where = {}

        if create_datetime_utc_from and create_datetime_utc_to:
            where[CREATE_UTC_FIELD] = {"$gte": create_datetime_utc_from, "$lt": create_datetime_utc_to}
        return self._collection.find(where).count()
