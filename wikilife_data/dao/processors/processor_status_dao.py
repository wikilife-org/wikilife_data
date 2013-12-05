# coding=utf-8

from pymongo import ASCENDING
from wikilife_data.dao.base_dao import BaseDAO

SINCE_UTC_FIELD = "sinceUTC"


class ProcessorStatusDAOException(Exception):
    pass

class ProcessorStatusDAO(BaseDAO):
    """
    Processor status Manager
    
    Processor Status model:
    {
        "_id": "", #processor_class_fullname
        "sinceUTC": ISODate,
    }
    """

    _collection = None

    def _initialize(self):
        self._collection = self.get_db().processor_status

    def get_processors_status(self):
        """
        returns pymongo cursor
        """
        return self._collection.find().sort("_id", ASCENDING)

    def get_processor_status(self, id):
        """
        Returns: Dict. processor_status_mo
        """
        return self._collection.find_one({"_id": id})

    def insert_processor_status(self, id, since_datetime_utc):
        """
        id: String
        since_datetime_utc: Date
        """

        if self.get_processor_status(id) != None:
            raise ProcessorStatusDAOException("Processor Status already exists. Id: %s" %id)

        processor_status_mo = {}
        processor_status_mo["_id"] = id
        processor_status_mo[SINCE_UTC_FIELD] = since_datetime_utc

        self._collection.insert(processor_status_mo)

    def update_processor_status(self, processor_status_mo):
        """
        """
        self._collection.save(processor_status_mo)

    def delete_processors_status_except(self, ids):
        """
        Delete all processors except the specified ids
        ids: List 
        Returns: List. Deleted ids.
        """
        del_ids = []
        for del_prc_status in self._collection.find({"_id": {"$nin": ids}}):
            del_ids.append(del_prc_status["_id"])

        self._collection.remove({"_id": {"$nin": ids}})
        return del_ids