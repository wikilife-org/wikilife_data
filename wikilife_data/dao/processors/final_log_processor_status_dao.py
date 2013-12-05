# coding=utf-8
from wikilife_data.dao.base_dao import BaseDAO

STARTED_UTC_FIELD = "startedUTC"
LAST_INITIALIZED_RAW_LOG_UTC_FIELD = "lastInitializedRawLogUTC"

class FinalLogProcessorStatusDAOException(Exception):
    pass

class FinalLogProcessorStatusDAO(BaseDAO):
    """
    Final Log Processor status Manager

    Model:
    {
        "_id": "",
        "startedUTC": ISODate,
        "lastInitializedRawLogUTC": ISODate
    }

    Works with one only record
    """

    _collection = None

    def _initialize(self):
        self._collection = self.get_db().fl_processor_status
        self._check()

    def _check(self):
        if self._collection.find().count() > 1:
            raise FinalLogProcessorStatusDAOException("More than one Final Log Processor item found")

    def get_status(self):
        """
        Returns: Dict. fl_processor_status_mo
        """
        return self._collection.find_one()

    def insert_status(self, final_log_processor_id, started_datetime_utc, last_initialized_raw_log_datetime_utc=None):
        """
        final_log_processor_id: String
        started_datetime_utc: Date
        last_initialized_raw_log_datetime_utc: Date
        """

        if self.get_status() != None:
            raise FinalLogProcessorStatusDAOException("Final Log Processor Status already exists. Id: %s" %final_log_processor_id)

        processor_status_mo = {}
        processor_status_mo["_id"] = final_log_processor_id
        processor_status_mo[STARTED_UTC_FIELD] = started_datetime_utc
        processor_status_mo[LAST_INITIALIZED_RAW_LOG_UTC_FIELD] = last_initialized_raw_log_datetime_utc

        self._collection.insert(processor_status_mo)

    def update_last_initialized_raw_log_datetime_utc(self, last_initialized_raw_log_datetime_utc):
        self._collection.update({}, {"$set": {LAST_INITIALIZED_RAW_LOG_UTC_FIELD: last_initialized_raw_log_datetime_utc}})

    def delete_status(self):
        self._collection.remove({})
