# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO


class UserOptionLastLogDAO(BaseDAO):

    def get_instance_for_life_variable_ns(self, life_variable_ns):
        """
        :param life_variable_ns: collection ns suffix
        :type node: str

        :rtype: _UserOptionLastLogDAO
        """
        return _UserOptionLastLogDAO(self._logger, self._db, life_variable_ns)

        
class _UserOptionLastLogDAOException(Exception):
    pass

class _UserOptionLastLogDAO(BaseDAO):
    """
    Model
    {
        "userId": str, 
        "option": str, 
        "execUTC": ISODate
    }
    """

    _processor_id = None
    _collection = None
    
    def __init__(self, logger, db, processor_id):
        self._processor_id = processor_id
        BaseDAO.__init__(self, logger, db)
        
    def _initialize(self):
        self._collection = self._db["user_option_last_log."+self._processor_id]
        self._collection.ensure_index("userId", unique=True)
        self._collection.ensure_index("option")

    def get_option_by_user_id(self, user_id):
        return self._collection.find_one({"userId": user_id})

    def count_option(self, option, from_date, to_date):
        where = {}
        where["option"] = option
        where["execUTC"] = {'$gte' : from_date, '$lt' : to_date}
        return self._collection.find(where).count()

    def insert_option(self, user_id, option, exec_utc):
        if self.get_option_by_user_id(user_id) != None:
            raise _UserOptionLastLogDAOException("User option already exists for user id: %s" %user_id)

        self._collection.insert({
            "userId": user_id, 
            "option": option, 
            "execUTC": exec_utc
        })

    def update_option(self, user_id, option, exec_utc):
        user_option = self.get_option_by_user_id(user_id)

        if user_option == None:
            raise _UserOptionLastLogDAOException("User option not found for user id: %s" %user_id)

        user_option["option"] = option
        user_option["execUTC"] = exec_utc
        self._collection.save(user_option)
