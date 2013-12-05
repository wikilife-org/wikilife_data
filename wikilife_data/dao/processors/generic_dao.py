# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO


class GenericDAO(BaseDAO):

    def get_instance_for(self, name, indexes):
        """
        :param name: Collection suffix name
        :type node: str

        :param indexes: List indexes
        :type node: list

        :rtype: _GenericDAO
        """
        return _GenericDAO(self._logger, self._db, name, indexes)


class _GenericDAO(BaseDAO):
    _name = None
    _indexes = None
    _collection = None

    def __init__(self, logger, db, name, indexes):
        self._name = name
        self._indexes = indexes
        BaseDAO.__init__(self, logger, db)

    def _initialize(self):
        self._collection = self._db["generic_dao."+self._name]

        for index in self._indexes:
            self._collection.ensure_index(index["field"], unique=index["unique"] if "unique" in index else False)

    def get_single(self, where):
        return self._collection.find_one(where)

    def get_multi(self, where):
        return list(self._get_multi_cursor(where))

    def _get_multi_cursor(self, where):
        return self._collection.find(where)

    def get_avg(self, avg_field, where):
        cursor = self._collection.find(where)
        total = cursor.count()
        
        if total == 0:
            return 0

        sum = 0
        for item in cursor:
            sum += item[avg_field]

        return sum*1.0/total

    def insert(self, item):
        self._collection.insert(item)

    def update(self, item):
        self._collection.save(item)
    
    def delete(self, item):
        self._collection.delete(item)
