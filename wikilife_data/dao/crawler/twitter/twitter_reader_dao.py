# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO

READER_NAME_FIELD = "reader_name"
SINCE_ID_FIELD = "since_id"


class TwitterReaderDAO(BaseDAO):

    _collection = None

    def _initialize(self):
        self._collection = self._db.twitter_readers
        self._collection.ensure_index(READER_NAME_FIELD, unique=True)

    def find_reader(self, reader_name):
        filter = {}
        filter[READER_NAME_FIELD] = reader_name
        reader = self._collection.find_one(filter)
        return reader

    def create_reader(self, reader_name, initial_since_id=0):
        reader = {}
        reader[READER_NAME_FIELD] = reader_name
        reader[SINCE_ID_FIELD] = initial_since_id
        created_reader = self._collection.save(reader)
        return created_reader

    def get_reader_since_id(self, reader_name):
        reader = self.find_reader(reader_name)
        return reader[SINCE_ID_FIELD]

    def set_reader_since_id(self, reader_name, since_id):
        reader = self.find_reader(reader_name)
        reader[SINCE_ID_FIELD] = since_id
        updated_reader = self._collection.save(reader)
        return updated_reader
