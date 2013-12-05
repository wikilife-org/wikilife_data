# coding=utf-8


class BaseDAO(object):

    _logger = None
    _db = None

    def __init__(self, logger, db):
        self._logger = logger
        self._db = db
        self._initialize()

    #protected
    def _initialize(self):
        pass

    def get_db(self):
        return self._db

    def get_logger(self):
        return self._logger
