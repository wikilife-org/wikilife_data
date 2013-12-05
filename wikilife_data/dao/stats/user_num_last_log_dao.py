# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO


class UserNumLastLogDAO(BaseDAO):

    def get_instance_for_life_variable_ns(self, life_variable_ns):
        """
        :param life_variable_ns: collection ns suffix
        :type node: str

        :rtype: _UserNumLastLogDAO
        """
        return _UserNumLastLogDAO(self._logger, self._db, life_variable_ns)


class _UserNumLastLogDAOException(Exception):
    pass


class _UserNumLastLogDAO(BaseDAO):
    """
    Model
    {
        "userId": str, 
        "value": float, 
        "execUTC": ISODate
    }
    """
