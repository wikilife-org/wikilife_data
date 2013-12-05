# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO
from wikilife_utils.date_utils import DateUtils
from pymongo import ASCENDING


class DevelopersDAO(BaseDAO):
    """
    Model:
       {
        "developer_id":"abc123",
        "developer_name": "abc123",
        "email": "abc123",
        "password": "abc123",
        "create_utc": ISODate("2012-10-15T12:00:00.000Z"),
        }
    """

    _developers_collection = None
    _sessions_collection = None
    
    def _initialize(self):
        self._developers_collection = self.get_db().developers 
        self._developers_collection.ensure_index("developerId", unique=True)
        self._developers_collection.ensure_index("developerName", unique=True)
        self._developers_collection.ensure_index("email", unique=True)
    
        self._sessions_collection = self.get_db().sessions
        self._sessions_collection.ensure_index([("developerId", ASCENDING), ("token", ASCENDING)], unique=True)
        
    def get_developer(self, developer_name, password):
        """
        developerName: wikilife developer user name
        password:  wikilife developer password
        """
        return self._developers_collection.find_one({"developerName": developer_name, "password": password})

    def get_developer_by_id(self, developer_id):
        """
        developer_id: String
        """
        return self._developers_collection.find_one({"developerId": developer_id})

    def get_developer_by_email(self, email):
        """
        email: String
        """
        return self._developers_collection.find_one({"email": email})

    def get_developer_by_name(self, developer_name):
        """
        developer_name: String
        """
        return self._developers_collection.find_one({"developerName": developer_name})


    def insert_developer(self, developer_id, developer_name, password, email):
        """
        developer_id: wikilife developer id
        developer_name: wikilife developer user name
        password: wikilife developer password
        email: wikilife developer email
        """
        create_utc = DateUtils.get_datetime_utc()
        self._developers_collection.insert({ "developerId": developer_id, "developerName": developer_name, "password": password, "email": email,"create_utc": create_utc})

    def delete_developer(self, developer_id):
        """
        developer_id: wikilife  developer id
        """
        self._developers_collection.remove({"developerId": developer_id})
        
    
    def save_developer(self, developer_dto):
        """
        developer_dto: developer obj
        """
        
        self._developers_collection.save(developer)

    def insert_session(self, developer_id, token):
        """
        developer_id: wikilife  developer id
        token: String
        """
        
        create_utc = DateUtils.get_datetime_utc()
        self._sessions_collection.insert({ "developerId": developer_id, "token": token, "create_utc": create_utc})
    
    def delete_session(self, developer_id, token):
        """
        developer_id: wikilife  developer id
        token: String
        """
        self._sessions_collection.remove({"developerId": developer_id, "token": token})

    def get_session(self, developer_id, token):
        """
        developer_id: wikilife  developer id
        token: String
        """
        return self._sessions_collection.find_one({ "developerId": developer_id, "token": token})

    def get_session_by_token(self, token):
        session = self._sessions_collection.find_one({ "token": token})
        if session:
            return session["developerId"]
        
        return None