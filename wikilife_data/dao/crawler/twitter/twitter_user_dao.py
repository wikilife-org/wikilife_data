# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO

INTERNAL_ID_FIELD = "internal_id"
TWITTER_ID_HASH_FIELD = "twitter_id_hash"


class TwitterUserDAO(BaseDAO):
    
    _collection = None
    
    def _initialize(self):
        self._collection = self._db.twitter_users
        self._collection.ensure_index(TWITTER_ID_HASH_FIELD, unique=True)
    
    def find_twitter_user(self, twitter_id_hash):
        """
        twitter_id_hash: Hash
        """
        user = self._collection.find_one({TWITTER_ID_HASH_FIELD: twitter_id_hash})
        return user

    def create_twitter_user(self, internal_user_id, twitter_id_hash):
        """
        internal_user_id: wikilife user id
        twitter_id_hash: Hash
        """
        user = {}
        user[INTERNAL_ID_FIELD] = internal_user_id
        user[TWITTER_ID_HASH_FIELD] = twitter_id_hash
        self._collection.save(user)

    def set_internal_user(self, twitter_id_hash, user_id):
        """
        internal_user_id: wikilife user id
        twitter_id_hash: Hash
        """
        t_user = self._collection.find_one({TWITTER_ID_HASH_FIELD: twitter_id_hash})
        t_user[INTERNAL_ID_FIELD] = user_id
        self._collection.save(t_user)
        
    def count_total_twitter_users(self):
        return self._collection.count()