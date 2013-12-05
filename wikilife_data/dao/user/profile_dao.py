# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO


class ProfileDAO(BaseDAO):
    """
    Model:
    {
        "userId": "CKJG3L",
        "updateUTC": ISODate(),
        "items" : {
            "gender": {"nodeId": 0, "metricId": 0, value: ""},
            "birthdate": {"nodeId": 0, "metricId": 0, value: ""},
            "height": {"nodeId": 0, "metricId": 0, value: ""},
            "weight": {"nodeId": 0, "metricId": 0, value: ""},
            "country": {"nodeId": 0, "metricId": 0, value: ""},
            "region": {"nodeId": 0, "metricId": 0, value: ""},
            "city": {"nodeId": 0, "metricId": 0, value: ""}
        }
    }
    """

    _collection = None

    def _initialize(self):
        self._collection = self.get_db().profile
        self._collection.ensure_index("userId", unique=True)

    def save_profile(self, profile_mo):
        self._collection.save(profile_mo)

    def remove_profile_by_user_id(self, user_id):
        self._collection.remove({"userId": user_id})

    def get_profile_by_user_id(self, user_id):
        return self._collection.find_one({"userId": user_id})

    def get_new_profile(self, user_id, update_utc):
        return {
            "userId": user_id,
            "updateUTC": update_utc,
            "items" : self.get_profile_items()
        }

    def get_profile_items(self):
        return {
            "gender": {"nodeId": 271256, "metricId": 271257, "value": None},
            "birthdate": {"nodeId": 353, "metricId": 276045, "value": None},
            "height": {"nodeId": 271241, "metricId": 271242, "value": None},
            "weight": {"nodeId": 271245, "metricId": 271246, "value": None},
            "country": {"nodeId": 354, "metricId": 276047, "value": None},
            "region": {"nodeId": 354, "metricId": 276048, "value": None},
            "city": {"nodeId": 354, "metricId": 276049, "value": None},
            "tz": {"nodeId": 354, "metricId": 276046, "value": None}
        }