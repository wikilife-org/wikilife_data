# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO


class ProfileDAO(BaseDAO):
    """
    Model:
    {
        "userId": "CKJG3L",
        "items" : {
            "gender": {"nodeId": 0, "metricId": 0, value: "", "updateUTC": ISODate("")},
            "birthdate": {"nodeId": 0, "metricId": 0, value: "", "updateUTC": ISODate("")},
            "height": {"nodeId": 0, "metricId": 0, value: "", "updateUTC": ISODate("")},
            "weight": {"nodeId": 0, "metricId": 0, value: "", "updateUTC": ISODate("")},
            "country": {"nodeId": 0, "metricId": 0, value: "", "updateUTC": ISODate("")},
            "region": {"nodeId": 0, "metricId": 0, value: "", "updateUTC": ISODate("")},
            "city": {"nodeId": 0, "metricId": 0, value: "", "updateUTC": ISODate("")}
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

    def get_blank_profile(self, user_id):
        return {
            "userId": user_id,
            "items" : self.get_profile_items()
        }

    def get_profiles_with_no_location(self, limit):
        return self._collection.find({"items.city.value": None, "items.region.value": None, "items.country.value": None,}).limit(limit)
    
    def get_profile_items(self):
        return {
            "gender": {"nodeId": 271256, "metricId": 271257, "value": None, "updateUTC": None},
            "birthdate": {"nodeId": 353, "metricId": 276045, "value": None, "updateUTC": None},
            "height": {"nodeId": 271241, "metricId": 271242, "value": None, "updateUTC": None},
            "weight": {"nodeId": 271245, "metricId": 271246, "value": None, "updateUTC": None},
            "country": {"nodeId": 354, "metricId": 276047, "value": None, "updateUTC": None},
            "region": {"nodeId": 354, "metricId": 276048, "value": None, "updateUTC": None},
            "city": {"nodeId": 354, "metricId": 276049, "value": None, "updateUTC": None},
            "tz": {"nodeId": 354, "metricId": 276046, "value": None, "updateUTC": None}
        }