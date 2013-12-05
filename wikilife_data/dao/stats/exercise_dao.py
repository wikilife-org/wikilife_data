# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO
from wikilife_utils.date_utils import DateUtils
from pymongo import ASCENDING, DESCENDING
from bson.code import Code


class ExerciseDAOException(Exception):
    pass

class ExerciseDAO(BaseDAO):

    _collection = None

    def _initialize(self):
        self._collection = self._db.exercises
        self._collection.ensure_index([("userId", ASCENDING), ("nodeId", ASCENDING), ("date", ASCENDING)], unique=True)
        self._collection.ensure_index("userId")
        self._collection.ensure_index("node")
        self._collection.ensure_index("date")

    def save_exercise(self, exerciseDTO):
        """
        Saves a valid exercise
        """

        if "userId" not in exerciseDTO or "date" not in exerciseDTO  or "nodeId" not in exerciseDTO \
                 or "properties" not in exerciseDTO or "nodeCount" not in exerciseDTO:
            raise ExerciseDAOException("Invalid ExerciseDTO Structure")

        _id = "%s-%s-%s" % (exerciseDTO["userId"], exerciseDTO["nodeId"], exerciseDTO["date"])

        if "_id" not in exerciseDTO:
            exerciseDTO["_id"] = _id
        elif exerciseDTO["_id"] != _id:
            raise ExerciseDAOException("Invalid _id for ExerciseDTO")

        exerciseDTO["nodeId"] = int(exerciseDTO["nodeId"])

        if self._collection.find_one({"_id":_id}):
            self._collection.save(exerciseDTO)
        else:
             self._collection.insert(exerciseDTO)

    def delete_exercise(self, exerciseDTO):
        """
        Remove a Exercise
        """
        try:
            user_id = exerciseDTO["userId"]
            node_id = exerciseDTO["nodeId"]
            date = exerciseDTO["date"]
        except KeyError as e:
            raise ExerciseDAOException("Invalid Exercise Structure", e)

        #Ensure only remove one record
        query = {"userId": user_id, "nodeId":node_id, "date":date }

        count = self._collection.find(query).count()
        if count > 1:
            raise ExerciseDAOException("Duplicated values in DB: More than 1 record with: userId  = %s, nodeId= %s, date=%s" % (user_id, node_id, date))

        self._collection.remove(query)

    def get_exercises(self, user_id, from_date, to_date, node_id=None):

        """
        user_id: String
        from_date: String YYYY-MM-DD Format
        to_date: String YYYY-MM-DD Format
        """

        query = {}
        query["userId"] = user_id

        date_query = {"$lte": to_date}
        date_query["$gte"] = from_date

        query["date"] = date_query

        if node_id:
            query["nodeId"] = int(node_id)


        exercises = self._collection.find(query).sort("date", DESCENDING)
        return list(exercises)

    def get_exercise(self, user_id, node_id, d_date):
        _id = "%s-%s-%s" % (user_id, node_id, d_date)
        return self._collection.find_one({"_id":_id})

    def delete_exercise_by_user(self, user_id):
        self._collection.remove({"user_id": user_id})

    def get_ranking_by_user(self, user_id, from_date, to_date, top=3):
        _REDUCE = Code("function(key, values){ var result = {count:0}; values.forEach(function(value){result.count +=value.count; }); return result;}")
        _MAP_TPL = Code("function(){emit(this.nodeId, {count:this.nodeCount});}")
        _EXERCISE_RANKING_REPORT = "exercise_ranking_top_%s_for_%s_from_%s_to_%s" % (top, user_id, from_date, to_date)

        #Query: Userid, rango fechas, excluir Water ids

        query = {}
        query["userId"] = user_id
        query["date"] = {"$gt": from_date, "$lt":to_date}

        self._collection.map_reduce(_MAP_TPL, _REDUCE, _EXERCISE_RANKING_REPORT, query=query)

        collection = self.get_db()[_EXERCISE_RANKING_REPORT]
        result = list(collection.find().sort("value.count", DESCENDING).limit(top))
        collection.drop()
        return result
