# coding=utf-8

from bson.code import Code
from pymongo import ASCENDING
from wikilife_data.dao.base_dao import BaseDAO


class AggregationNodeDAO(BaseDAO):
    """
    Model
    {
      nodeId: int,
      userId: str,
      date: ISODate("YYYY-MM-DD"),
      count: int,
      
      a <- b <- c 
      3    2    1
        <- d <- e
           1    1 
             <- f
                1
    }
    """

    _collection = None

    def _initialize(self):
        self._collection = self._db.aggr_node
        #TODO single field index included in multiple field index, how mongo manage this ?
        self._collection.ensure_index("nodeId")
        self._collection.ensure_index("date")
        self._collection.ensure_index([("nodeId", ASCENDING), ("date", ASCENDING)])
        self._collection.ensure_index([("nodeId", ASCENDING), ("date", ASCENDING), ("userId", ASCENDING)], unique=True)

    def count_active_users(self, from_date, to_date):
        #Map Reduce instead of group because of sharded collection support
        result = self._collection.map_reduce(map=Code("function(){emit(this.userId, 1);}"),
                                             reduce=Code("function(key, values){}"),
                                             query={"date": {'$gte' : from_date, '$lt' : to_date}},
                                             out="mr_active_users")
        return result.count()

    def count_logged_nodes(self, node_ids, user_id=None, from_date=None, to_date=None):
        where = {}
        where["nodeId"] = {"$in": node_ids}

        if user_id:
            where["userId"] = user_id

        if from_date and to_date:
            where["date"] = {"$gte": from_date, "$lt": to_date}
        elif from_date:
            where["date"] = {"$gte": from_date}
        elif to_date:
            where["date"] = {"$lt": to_date}

        result = self._collection.map_reduce(map=Code("function(){emit(this.nodeId, this.count);}"),
                                             reduce=Code("function(key, values){ return Array.sum(values)}"),
                                             query=where,
                                             out="mr_logged_nodes")

        return list(result.find()) 

    def inc_logged_nodes_count(self, node_ids, user_id, date_day, inc):
        for node_id in node_ids:
            self._collection.update(spec={"nodeId": node_id, "userId": user_id, "date": date_day}, 
                                    document={"$set":{"nodeId": node_id, "userId": user_id, "date": date_day}, "$inc": {"count": inc}}, 
                                    upsert=True, multi=True)
