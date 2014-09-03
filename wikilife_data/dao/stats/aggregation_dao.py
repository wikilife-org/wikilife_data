# coding=utf-8

from bson.code import Code
from pymongo import ASCENDING, DESCENDING
from wikilife_data.dao.base_dao import BaseDAO

LIMIT = 10000

TYPE_NUMERIC = "num"
TYPE_OPTIONS = "txt"


class AggregationDAO(BaseDAO):
    """
    Life Variable (node+metric) Aggregation

    Model
    {
      nodeId: int,
      metricId: int,
      userId: str,
      date: ISODate("YYYY-MM-DD"),
      count: int,
      type:     "num" | "txt",
      sum:      float | null ,
      optCount: null  | {opt_key: opt_count, "Green": 4}  
    }
    """

    _collection = None

    def _initialize(self):
        self._collection = self._db.aggr
        #TODO single field index included in multiple field index, how mongo manage this ?
        self._collection.ensure_index("date")
        self._collection.ensure_index([("nodeId", ASCENDING), ("date", ASCENDING)])
        self._collection.ensure_index([("nodeId", ASCENDING), ("metricId", ASCENDING), ("date", ASCENDING)])
        self._collection.ensure_index([("nodeId", ASCENDING), ("metricId", ASCENDING), ("date", ASCENDING), ("userId", ASCENDING)], unique=True)

    def get_user_life_variable_day(self, node_id, metric_id, user_id, date_day):
        where = {}
        where["nodeId"] = node_id
        where["metricId"] = metric_id
        where["userId"] = user_id
        where["date"] = date_day
        return self._collection.find_one(where)

    def get_life_variable(self, node_id, metric_id, from_date, to_date):
        where = {}
        where["nodeId"] = node_id
        where["metricId"] = metric_id
        where["date"] = {'$gte' : from_date, '$lt' : to_date}
        cursor = self._collection.find(where).sort("date", ASCENDING).limit(LIMIT)
        return list(cursor)

    def get_times_group_by_user(self, node_id, from_date, to_date):
        where = {}
        where["nodeId"] = node_id
        where["date"] = {'$gte' : from_date, '$lt' : to_date}
        result = self._collection.map_reduce(map=Code("function(){emit(this.userId,this.count);}"),
                                             reduce=Code("function(key, values){ function(key, values){ return Array.sum(values)} }"),
                                             query=where,
                                             out="mr_logged_by_user_nodes")

        return list(result.find()) 

    def get_life_variable_by_day(self, metric_id, from_date, to_date):
        where = {}
        #where["nodeId"] = node_id
        where["metricId"] = metric_id
        where["date"] = {'$gte' : from_date, '$lt' : to_date}
        #reduce=Code("function(key, values){ var total=0; var count=0;for (var i=1; i< values.length; i++){ total+=values[i].sum;count+=values[i].count}; return total/count}"),
        #reduce=Code("function(key, values){ var total=0; for (var i=1; i< values.length; i++){ total+=values[i]['sum']};  if (total>0 && values.length>0){ return total/values.length}else{return 0}}"),

        result = self._collection.map_reduce(map=Code("function(){emit(this.date, {sum:this.sum, count:1});}"),
                                             reduce=Code("function(key, values){ var total=0; var reducedVal = { entries: 0, sum: 0, avg:0 }; for (var i=1; i< values.length; i++){ total+=values[i]['sum']};  if (total>0 && values.length>0){ reducedVal.sum=total;reducedVal.entries=values.length;reducedVal.avg=total/values.length; return reducedVal}else{return reducedVal}}"),
                                             query=where,
                                             out="mr_logged_by_day_nodes")

        return list(result.find()) 
    #TODO move to other DAO
    #TODO naming
    def get_option_last_log(self, node_id, metric_id):
        pass

    '''
    def count_active_users(self, from_date, to_date):
        #Map Reduce instead of group because of sharded collection support
        result = self._collection.map_reduce(map=Code("function(){emit(this.userId, 1);}"),
                                             reduce=Code("function(key, values){}"),
                                             query={"date": {'$gte' : from_date, '$lt' : to_date}},
                                             out="mr_active_users")
        return result.count()

    def count_logged_nodes(self, node_ids, from_date=None, to_date=None):
        where = {}
        where["nodeId"] = {"$in": node_ids}

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
    '''

    """
    def get_numeric_user_life_variable_last_day(self, node_id, metric_id, user_id, from_date, to_date):
        where = {}
        where["nodeId"] = node_id
        where["metricId"] = metric_id
        where["userId"] = user_id
        where["date"] = {'$gte' : from_date, '$lt' : to_date}
        cursor = self._collection_numeric.find(where).sort("date", DESCENDING).limit(1)

        try:
            item = cursor.next()
        except:
            item = None

        return item

    def _get_user_life_variable(self, collection, node_id, metric_id, user_id, from_date, to_date):
        where = {}
        where["nodeId"] = node_id
        where["metricId"] = metric_id
        where["userId"] = user_id
        where["date"] = {'$gte' : from_date, '$lt' : to_date}
        cursor = collection.find(where).sort("date", ASCENDING).limit(LIMIT)
        return list(cursor)

    def get_numeric_user_life_variable(self, node_id, metric_id, user_id, from_date, to_date):
        return self._get_user_life_variable(self._collection_numeric, node_id, metric_id, user_id, from_date, to_date)

    def get_options_user_life_variable(self, node_id, metric_id, user_id, from_date, to_date):
        return self._get_user_life_variable(self._collection_options, node_id, metric_id, user_id, from_date, to_date)

    def get_numeric_node(self, node_id, from_date, to_date):
        where = {}
        where["nodeId"] = node_id
        where["date"] = {'$gte' : from_date, '$lt' : to_date}
        cursor = self._collection_numeric.find(where).sort("date", ASCENDING).limit(LIMIT)

        #TODO mapReduce
        prev_item = None
        items = []

        for item in cursor:
            if prev_item==None or item["date"]==prev_item["date"]:
                pass

        return items
    """

    def add(self, node_ids, metric_id, user_id, date_day, value, type):
        for node_id in node_ids:
            #TODO maybe mongo allows to resolve this in a single query using update upsert
            item = self.get_user_life_variable_day(node_id, metric_id, user_id, date_day)

            if item!=None:
                self._update_item(item, value, type)
                self._collection.save(item)

            else:
                item = self._create_item(node_id, metric_id, user_id, date_day, value, type)
                self._collection.insert(item)

    def remove(self, node_ids, metric_id, user_id, date_day, value, type):
        for node_id in node_ids:
            #TODO maybe mongo allows to resolve this in a single query using update upsert
            item = self.get_user_life_variable_day(node_id, metric_id, user_id, date_day)

            if item!=None:
                """
                Negative values are valid when PRCs is not fully initialized. 
                
                For example:
                Log{id: 1, oper: "i", lv: 1, date: 1, value: 10}
                Log{id: 2, oper: "i", lv: 1, date: 2, value: 4}
                Log{id: 3, oper: "i", lv: 2, date: 3, value: 7}
                Log{id: 4, oper: "d", lv: 1, date: 3, value: 10, origId: 1}
                
                If PRC was initialized since date 2, lv 1 value will be updated to -6 
                """
                self._update_item(self, item, value, type)
                self._collection.save(item)

            else:
                """
                Removing not existing items is valid when PRCs is not fully initialized.
                Negative items are generated as result. 
                
                For example:
                Log{id: 1, oper: "i", lv: 1, date: 1, value: 10}
                Log{id: 2, oper: "i", lv: 1, date: 2, value: 4}
                Log{id: 3, oper: "i", lv: 2, date: 3, value: 7}
                Log{id: 4, oper: "d", lv: 1, date: 3, value: 10, origId: 1}
                
                If PRC was initialized since date 3, lv 1 will be a new item with value -10 
                """
                item = self._create_item(node_id, metric_id, user_id, date_day, value, type, -1)
                self._collection.insert(item)

    def _update_item(self, item, value, type, sign=1):
        item["count"] += 1*sign

        if type==TYPE_NUMERIC:
            item["sum"] += value*sign
        else:
            item["optCount"][value] = item["optCount"][value]+1*sign if value in item["optCount"] else 1*sign

    def _create_item(self, node_id, metric_id, user_id, date_day, value, type, sign=1):
        item = {
          "nodeId": node_id,
          "metricId": metric_id,
          "userId": user_id,
          "date": date_day,
          "count": 1*sign,
          "type": type
        }

        
        if type==TYPE_NUMERIC:
            item["sum"] = value*sign
            item["optCount"] = None
        else:
            item["sum"] = None
            item["optCount"] = {value: 1*sign}

        return item