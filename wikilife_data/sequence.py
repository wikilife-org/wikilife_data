# coding=utf-8

class Sequence(object):
    """
    DB Sequence
    Stores data in 'seq' collection
    """

    NAME_FIELD = "_id"
    VALUE_FIELD = "seq"
    
    _collection = None #the internal sequence collection. See asign in __init__(): self._collection = db.seq  
    _name = None

    def __init__(self, db, name):
        """
        db PyMongo DB object.
        name Sequence name. Commonly the name of te collection using the sequence as PK.
        """

        self._collection = db.seq
        self._name = name
        self._check()

    def _check(self):
        if not self._get_seq():
            self._collection.insert({self.NAME_FIELD: self._name, self.VALUE_FIELD: 0});

    def _get_seq(self):
        return self._collection.find_one({self.NAME_FIELD: self._name})

    def get_name(self):
        return self._name

    def current_value(self):
        return self._get_seq()[self.VALUE_FIELD]

    def next_value(self):
        updated_sequence = self._collection.find_and_modify(query={self.NAME_FIELD: self._name}, update={"$inc": {self.VALUE_FIELD: 1}}, upsert=True, new=True)
        return int(updated_sequence[self.VALUE_FIELD]);
    
    def reset_to_zero(self):
        self.reset_to(0)
    
    def reset_to(self, value):
        self._collection.find_and_modify(query={self.NAME_FIELD: self._name}, update={self.VALUE_FIELD: value}, upsert=True, new=True)
