"""generic record class for tracking model training and evaluation metrics
"""

import sys
import os
import datetime
from psycopg2 import sql
import bson

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
from utils import Utils
import bson


class Record:
    """
    """
    u = Utils()
    object_type = 'record'
    u.mongo.collection = 'records'

    def __init__(self, id=None, **kwargs):
        """
        """
        self._id = id
        self.kwargs = kwargs
        self.setup_mode = False
        self.object_type = self.kwargs.get('object_type', 'record')
        self.parent_id = None
        self.created_at = None
        self.updated_at = None

    @classmethod
    def search(cls, **kwargs):
        """get records from mongodb, filter by kwargs, 
            Returns a list of Record or child class instances
        """
        results = cls.u.m_conn.find(kwargs)
        results = list(results)
        if len(results) == 0:
            print(f"no records found for query: {kwargs}")
            return []
        return [cls.from_dict(**r) for r in results]
    
    @classmethod
    def from_dict(cls, **kwargs):
        """create a record from a dictionary
        """
        if 'object_type' not in kwargs:
            raise ValueError("object_type must be provided")
        if kwargs.get('object_type') != cls.object_type:
            raise ValueError(f"object type mismatch, expected {cls.object_type}, got {kwargs.get('object_type')}")
        kwargs.pop('object_type') # remove object_type from kwargs, as it is set in super().__init__
        return cls(**kwargs)
    
    @property
    def id(self):
        """ 
        """
        return self._id

    def _from_id(self, id=None):
        """
        """
        if id is None:
            id = self._id
        if isinstance(id, str) and len(id) == 24:
            id = bson.ObjectId(id)
        res = self.u.m_conn.find_one({'_id': id})
        if res is None:
            raise ValueError(f"no record found for id: {id}, object_type: {self.object_type}")
        self._from_dict(config_dict=res)

    def _from_dict(self, config_dict={}, **kwargs):
        """
        """
        # allow for kwargs or config_dict
        if config_dict is None and len(kwargs) == 0:
            raise ValueError("Must provide config_dict or kwargs")
        self.setup_mode = True
        if len(kwargs) > 0:
            config_dict.update(**kwargs)
        for cfg_name in ['cfg', 'config']:
            if cfg_name in config_dict:
                config_dict.update(config_dict[cfg_name]) # merge but don't pop
        for (k, v) in config_dict.items():
            setattr(self, k, v)
        self.setup_mode = False

    def to_dict(self):
        """
        """
        # must be implemented by child class
        raise NotImplementedError("to_dict method must be implemented by child class")

    def save(self):
        """Save record, this method is typically overridden by child class
        """
        self._save_record()

    def _save_record(self):
        """
        """
        if self._id is None:
            self.created_at = datetime.datetime.now()
            self.updated_at = datetime.datetime.now()
            self._id = self.u.m_conn.insert_one(self.to_dict()).inserted_id
        else:
            self.updated_at = datetime.datetime.now()
            self.u.m_conn.update_one({"_id": self._id}, {"$set": self.to_dict()})

    def add_keys(self, **kwargs):
        """add a new key to the object and save it to mongodb
        """
        begin_with_setup_mode = self.setup_mode 
        self.setup_mode = True #prevent infinite loop
        for key, value in kwargs.items():
            setattr(self, key, value)
            self.u.m_conn.update_one({"_id": self._id}, {"$set": {key: value}, "$currentDate": {"updated_at": True}})
        if not begin_with_setup_mode:  # upstream func may have set setup_mode, don't change it
            self.setup_mode = False 
