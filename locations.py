"""base calss for managing locations such as nodes, hubs, and busses
"""
import os
import sys
from psycopg2 import sql

sys.path.append(os.path.expanduser('~/ercot_virts/utils'))
from utils import Utils


class Location(object):
    """
    """
    u = Utils()
    table = 'location'

    def __init__(self, id=None, **kwargs):
        """
        """
        self.id = id
        self.kwargs = kwargs
        self.name = None
        self.location_type = None
        self.iso_id = None
        self.lat = None
        self.lon = None
        self.is_aggregate = False
        self._setup()
    
    def _setup(self):
        """
        """
        if self.id is not None:
            self._get_from_db()
        else:
            self._from_kwargs()
    
    def _from_kwargs(self):
        """
        """
        for (key, val) in self.kwargs.items():
            setattr(self, key, val)
    
    def _get_from_db_id(self):
        """
        """
        query = sql.SQL("""SELECT * FROM {table} WHERE id = {id}""").format(table=sql.Identifier(self.table), id=sql.Literal(self.id))
        res = self.u.execute_query(query)
        if res is None:
            raise ValueError(f'no location with id {self.id}')
        else:
            for (key, val) in res.items():
                setattr(self, key, val)
    
    def get_location_id(self, name, iso_id):
        """
        """
        query = sql.SQL("""SELECT id FROM {table} WHERE name = {name} AND iso_id = {iso_id}""").format(
                        table=sql.Identifier(self.table), 
                        name=sql.Literal(name),
                        iso_id=sql.Literal(iso_id))
        res = self.u.execute_query(query, fetch='one', dict_cursor=True)
        if res is None:
            return None
        else:
            return res['id']
    
    def save(self):
        """
        """
        if self.id is None:
            self._insert()
        else:
            self._update()
    
    def _insert(self):
        """
        """
        res_dict = self.to_dict(skip_id=True)
        self.id = self.u.insert_dict_record(table=self.table, record_dict=res_dict, return_id=True)
    
    def _update(self):
        """
        """
        raise NotImplementedError("Update not implemented for this class")
    
    def to_dict(self, skip_id=False):
        """
        """
        res = {}
        ignore_keys = ['u', 'table', 'kwargs']
        if skip_id:
            ignore_keys.append('id')
        for (key, val) in self.__dict__.items():
            if key not in ignore_keys:
                res[key] = val
        return res
    

