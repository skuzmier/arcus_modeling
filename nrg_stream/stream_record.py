"""
"""
import os
import sys
import bson
import datetime
import pytz

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts"))
from utils import Utils
from locations import Location


class StreamRecord:
    """
    """
    collection = 'stream_records'
    u = Utils()

    def __init__(self, id=None, stream_id=None, **kwargs):
        """
        """
        self._id = id
        self.kwargs = kwargs
        self.stream_id = stream_id
        self.iso = None
        self.iso_id = None
        self.location = None
        self.location_type = None
        self.location_id = None
        self._first_date = None
        self._last_date = None
        self.available_from = None
        self.last_updated = None
        self.active = True
        self.freq = None
        self.missing_dates = []
        self.stream_data = {}
        self.db_info = None
        self.setup_mode = False
        self.object_type = 'stream_record'
        self.scraper = None
        self._setup()
    
    @classmethod
    def get_all(cls, **kwargs):
        """get all stream records for db,
            Args:
            -----
                kwargs: dict, optional
                    additional query parameters
        """
        cls.u.mongo.collection = cls.collection
        res = cls.u.m_conn.find({'object_type': 'stream_record', **kwargs})
        if res is None:
            print('no records found')
            return []
        return [StreamRecord(**x) for x in res]
    
    @property
    def id(self):
        """
        """
        return self._id
    
    @property
    def first_date(self):
        """return first date localized to table tz
        """
        if self._first_date is None:
            return None
        tz = pytz.timezone(self.db_info.table_tz)
        return tz.localize(self._first_date)
    
    @first_date.setter
    def first_date(self, value):
        """
        """
        if self.setup_mode:
            return None
        if not isinstance(value, datetime.datetime):
            value = self.u.time_converter(value, target='dt')
        if self._first_date is None:
            self._first_date = value
            if self._id is not None:
                self.u.m_conn.update_one({'_id': self._id}, {'$set': {'first_date': self._first_date, 
                                                                      'last_updated': datetime.datetime.now()}})
        else:
            if value < self._first_date:
                self._first_date = value
                self.u.m_conn.update_one({'_id': self._id}, {'$set': {'first_date': self._first_date, 
                                                                      'last_updated': datetime.datetime.now()}})
            else:
                raise ValueError(f'first_date must be earlier than current first_date: {self._first_date}')
    
    @property
    def last_date(self):
        """
        """
        if self._last_date is None:
            return None
        tz = pytz.timezone(self.db_info.table_tz)
        return tz.localize(self._last_date)
    
    @last_date.setter
    def last_date(self, value):
        """update last date if value is later than current last date
        """
        if self.setup_mode:
            return None
        if not isinstance(value, datetime.datetime):
            value = self.u.time_converter(value)
        if self._last_date is None:
            self._last_date = value
            if self._id is not None:
                self.u.m_conn.update_one({'_id': self._id}, {'$set': {'last_date': self._last_date, 
                                                                      'last_updated': datetime.datetime.now()}})
        else:
            if value > self._last_date:
                self._last_date = value
                self.u.m_conn.update_one({'_id': self._id}, {'$set': {'last_date': self._last_date, 
                                                                      'last_updated': datetime.datetime.now()}})
            else:
                raise ValueError(f'last_date must be later than current last_date: {self._last_date}')
    
    @property
    def stream_tz(self):
        """get the tz of the stream
        """
        return self.stream_data['raw_data']['dataTimeZone']

    def _setup(self):
        """
        """
        self.u.mongo.collection = self.collection
        if self._id is not None:
            self._from_id()
        elif len(self.kwargs) > 3:
            self._from_dict()
        elif self.stream_id is not None:
            self._from_stream_id()
        else:
            raise ValueError('id or stream_id is required')
        if self.stream_id is None:
            raise ValueError('stream_id is required')
        self.stream_id = int(self.stream_id)
    
    def _from_stream_id(self):
        """
        """
        if self.stream_id is None:
            raise ValueError('stream_id is None')
        self.stream_id = int(self.stream_id)
        res = self.u.m_conn.find_one({'stream_id': self.stream_id})
        if res is None or len(res) == 0:
            #allow for new object creation with stream_id defined
            if len(self.kwargs) > 2:
                self._from_dict()
            else:
                raise ValueError(f'no record found for stream_id: {self.stream_id}')
        if res['object_type'] != self.object_type:
            raise ValueError(f'record with stream_id: {self.stream_id} is not a {self.object_type}')
        self.setup_mode = True
        for (k, v) in res.items():
            if k == 'db_info':
                self.db_info = DbInfo(**v)
            else:
                setattr(self, k, v)
        self.setup_mode = False
    
    def _from_id(self):
        """
        """
        if self._id is None:
            raise ValueError('id is None')
        if isinstance(self._id, str):
            self._id = bson.ObjectId(self._id)
        res = self.u.m_conn.find_one({'_id': self._id})
        if res is None or len(res) == 0:
            raise ValueError(f'no record found for id: {self._id}')
        if res['object_type'] != self.object_type:
            raise ValueError(f'record with id: {self._id} is not a {self.object_type}')
        self.setup_mode = True
        for (k, v) in res.items():
            if k == 'db_info':
                self.db_info = DbInfo(**v)
            else:
                setattr(self, k, v)
        self.setup_mode = False
        
    def _check_for_existing(self):
        """check to see if a stream record with this id already exists
        """
        res = self.u.m_conn.find_one({'stream_id': self.stream_id})
        if res is not None:
            for (key, value) in res.items():
                setattr(self, key, value)
            return True
        else:
            return False
    
    def to_dict(self):
        """convert to a dict

            Returns:
            --------
                dict
        """
        skip_args = ['kwargs', 'u', 'collection', 'db_info', '_id', 'setup_mode', 'create_mode']
        res = {}
        for (k, v) in self.__dict__.items():
            if k not in skip_args:
                res[k] = v
        res['db_info'] = self.db_info.to_dict()
        return res
    
    def _from_dict(self):
        """convert from a dict

            Args:
            -----
                kwargs: dict
        """
        self.setup_mode = True
        for (k, v) in self.kwargs.items():
            if k == 'db_info':
                self.db_info = DbInfo(**v)
            else:
                setattr(self, k, v)
        self.setup_mode = False
    
    def save(self):
        """save to db
        """
        # self._setup()
        if self._id is None:
            self._get_location_id()
            already_exists = self._check_for_existing()
            if already_exists:
                print('record already exists, skipping')
            else:
                self._id = self.u.m_conn.insert_one(self.to_dict()).inserted_id
        else:
            self.u.m_conn.update_one({'_id': self._id}, {'$set': self.to_dict()})
    
    def _get_location_id(self):
        """
        """
        if self.location_id is None:
            iso_id = self._get_iso_id(self.iso)
            loc = Location()
            id = loc.get_location_id(self.location, iso_id)
            if id is None:
                loc = Location(name=self.location, iso_id=iso_id)
                loc.save()
                self.location_id = loc.id
            else:
                self.location_id = id
    
    def _get_iso_id(self, name):
        """
        """
        if self.iso_id is not None:
            return self.iso_id
        res = self.u.execute_query("select id, name from iso", dict_cursor=True, fetch='all')
        res = {x['name']: x['id'] for x in res}
        if name not in res:
            raise ValueError(f'iso {name} not found')
        self.iso_id = res[name]
        return self.iso_id
    
    def add_missing_dates(self, dates):
        """
        """
        if not isinstance(dates, list):
            dates = [dates]
        for date in dates:
            if not isinstance(date, datetime.datetime):
                date = self.u.time_converter(date, target='dt')
            self.missing_dates.append(date)
        self.missing_dates = list(set(self.missing_dates))
        self.u.m_conn.update_one({'_id': self._id}, {'$set': {'missing_dates': self.missing_dates}})
        

class DbInfo:
    """
    """
    
    required_args = ['table', 'table_tz', 'date_col', 'columns']

    def __init__(self, **kwargs):
        """
        """
        self.kwargs = kwargs
        self.table = None
        self.table_tz = None
        self.date_col = None
        self.columns = []
        self._setup()
    
    def _setup(self):
        """
        """
        for arg in self.required_args:
            if arg not in self.kwargs:
                raise ValueError(f'{arg} is required')
        for (k, v) in self.kwargs.items():
            setattr(self, k, v)
    
    def to_dict(self):
        """
        """
        res = {}
        for (k, v) in self.__dict__.items():
            res[k] = v
        return res


def update_columns(sr_id):
    """
    """
    columns = ['iso_id', 'location_id', 'obs_type_id', 'stream_id', 'dt', 'mw']
    sr = StreamRecord(stream_id=sr_id)
    sr.db_info.columns = columns
    sr.columns = columns
    sr.save()