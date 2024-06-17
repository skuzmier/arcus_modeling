"""
"""

import os
from psycopg2 import sql
import datetime
import sys

sys.path.append(os.path.expanduser('~/ercot_virts/utils'))
sys.path.append(os.path.expanduser('~/ercot_virts/nrg_stream'))
sys.path.append(os.path.expanduser('~/ercot_virts/nrg_stream/scrapers'))

from utils import Utils
from stream_record import StreamRecord


class ScraperJob:
    u = Utils()

    """
    """
    def __init__(self, id=None, stream_id=None, s_date=None, e_date=None, **kwargs):
        """
        """
        self.id = id
        self.stream_id = stream_id
        self.scraper = None
        self.s_date = s_date
        self.e_date = e_date
        self.kwargs = kwargs
        self._status = None
        self._worker_ip = None
        self.updated_at = None
        self.setup_mode = False
        self._setup()
    
    
    @property
    def status(self):
        """
        """
        return self._status
    
    @status.setter
    def status(self, value):
        """
        """
        if value not in ['created', 'running', 'complete', 'failed']:
            raise ValueError(f'invalid status {value}')
        self._status = value
        if not self.setup_mode:
            self.u.update_record(table='scraper_jobs', 
                                 data={'status': value, 'updated_at': datetime.datetime.now()},
                                 id=self.id)
            self.updated_at = datetime.datetime.now()
    
    @property
    def worker_ip(self):
        """
        """
        return self._worker_ip
    
    @worker_ip.setter
    def worker_ip(self, value):
        """
        """
        self._worker_ip = value
        if not self.setup_mode:
            self.u.update_record(self, table='scraper_jobs', 
                                 data={'worker_ip': value, 'updated_at': datetime.datetime.now()},
                                 id=self.id)
            self.updated_at = datetime.datetime.now()
    
    def _setup(self):
        """
        """
        if self.id is None:
            self._setup_new()
        else:
            self._from_id()
    
    def _setup_new(self):
        """create a new record in the scraper_jobs table
        """
        self.setup_mode = True
        self.status = 'created'
        for (k, v) in self.kwargs.items():
            setattr(self, k, v)
        self.setup_mode = False
        self._get_scraper()
        if not self.kwargs.get('debug', False):
            self._save()
    
    def _get_scraper(self):
        """get the scraper name for a given stream id
        """
        sr = StreamRecord(stream_id=self.stream_id)
        self.scraper = sr.scraper
    
    def _from_id(self):
        """
        """
        query = sql.SQL(""" SELECT * FROM scraper_jobs WHERE id = {id} """).format(id=sql.Literal(self.id))
        record = self.u.execute_query(query, dict_cursor=True, fetch='one')
        if len(record) == 0:
            raise ValueError(f'no scraper job with id {self.id}')
        else:
            self.setup_mode = True
            for (key, value) in record.items():
                setattr(self, key, value)
            self.setup_mode = False
    
    def to_dict(self, include_id=False):
        """
        """
        record = {'stream_id': self.stream_id, 's_date': self.s_date,
                  'e_date': self.e_date, 'status': self.status, 'scraper':self.scraper}
        if include_id:
            record['id'] = self.id
        return record
    
    def _save(self):
        """
        """
        self.id = self.u.insert_dict_record(table='scraper_jobs', 
                                            record_dict=self.to_dict(include_id=False),
                                            return_id=True)
        

        

class ScraperJobs:
    """
    """
    def __init__(self, **kwargs):
        """
        """
        self.u = Utils()
        self.kwargs = kwargs
        self.jobs = []
        self.s_date = None
        self.e_date = None
        self.scraper = None
        self.split_days = None
        self.stream_ids = []
        self.date_splits = []
        self.u.mongo.collection = 'stream_records'
        if len(self.kwargs) > 0:
            for (k, v) in self.kwargs.items():
                setattr(self, k, v)

    def _setup(self):
        """
        """
        self.u.mongo.collection = 'stream_records'
        self._validate()
        self._get_stream_ids()
        self._create_jobs()
    
    def _validate_setup(self):
        """
        """
        self.s_date = self.u.time_converter(self.s_date, target='dt')
        self.e_date = self.u.time_converter(self.e_date, target='dt')
        if self.s_date >= self.e_date:
            raise ValueError('start date must be before end date')
        if not isinstance(self.split_days, int):
            raise ValueError('split days must be an integer')
        if not isinstance(self.stream_ids, list):
            raise ValueError('stream ids must be a list')
        if len(self.stream_ids) == 0:
            raise ValueError('stream ids must not be empty')

    def get_stream_ids(self, scraper, iso='ERCOT', table=None):
        """
        """
        res = []
        search_dict = {'object_type': 'stream_record', 'scraper': scraper}
        if iso is not None:
            search_dict['iso'] = iso
        if table is not None:
            search_dict['db_info.table'] = table
        streams = self.u.m_conn.find(search_dict) 
        for stream in streams:
            res.append(stream['stream_id'])
        return res
    
    def create_scraper_jobs(self, s_date, e_date, split_days, stream_ids):
        """
        """
        self.s_date = s_date
        self.e_date = e_date
        self.split_days = split_days
        self.stream_ids = stream_ids
        self._validate_setup()
        self._get_date_splits()
        for date_split in self.date_splits:
            for stream_id in self.stream_ids:
                job = ScraperJob(stream_id=stream_id, s_date=date_split['s_date'], 
                           e_date=date_split['e_date'], status='created',
                           debug=self.kwargs.get('debug', False))
                self.jobs.append(job)

    def _get_date_splits(self):
        """
        """
        c_date = self.s_date
        while c_date < self.e_date:
            e_date = min(c_date + datetime.timedelta(days=self.split_days), self.e_date)
            self.date_splits.append({'s_date': c_date, 'e_date': e_date})
            c_date += datetime.timedelta(days=self.split_days)
    
        



""" fix dates not synced"""
class ScraperUpdater:
    """
    """
    def __init__(self, scraper_ids):
        """
        """
        self.u = Utils()
        self.scraper_ids = scraper_ids
        self.stream_records = []
        self._setup()
    
    def _setup(self):
        """
        """
        self.u.mongo.collection = 'stream_records'
        self._get_stream_records()
    
    def run(self):
        """update stream records from db
        """
        cnt = 0
        for stream_record in self.stream_records:
            cnt += 1
            self._update_date_range(stream_record)
            if cnt % 10 == 0:
                print(f'updating stream record {cnt} of {len(self.stream_records)}')
    
    def _get_stream_records(self):
        """
        """
        for scraper_id in self.scraper_ids:
            sr = StreamRecord(stream_id=scraper_id)
            self.stream_records.append(sr)
    
    def _update_date_range(self, stream_record):
        """
        """
        query = sql.SQL(""" SELECT min(dt) as min_dt, max(dt) as max_dt FROM {table} 
                            WHERE node_id = {location_id}""").format(
                                table=sql.Identifier(stream_record.db_info.table),
                                location_id=sql.Literal(stream_record.location_id))
        record = self.u.execute_query(query, dict_cursor=True, fetch='one')
        stream_record.first_date = record['min_dt']
        stream_record.last_date = record['max_dt']