"""
"""

import sys
import os 
import datetime

sys.path.append("/home/ubuntu/ercot_virts/utils")
sys.path.append("/home/ubuntu/ercot_virts/nrg_stream/scrapers")

from utils import Utils
from feature_record import FeatureRecord
from celery import Celery


ampq_url = "amqp://%s:%s@%s:%s" % (
    os.environ["RABBITUSER"],
    os.environ["RABBITPASS"],
    os.environ["RABBITHOST"],
    os.environ["RABBITPORT"],
)
rpc_url = "redis://%s:%s/8" % (os.environ["REDISHOST"], os.environ["REDISPORT"])

app = Celery("nrg_stream", backend=rpc_url, broker=ampq_url)


class FeatureScraperJob:
    """
    """

    u = Utils()

    def __init__(self, id=None, feature_id=None, feature_record=None, s_date=None, e_date=None, **kwargs):
        """
        """
        self.id = id
        self.feature_id = feature_id
        self._feature_record = feature_record
        self.s_date = s_date
        self.e_date = e_date
        self.e_date = e_date
        self.kwargs = kwargs
        self.scraper = None
        self._status = None
        self._worker_ip = None
        self.updated_at = None
        self.setup_mode = False
        self._task_id = None
        self.priority = 2
        self._setup()
    
    def __repr__(self):
        """
        """
        return f"FeatureScraperJob(id={self.id}, feature_id={self.feature_id}, s_date={self.s_date}, e_date={self.e_date}, status={self.status}"
    
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
            self.u.update_record(table='feature_scraper_jobs', 
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
            self.u.update_record(table='feature_scraper_jobs', 
                                 data={'worker_ip': value, 'updated_at': datetime.datetime.now()},
                                 id=self.id)
            self.updated_at = datetime.datetime.now()
    
    @property
    def task_id(self):
        """
        """
        return self._task_id
    
    @task_id.setter
    def task_id(self, value):
        """
        """
        self._task_id = value
        if not self.setup_mode:
            self.u.update_record(table='feature_scraper_jobs', 
                                 data={'task_id': value, 'updated_at': datetime.datetime.now()},
                                 id=self.id)
            self.updated_at = datetime.datetime.now()
    
    @property
    def feature_record(self):
        """
        """
        if self._feature_record is None:
            fr = FeatureRecord(id=self.feature_id)
            self._feature_record = fr
        return self._feature_record
        
    def _setup(self):
        """
        """
        if len(self.kwargs) > 2 and self.feature_record is None and self.id is not None:
            self._from_dict(self.kwargs)
        elif self.id is not None:
            self._from_id()
        else:
            self._setup_new()
        self.s_date = self.u.time_converter(self.s_date, target='dt')
        self.e_date = self.u.time_converter(self.e_date, target='dt')
    
    def _setup_from_feature_record(self):
        """
        """
        self.feature_id = self.feature_record.pg_id
        self.scraper = self.feature_record.scraper
        self.priority = self.feature_record.sync_params.priority
        self._status = 'created'
    
    def _setup_new(self):
        """
        """
        self.setup_mode = True
        self.status = 'created'
        if self.feature_record is not None:
            self._setup_from_feature_record()
        for k, v in self.kwargs.items():
            setattr(self, k, v)
        if self.scraper is None:
            raise ValueError('scraper is required')
        self.setup_mode = False
        self.validate_params()
    
    def _from_dict(self, data):
        """
        """
        self.setup_mode = True
        for k, v in data.items():
            setattr(self, k, v)
        self.setup_mode = False
    
    def _from_id(self):
        """
        """
        query = f"select * from feature_scraper_jobs where id = {self.id}"
        record = self.u.execute_query(query, dict_cursor=True, fetch='one')
        for k, v in record.items():
            setattr(self, k, v)
        self.s_date = self.s_date
        self.e_date = self.e_date
    
    def validate_params(self):
        """
        """
        if self.s_date is None or self.e_date is None:
            raise ValueError('s_date and e_date are required')
        if self.s_date > self.e_date:
            raise ValueError('s_date must be before e_date')
        if self.feature_id is None:
            raise ValueError('feature_id is required')
        if not isinstance(self.feature_id, int):
            raise ValueError('feature_id must be an integer')
        if self.scraper is None:
            raise ValueError('scraper is required')
    
    def to_dict(self, as_str=False):
        """
        """
        str_params = ['s_date', 'e_date']
        required_params = ['feature_id', 's_date', 'e_date', '_status', 'scraper', 'worker_ip']
        res = {}
        for k, v in self.__dict__.items():
            if k in required_params:
                if k in str_params or as_str:
                    res[k] = str(v)
                else:
                    res[k.lstrip('_')] = v
        if as_str:
            res['updated_at'] = str(datetime.datetime.now(datetime.timezone.utc))
        else:
            res['updated_at'] = datetime.datetime.now(datetime.timezone.utc)
        if self.id is not None:
            res['id'] = self.id
        return res
    
    def save(self):
        """
        """
        if self.id is None:
            self.id = self.u.insert_dict_record(table='feature_scraper_jobs', 
                                                record_dict=self.to_dict(),
                                                return_id=True)
        else:
            self.u.update_record(table='feature_scraper_jobs', 
                                 data=self.to_dict(),
                                 id=self.id)
    
    def send_to_queue(self):
        """
        """
        job_dict = self.to_dict(as_str=True)
        job_dict.pop('updated_at')
        res = app.send_task(
            "nrg_stream.scrapers.feature_scraper.run",
            args=[job_dict],
            queue="nrg_stream",
        )
        self.task_id = res.id
        self.save()
