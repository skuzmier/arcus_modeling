"""create new scrape jobs on an automated schedule
"""


import sys
import os
import datetime
from celery import Celery

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.appennd(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))

from utils import Utils
from feature_record import FeatureRecord
from feature_scrape_job_builder import FeatureScraperJobBuilder
from update_schedule_inference import UpdateScheduleInference

ampq_url = "amqp://%s:%s@%s:%s" % (
    os.environ["RABBITUSER"],
    os.environ["RABBITPASS"],
    os.environ["RABBITHOST"],
    os.environ["RABBITPORT"],
)
rpc_url = "redis://%s:%s/8" % (os.environ["REDISHOST"], os.environ["REDISPORT"])

app = Celery("nrg_stream", backend=rpc_url, broker=ampq_url)


class SyncManager:
    """
    """
    def __init__(self, **kwargs):
        """
        """
        self.u = Utils()
        self.kwargs = kwargs
        self.feature_records = []
        self.jobs = []
        self.r = self.u.get_redis_conn(db=1)
        self._setup()

    def _setup(self):
        """
        """
        self.u.mongo.collection = 'feature_records'

    def run(self):
        """
        """
        # check if sync is active
        if self.r.get('nrg_sync_active') == '0':
            return True
        self._get_catchup_only_records()
        self._get_standard_records()
        self._get_jobs()

    def _get_catchup_only_records(self):
        """
        """
        query = {'sync_params.sync_active': False, 'sync_params.catchup_complete': False}
        res = self.u.m_conn.find(query)
        res = [FeatureRecord(**x) for x in res]
        self.feature_records.extend(res)

    def _get_standard_records(self):
        """
        """
        query = {'sync_params.sync_active': True, 'sync_params.next_sync': {'$lte': datetime.datetime.now()}, 'sync_params.pending_sync': False}
        res = self.u.m_conn.find(query)
        res = [FeatureRecord(**x) for x in res]
        self.feature_records.extend(res)

    def _get_jobs(self):
        """
        """
        f = FeatureScraperJobBuilder(feature_records=self.feature_records)
        f.run()
        self.jobs = f.jobs

    def get_update_inference_jobs(self):
        """
        """
        self.u.mongo.collection = "update_schedule_inference"
        query = {"active": True, "next_run": {"$lte": datetime.datetime.now()}}
        res = self.u.m_conn.find(query)
        records = [UpdateScheduleInference(**x) for x in res]
        return records

