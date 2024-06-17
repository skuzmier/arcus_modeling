"""
"""
import datetime
import sys
import re
import pytz

from psycopg2 import sql
import os
from celery import Celery

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))

from utils import Utils
from feature_record import FeatureRecord
from feature_scraper_job import FeatureScraperJob


ampq_url = "amqp://%s:%s@%s:%s" % (
    os.environ["RABBITUSER"],
    os.environ["RABBITPASS"],
    os.environ["RABBITHOST"],
    os.environ["RABBITPORT"],
)
rpc_url = "redis://%s:%s/8" % (os.environ["REDISHOST"], os.environ["REDISPORT"])

app = Celery("nrg_stream", backend=rpc_url, broker=ampq_url)


class FeatureScraperJobBuilder:
    """
    """
    def __init__(self, feature_records=[], feature_ids=[],**kwargs):
        """
        """
        self.u = Utils()
        self.feature_records = feature_records
        self.feature_ids = feature_ids
        self.kwargs = kwargs
        self.jobs = []
        self.failed_jobs = []
        self._setup()

    def _setup(self):
        """
        """
        if len(self.feature_ids) > 0:
            self._get_feature_records()
        if not isinstance(self.feature_records, list):
            self.feature_records = [self.feature_records]

    def run(self, feature_ids=None):
        """
        """
        if feature_ids is not None:
            self.feature_ids = feature_ids
        if isinstance(self.feature_ids, (str, int)):
            self.feature_ids = [self.feature_ids]
        # convert all elemet=nts to ints
        self.feature_ids = [int(x) for x in self.feature_ids]
        # get the feature records
        if len(self.feature_ids) > 0:
            self._get_feature_records()
        # get the date split interval
        for feature_record in self.feature_records:
            self.jobs.extend(self._get_jobs(feature_record))
        self._send_jobs()

    def _get_jobs(self, feature_record):
        """create jobs for a single feature record
        """
        local_tz = pytz.timezone(feature_record.source_tz)
        # do not create jobs if sync is not active and catchup is complete
        if not feature_record.sync_params.sync_active and feature_record.sync_params.catchup_complete:
            return []
        if feature_record.last_date is None:
            s_date = feature_record.sync_params.tgt_first_date
        else:
            s_date = min(
                max(
                    feature_record.sync_params.tgt_first_date,
                    feature_record.last_date,
                ),
                # get current time in the source timezone
                datetime.datetime.now(datetime.timezone.utc)
                .astimezone(local_tz) # NRG Stream API uses local tz for each stream
                .replace(tzinfo=None)
            )
        source_tz = pytz.timezone(feature_record.source_tz)
        # get the current date in the source timezone and remove the timezone info
        e_date = datetime.datetime.now(source_tz).replace(tzinfo=None)
        s_date = s_date.replace(hour=0, minute=0, second=0, microsecond=0) # set to midnight
        e_date = e_date.replace(second=0, microsecond=0) # api does not accept seconds or microseconds
        date_groups = self._get_date_groups(
            s_date, e_date, self._get_date_split(feature_record)
        )
        jobs = []
        for date_group in date_groups:
            job = FeatureScraperJob(
                feature_id=feature_record._id,
                feature_record=feature_record,
                s_date=date_group["s_date"],
                e_date=date_group["e_date"],
            )
            job.save()
            jobs.append(job)
        return jobs
    
    def get_catchup_jobs(self, feature_records=None):
        """Handle situations where extending first_date is necessary, or initial sync had errors
        """
        if feature_records is not None:
            self.feature_records = feature_records
        if not isinstance(self.feature_records, list):
            self.feature_records = [self.feature_records]
        for feature_record in self.feature_records:
            self.jobs.extend(self._get_catchup_jobs(feature_record))
        self._send_jobs()
    
    def _get_catchup_jobs(self, feature_record):
        """create jobs to download data for period between sync_params.tgt_first_date and first_date
        """
        local_tz = pytz.timezone(feature_record.source_tz)
        s_date = feature_record.sync_params.tgt_first_date
        e_date = feature_record.first_date
        e_date = e_date.replace(hour=0, minute=0, second=0, microsecond=0) # set to midnight
        date_groups = self._get_date_groups(s_date, e_date, self._get_date_split(feature_record))
        jobs = []
        for date_group in date_groups:
            job = FeatureScraperJob(
                feature_id=feature_record._id,
                feature_record=feature_record,
                s_date=date_group["s_date"],
                e_date=date_group["e_date"],
            )
            job.save()
            jobs.append(job)
        return jobs

    def _send_jobs(self):
        """
        """
        for job in self.jobs:
            job_dict = job.to_dict()
            job_dict.pop('updated_at')
            res = app.send_task(
                "nrg_stream.scrapers.feature_scraper.run",
                args=[job_dict],
                queue="nrg_stream",
            )
            job.task_id = res.id
            job.save()

    def _get_feature_records(self):
        """
        """
        # get the feature records using either mongo_id or pg_id
        query = {
            "$or": [
                {"_id": {"$in": self.feature_ids}},
                {"pg_id": {"$in": self.feature_ids}},
            ]
        }
        self.u.mongo.collection = "feature_records"
        res = self.u.mongo.find(query)
        feature_record_dicts = [x for x in res]
        self.feature_records = [FeatureRecord(**x) for x in feature_record_dicts]

    def _get_date_split(self, feature_record):
        """get the number of days per api call
        """
        split_days = {1:60, 5:300, 15:900, 30:1800, 60:1800, 1440:1800}
        # regex to find integers in the string
        int_regex = re.compile(r'\d+')
        # find the integers in the string
        freq_min = int_regex.findall(feature_record.freq)
        return split_days[int(freq_min[0])]

    def _get_date_groups(self, s_date, e_date, split_days):
        """
        """
        date_groups = []
        c_date = s_date
        while c_date < e_date:
            job_e_date = min(c_date + datetime.timedelta(days=split_days), e_date)
            date_groups.append({"s_date": c_date, "e_date": job_e_date})
            c_date += datetime.timedelta(days=split_days)
        return date_groups

    def get_failed_jobs(self, window_days=1):
        """
        """
        query = """ SELECT * FROM feature_scraper_jobs WHERE status = 'failed'
                    AND updated_at > (NOW() - INTERVAL '{days} days') """
        query = sql.SQL(query).format(days=sql.Literal(window_days))
        failed_job_dicts = self.u.execute_query(query, dict_cursor=True, fetch='all')
        self.failed_jobs = [FeatureScraperJob(**x) for x in failed_job_dicts]
        print(f"found {len(self.failed_jobs)} failed jobs")

    def restart_failed_jobs(self, window_days=1):
        """
        """
        self.get_failed_jobs(window_days=window_days)
        for job in self.failed_jobs:
            job.status = 'created'
            job_dict = job.to_dict()
            job_dict.pop('updated_at')
            res = app.send_task(
                "nrg_stream.scrapers.feature_scraper.run",
                args=[job_dict],
                queue="nrg_stream",
            )
            job.task_id = res.id
            job.save()
