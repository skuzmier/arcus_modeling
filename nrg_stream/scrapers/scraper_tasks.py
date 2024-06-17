"""celery tasks file for 
"""
from __future__ import absolute_import, unicode_literals
from celery import Celery, chain
from celery.schedules import crontab
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.redis import RedisIntegration
import os
import sentry_sdk
import datetime

import sys
sys.path.append(os.path.expanduser('~/ercot_virts/utils'))
sys.path.append(os.path.expanduser('~/ercot_virts/nrg_stream'))
sys.path.append(os.path.expanduser('~/ercot_virts/nrg_stream/scrapers'))

from utils import Utils
from feature_scraper import FeatureScraper
from nrg_sync_manager import SyncManager
from token_pool import NRGTokenPool
from update_schedule_inference import UpdateScheduleInference


sentry_sdk.init(
    "https://c88a22cdaa3b498eb81b8cc8ee6b1af4@sentry.io/1884626",
    integrations=[CeleryIntegration(), RedisIntegration()],
)

ampq_url = "amqp://%s:%s@%s:%s/" % (
    os.environ["RABBITUSER"],
    os.environ["RABBITPASS"],
    os.environ["RABBITHOST"],
    os.environ["RABBITPORT"],
)
rpc_url = "redis://%s:%s/8" % (os.environ["REDISHOST"], os.environ["REDISPORT"])

app = Celery("nrg_stream", backend=rpc_url, broker=ampq_url)
app.conf.update(task_acks_late=True, worker_prefetch_multiplier=1)


app.conf.beat_schedule = {
    #
    "generate_jobs": {
        "task": "nrg_stream.scrapers.generate_jobs",
        "schedule": crontab(minute='*/15'),
        "args": (False),
        "options": {"queue": "nrg_stream"},
    },
    "create_sync_inference_jobs": {
        "task": "nrg_stream.scrapers.create_sync_inference_jobs",
        "schedule": crontab(minute='*/15'),
        "args": (False),
        "options": {"queue": "nrg_stream"},
    }
        
    # figuring out correct implementation
    #"refresh_token": {
    #    "task": "nrg_stream.scrapers.",
    #    "schedule": crontab(hour=14, minute=5),
    #    "args": (False),
    #    "options": {"queue": "model_tasks", 'priority': 0},
    #},
}


# generate scraper jobs
@app.task(bind=True, name="nrg_stream.scrapers.generate_jobs")
def generate_jobs(self):
    """
    """
    s = SyncManager()
    s.run()
    for job in s.jobs:
        job_rec = job.to_dict()
        #send job to scraper
        scrape_feature.s(job_rec).apply_async(queue='nrg_stream')

@app.task(bind=True, name="nrg_stream.scrapers.feature_scraper.run")
def scrape_feature(self, config_dict):
    """
    """
    if config_dict is None:
        raise ValueError("config_dict is None")
    if not isinstance(config_dict, dict):
        raise ValueError("config_dict is not a dict")
    if 'id' not in config_dict:
        raise ValueError("config_dict does not have a job id")
    if 'scraper' not in config_dict:
        raise ValueError("config_dict does not have a scraper")
    if config_dict['scraper'] == 'nrg_feature':
        f = FeatureScraper(scraper_job_id=config_dict['id'])
        f.run()
    else:
        raise ValueError("invalid scraper type %s" % config_dict['scraper'])

@app.task(bind=True, name='nrg_stream.scrapers.refresh_token')
def refresh_token(self):
    """
    """
    t = TokenPool()
    t.refresh_token('nrg_auth')

# run inference to estimate sync params
@app.task(bind=True, name='nrg_stream.scrapers.update_inference')
def update_inference(self, config_dict):
    """
    """
    u = UpdateScheduleInference(id=config_dict['_id'])
    u.run()

# create update inference jobs
@app.task(
    bind=True,
    name="nrg_stream.scrapers.create_sync_inference_jobs",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3},
    retry_backoff=True,
    retry_jitter=True,
    retry_backoff_max=60,
)
def create_sync_inference_jobs(self):
    """
    """
    s = SyncManager()
    sync_inference_records = s.get_update_inference_jobs()
    if len(sync_inference_records) == 0:
        print('no sync inference jobs found')
        return None
    for record in sync_inference_records:
        created_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        job_dict = {'_id': str(record.id), 'created_at': created_at}
        #send job to scraper
        update_inference.s(job_dict).apply_async(queue='nrg_stream')
