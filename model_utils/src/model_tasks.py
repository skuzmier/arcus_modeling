"""celery tasks file for 
"""

from __future__ import absolute_import, unicode_literals
from celery import Celery, chain
import json
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.redis import RedisIntegration
import os
import sentry_sdk

from model_training_run_job import ModelTrainingRunJob
from model_training_run import ModelTrainingRun
from inference import ModelInference

#connect to sentry for error logging
sentry_sdk.init(
    "https://c88a22cdaa3b498eb81b8cc8ee6b1af4@sentry.io/1884626",
    integrations=[CeleryIntegration(), RedisIntegration()],
)

#setup rabbitmq and redis urls
ampq_url = "amqp://%s:%s@%s:%s/" % (
    os.environ["RABBITUSER"],
    os.environ["RABBITPASS"],
    os.environ["RABBITHOST"],
    os.environ["RABBITPORT"],
)
rpc_url = "redis://%s:%s/8" % (os.environ["REDISHOST"], os.environ["REDISPORT"])

#initialize celery app
app = Celery("erc_model_tasks", backend=rpc_url, broker=ampq_url)
app.conf.update(task_acks_late=True, worker_prefetch_multiplier=1) #allow for long running tasks, limit each worker to one task at a time


@app.task(bind=True, name='model_tasks.run_model_training_run')
def run_model_training_run(self, config_dict):
    """run model training
        NOTE: May be cleaner to refactor ModelTrainingRunJob to be a subclass of ModelTrainingRun
              or have ModelTrainingRun accept a job_id and load the job config
    """
    job = ModelTrainingRunJob(id=config_dict.get('id'))
    try:
        job.status = 'running'
        training_run = ModelTrainingRun(config=job.config)
        training_run.run()
        job.status = 'complete'
    except Exception as e:
        job.status = 'failed'
        raise e


@app.task(bind=True, name='model_tasks.inference_model')
def inference_model(self, config_dict):
    """
    """
    config_dict['is_celery'] = True
    inference = ModelInference(**config_dict)
    results = inference.run(is_celery=True) # should I refacotr to remove is_celery?, excplit but not needed
    return results


