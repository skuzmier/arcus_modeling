"""Class for creating a batch of model training runs
"""

import sys
import json
import datetime
from collections import Counter

from record import Record
from training_dataset import TrainingDataset
from model_training_run_job import ModelTrainingRunJob
from upstream_model_inference import UpstreamModelInference

from celery import Celery
import os

ampq_url = "amqp://%s:%s@%s:%s/" % (
    os.environ["RABBITUSER"],
    os.environ["RABBITPASS"],
    os.environ["RABBITHOST"],
    os.environ["RABBITPORT"],
)
rpc_url = "redis://%s:%s/8" % (os.environ["REDISHOST"], os.environ["REDISPORT"])

# initialize celery app
app = Celery("erc_model_tasks", backend=rpc_url, broker=ampq_url)


class ModelTrainBatch(Record):
    """
    """
    object_type = 'model_train_batch'

    def __init__(self, id=None, model_config_path=None, region_config_path=None, market_run=None, **kwargs):
        """
        """
        super().__init__(id, object_type='model_train_batch', **kwargs)
        self.model_config = model_config_path
        self.region_config = region_config_path
        self.iso = None
        self.model_family = None
        self.market_run = market_run
        self.kwargs = kwargs
        self.training_run_dates = []
        self._jobs = []
        self.backtest_start = None
        self.backtest_end = None
        self.upstream_model_id = self.kwargs.get('upstream_model_id', None)
        self.dataset_id = None
        self._setup()

    @property
    def jobs(self):
        """check if unloaded job objects exist, if not load them from db
            NOTE: Assumes id will never be set without jobs
        """
        if self._jobs == [] and self.id is not None:
            self._get_jobs()
        return self._jobs

    def _setup(self):
        """
        """
        if self._id is not None:
            self._from_id()
        else:
            self.model_config = self._load_json(self.model_config)
            self.region_config = self._load_json(self.region_config)
            if self.market_run in self.region_config:
                self.region_config = self.region_config[self.market_run]
            self.iso = self.model_config['iso']
            # get backtest start and end dates
            dates = self.model_config['backtest_config']['dates']
            if self.market_run in dates:
                dates = dates[self.market_run]
                self.backtest_start = self.u.time_converter(dates['s_date'], target='dt')
                self.backtest_end = self.u.time_converter(dates['e_date'], target='dt')
            self._add_common_features()

    def _load_json(self, path):
        """ load json file
        """
        with open(path, 'r') as f:
            return json.load(f)

    def _get_dataset(self) -> None:
        """generate training dataset"""
        dates = self._get_dataset_dates()
        if self.upstream_model_id is not None:
            upstream_results = self._get_upstream_model_results()
            kwargs = {'upstream_model_results': upstream_results, 'region_config': self.region_config}
        else:
            kwargs = {}
        dataset = TrainingDataset(
            s_date=dates["s_date"], e_date=dates["e_date"], market_run=self.market_run, **kwargs
        )
        dataset.get_extract(extract_type="arcus")
        dataset.create_ts_record()
        self.dataset_id = str(dataset.id)

    def _get_upstream_model_results(self):
        """Results of upstream model.
            Example: upstream model is used to predict DAM prices, for RTM model training
            Model should be inferenced from train_start (bactest_start - lookback days) to backtest end
        """
        regions = self.model_config['backtest_config']['regions'] # regions to infer, from model config
        region_config = self.region_config
        train_start = self.backtest_start - datetime.timedelta(days=self._get_lookback())
        upstream = UpstreamModelInference(
            batch_id=self.upstream_model_id,
            s_date=train_start,
            e_date=self.backtest_end,
            regions=regions,
            region_config=region_config
        )
        results = upstream.run()
        return results

    def _get_dataset_dates(self) -> dict:
        """Dates for training dataset
            The start date is the backtest start date - lookback days 
            The end date is the backtest end date + retrain frequency 
            Each will have a one day buffer
        """
        dates = self.model_config['backtest_config']['dates']
        if self.market_run in dates:
            dates.update(**dates[self.market_run])
        # get dataset start date
        lookback = self._get_lookback() + 1
        s_date = self.backtest_start - datetime.timedelta(days=lookback)
        # get dataset end date=
        retrain_freq = dates['retrain_freq'] + 1
        e_date = self.backtest_end + datetime.timedelta(days=retrain_freq)
        return {'s_date': s_date, 'e_date': e_date}

    def _get_lookback(self) -> int:
        """get lookback period based on market run
        """
        dates = self.model_config['backtest_config']['dates']
        if self.market_run == 'DAM':
            lookback = dates['DAM_backtest_lookback']
        elif self.market_run == 'RTM':
            lookback = dates['RTM_backtest_lookback']
        return lookback

    def _get_jobs(self):
        """ """
        query = {'parent_id': self.id, 'object_type': 'model_training_run_job'}
        res  = self.u.m_conn.find(query)
        for r in res:
            self._jobs.append(ModelTrainingRunJob(**r))

    def _get_training_run_splits(self):
        """generate s_date and e_date for each training run
        NOTE: Check if subsetting is inclusive or exclusive 
        """
        dates = self.model_config['backtest_config']['dates']
        # allow for market run specific dates
        if self.market_run in dates:
            dates = dates[self.market_run]
        lookback = self._get_lookback()
        c_date = self.u.time_converter(dates['s_date'], target='dt') - datetime.timedelta(days=lookback)
        batch_e_date = self.u.time_converter(dates['e_date'], target='dt')
        retrain_freq = self.model_config['backtest_config']['dates']['retrain_freq']
        while c_date < batch_e_date:
            s_date = c_date
            e_date = c_date + datetime.timedelta(days=self._get_lookback()) - datetime.timedelta(seconds=1)
            e_date = min(e_date, batch_e_date)
            valid_from = e_date + datetime.timedelta(seconds=1)
            valid_until = e_date + datetime.timedelta(days=retrain_freq)
            self.training_run_dates.append({'s_date': s_date, 'e_date': e_date,
                                            'valid_from': valid_from, 'valid_until': valid_until})
            c_date += datetime.timedelta(days=retrain_freq)

    def _parse_model_config(self) -> dict:
        """parse model config and 
        """
        res = {}
        test_train_split = self.model_config['backtest_config']['test_train_split']
        res['test_train_split'] = test_train_split
        res['scaler'] = self.model_config['scaler']
        res['model_config'] = self.model_config[self.market_run]
        return res

    def _get_job_config(self, dates, region):
        """ """
        config = self._parse_model_config()
        region_config = self.region_config[region]
        config['region_config'] = region_config
        config['s_date'] = dates['s_date']
        config['e_date'] = dates['e_date']
        config['valid_from'] = dates['valid_from']
        config['valid_until'] = dates['valid_until']
        config['region'] = region
        config['dataset_id'] = self.dataset_id
        config['parent_id'] = self.id
        return config

    def _create_jobs(self):
        """
        """
        for dates in self.training_run_dates:
            for region in self.model_config['backtest_config']['regions']:
                job_config = self._get_job_config(dates, region)
                job = ModelTrainingRunJob(config=job_config)
                job.save()
                self._jobs.append(job)

    def to_dict(self) -> dict:
        """
        """
        return {
            'model_config': self.model_config,
            'region_config': self.region_config,
            'market_run': self.market_run,
            'training_run_dates': self.training_run_dates,
            'backtest_start': self.backtest_start,
            'backtest_end': self.backtest_end,
            'object_type': 'model_train_batch',
            'created_at': self.created_at,
            'dataset_id': self.dataset_id,
        }

    def _insert_jobs(self):
        """batch insert jobs, update job ids
        """
        docs = [j.to_dict() for j in self.jobs]
        results = self.u.m_conn.insert_many(docs)
        inserted_ids = results.inserted_ids
        for j, r in zip(self.jobs, inserted_ids):
            j._id = r

    def get_job_status(self):
        """current job status counters
        """
        self._jobs = []
        self._get_jobs()
        status = [j.status for j in self.jobs]
        return Counter(status)

    def send_jobs(self, only_failed=False):
        """send jobs to queue
        """
        if only_failed:
            for job in self.jobs:
                if job.status == 'failed':
                    self._send_job(job)
        else:
            for job in self.jobs:
                self._send_job(job)
                if job.task_id is None:
                    print(f"Failed to send job {job.id} to queue")

    def alter_job_status(self, old_status, new_status):
        """alter job status
        """
        self._get_jobs()
        for job in self.jobs:
            if job.status == old_status:
                job.status = new_status
                job.save()

    def restart_failed_jobs(self):
        """restart failed jobs
        """
        self._get_jobs()
        self.send_jobs(only_failed=True)

    def _send_job(self, job):
        """send job to queue
        """
        task = app.send_task(
            "model_tasks.run_model_training_run",
            args=[{"id": str(job.id)}],
            queue="erc_model_tasks",
            priority=6 #low priority
        )
        job.task_id = task.id

    def _add_common_features(self) -> None:
        """add common features to the config list of the regions in region_config
        NOTE; Assumes region names are in all caps, and no other keys in region_config
        """
        if 'common_features' not in self.region_config:
            return None
        common_features = self.region_config['common_features']
        for (region, config) in self.region_config.items():
            if region == 'common_features':
                continue
            config['features'] += common_features
        return None

    def run(self):
        """ """
        self._get_dataset()
        self.save()  # save to get id
        self._get_training_run_splits()
        self._create_jobs()
        self._insert_jobs()
        self.save()


# ""663331216d8336d73c11f4dc"" Test ModelTrainBatch id
