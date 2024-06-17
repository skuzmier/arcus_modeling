"""
"""

import sys
import os
import datetime
import time

import pandas as pd
import numpy as np
from celery import Celery
from io import StringIO
import bson

from model_training_run import ModelTrainingRun
from training_dataset  import TrainingDataset


# setup rabbitmq and redis urls
ampq_url = "amqp://%s:%s@%s:%s/" % (
    os.environ["RABBITUSER"],
    os.environ["RABBITPASS"],
    os.environ["RABBITHOST"],
    os.environ["RABBITPORT"],
)
rpc_url = "redis://%s:%s/8" % (os.environ["REDISHOST"], os.environ["REDISPORT"])

# initialize celery app
app = Celery("erc_model_tasks", backend=rpc_url, broker=ampq_url)

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
from utils import Utils


class ModelInference:
    """
    """

    def __init__(self, batch_id, s_date, e_date, region, market_run=None, local=False, **kwargs):
        """ 
            Args:
            -----
                batch_id: str, bson.ObjectId
                    model training batch id
                s_date: str, datetime.datetime
                    start date for inference
                e_date: str, datetime.datetime
                    end date for inference
                market_run: str
                    market run for inference
                region: str
                    region for inference
                local: bool
                    run locally or send to remote worker
                kwargs: dict
                    additional arguments
        """
        self.u = Utils()
        self.batch_id = batch_id
        self.s_date = s_date
        self.e_date = e_date
        self.market_run = market_run
        self.region = region
        self.kwargs = kwargs
        self.local = local
        self.training_runs = []
        self.dataset = None
        self.downstream_setup = kwargs.get('downstream_setup', False)
        self.mapping = None
        self._results = pd.DataFrame()
        self.task = None
        self._setup()

    @property
    def results(self):
        """
        """
        # if local or results already exist, return them
        if len(self._results) > 0:
            return self._results
        # if task is not None, return the results
        elif self.task is not None:
            return self._get_async_results()
        return None

    @results.setter
    def results(self, value):
        """
        """
        self._results = value

    @property
    def status(self):
        """get the status of the task
        """
        if self.task is not None:
            return self.task.status
        return None

    def _setup(self):
        """
        """
        self._check_dates()
        if isinstance(self.batch_id, str):
            self.batch_id = bson.ObjectId(self.batch_id)
        if self.local or self.kwargs.get('is_celery', False): # if running locally or in celery
            self._get_batch_model_runs()
            self._get_training_dataset()
        if not self.local and not self.kwargs.get('is_celery', False):
            self._apply_async()
    
    def _check_dates(self):
        """format dates and check s_date < e_date
        """
        self.s_date = self.u.time_converter(self.s_date, target="dt")
        self.e_date = self.u.time_converter(self.e_date, target="dt")
        if self.s_date >= self.e_date:
            raise ValueError("s_date must be less than e_date")

    def _get_batch_model_runs(self):
        """load the batch of model training runs
        """
        # get model training runs for batch
        self.training_runs = ModelTrainingRun.search(
            parent_id=self.batch_id,
            object_type="model_training_run",
            region=self.region,
        )
        if len(self.training_runs) == 0:
            raise ValueError("No model training runs found for batch")

    def _get_training_dataset(self):
        """load the training dataset
        """
        dataset_id = self.training_runs[0].dataset_id
        self.dataset = TrainingDataset(id=dataset_id)

    def _get_model_period_mapping(self) -> list:
        """get the model period mapping
            For each model training run with a valid_from between s_date and e_date
            get a dict with the model run and the corresponding valid_from and valid_to.
            Sort the list by period start date.
        """
        self.mapping = []
        for run in self.training_runs:
            if run.valid_until >= self.s_date and run.valid_from <= self.e_date:
                self.mapping.append(
                    {
                        "model_run": run,
                        "valid_from": run.valid_from,
                        "valid_until": run.valid_until,
                    }
                )
        self.mapping = sorted(self.mapping, key=lambda x: x["valid_from"])
        return self.mapping
    
    def _extend_model_period_mapping(self, mapping) -> list:
        """extend the model period mapping for backtesting with downstream models
            This creates minor lookahead bias in the first training run but is necessary until larger refactor
        """
        #set the earliest valid_from date date to the s_date
        mapping[0]['valid_from'] = self.s_date
        return mapping

    def _get_target_data(self, model_run: ModelTrainingRun) -> dict:
        """get the target data for the model run, 
            during the valid period, up until the e_date
        """
        
        s_date = max(self.s_date, model_run.valid_from)
        e_date = min(model_run.valid_until, self.e_date)
        feature_names = model_run.config['region_config']['features']
        tgt_col = model_run.config['region_config']['tgt_col']
        features = self.dataset.subset(s_date, e_date, columns=feature_names)
        actuals = self.dataset.subset(s_date, e_date, columns=tgt_col)
        dates = features.index
        return {"features": features, "actuals": actuals, "dates": dates}

    def _inference_model(self, model_run: ModelTrainingRun, data: dict) -> pd.DataFrame:
        """run the inference for a model run

        Args:
        -----
            model_run: ModelTrainingRun
                model training run to run inference for
            data: dict
                dictionary with features and actuals
        
        Returns:
        --------
            pd.DataFrame
                dataframe with preds and actuals and datetime index
        """
        scaled_features = model_run.feature_scaler.transform(data['features'])
        scaled_results = model_run.model.predict(scaled_features)
        results = model_run.target_scaler.inverse_transform(scaled_results.reshape(-1, 1)).ravel()
        df = pd.DataFrame(results, index=data['dates'], columns=['preds'])
        df['actuals'] = data['actuals']
        return df


    def to_dict(self):
        """convert to dictionary for JSON serialization
        """
        batch_id = str(self.batch_id) # convert to string for json serialization
        return {
            'batch_id': batch_id,
            's_date': self.s_date,
            'e_date': self.e_date,
            'market_run': self.market_run,
            'region': self.region,
            'sent_at': datetime.datetime.now(),
            'local': self.local,
            'downstream_setup': self.downstream_setup
        }

    def _apply_async(self):
        """convert to dictionary and send to Celery queue
        """
        self.task = app.send_task('model_tasks.inference_model',
                                   args=[self.to_dict()],
                                   queue='erc_model_tasks',
                                   priority=1)

    def _run_local(self):
        """apply the inference to the model runs locally
        """
        mapping = self._get_model_period_mapping()
        #extebd vaid period of first model run if downstream setup
        if self.downstream_setup:
            mapping = self._extend_model_period_mapping(mapping)
        for m in mapping:
            data = self._get_target_data(m['model_run'])
            df = self._inference_model(m['model_run'], data)
            self.results = pd.concat([self.results, df])
    
    def _get_async_results(self):
        """get the results from the async task
        """
        if self.task.state == 'SUCCESS':
            results = self._format_results()
        elif self.task.failed():
            print('Task failed with error: ', self.task.traceback)
            results = None
        elif self.task.state == 'PENDING':
            print('Task is still pending')
            results = None
        else:
            print(f'unexpected task state: {self.task.state}')
            results = None
        return results
    
    def get_rmse(self):
        """calculate the RMSE for the results
        """
        if len(self.results) == 0:
            return None
        rmse = np.sqrt(np.mean((self.results['preds'] - self.results['actuals'])**2))
        return rmse

    def _format_results(self) -> pd.DataFrame:
        """Get the results from the async task and format them as a DataFrame
        """
        results = self.task.get()
        results = pd.read_json(StringIO(results))
        #remove task from redis
        self.task.forget()
        #convert datetime
        results['ldt'] = pd.to_datetime(results['ldt'], unit='ms')
        results.set_index('ldt', inplace=True)
        results['region'] = self.region
        self.results = results
        return results
    
    def wait_for_results(self):
        """wait for the results to be available
        """
        while self.task.state == 'PENDING':
            time.sleep(0.05)

    def run(self, local=False, is_celery=False) -> None:
        """run the model inference or send to remote worker

        Args:
        -----
            local: bool
                run locally or send to remote worker
            is_celery: bool
                is this the celery remote worker,
        """
        if local or is_celery:
            self._run_local()
            if is_celery:
                return self.results.reset_index().to_json()
        else:
            self._apply_async()