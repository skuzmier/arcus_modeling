"""Inference upstream mode as part of dataset creation
    This is seperated from TrainingDataset to prevent a circular import error with ModelInference
"""

import os
import sys
import pandas as pd
import time
from inference import ModelInference

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
from utils import Utils


class UpstreamModelInference():
    """
    """
    def __init__(self, batch_id, s_date, e_date, regions, region_config, **kwargs):
        """
            Args:
            -----
                batch_id: str, bson.ObjectId
                    model training batch id
                s_date: str, datetime.datetime
                    start date for inference
                e_date: str, datetime.datetime
                    end date for inference
                region_config: dict
                    region configuration from ModelTrainingBatch
                kwargs: dict
                    additional arguments 
        """
        self.u = Utils()
        self.batch_id = batch_id
        self.s_date = s_date
        self.e_date = e_date
        self.regions = regions
        self.region_config = region_config
        self.kwargs = kwargs
        self.df = pd.DataFrame()
    
    def _inference_upstream_model(self, region, pred_col_name) -> pd.DataFrame:
        """
            Args:
            -----
                region: str
                    region configuration
                pred_col_name: str
                    name of the prediction column

        """
        inference = ModelInference(
            batch_id=self.batch_id,
            s_date=self.s_date,
            e_date=self.e_date,
            region=region,
            downstream_setup=True
        )
        inference.run()
        inference.wait_for_results()
        results = inference.results[['preds']]
        if results is None:
            raise ValueError(f"Inference results are None, task status: {inference.task.status}")
        results.rename(columns={'preds': pred_col_name}, inplace=True)
        return results

    def run(self):
        """
        """
        for region in self.regions:
            pred_col_name = self.region_config[region]['input_pred_col']
            results = self._inference_upstream_model(region, pred_col_name=pred_col_name)
            self.df = pd.concat([self.df, results], axis=1)
        return self.df
    

    
