"""
"""

import sys
import pandas as pd
from ts_record import TSRecord
from record import Record
import time

sys.path.append("/home/ubuntu/ercot_virts/utils")
from utils import Utils
from arcus_extract import ArcusErcotExtractBuilder


class TrainingDataset(Record):
    """
    """
    object_type = 'training_dataset'

    def __init__(self, id=None, s_date=None, e_date=None, market_run=None, **kwargs):
        """
        Args:
        -----
            id: str, bson.ObjectId uuid of record
            s_date: str, datetime.datetime 
                start date
            e_date: str, datetime.datetime
                end date
        """
        super().__init__(id, object_type='training_dataset', **kwargs)
        self.kwargs = kwargs
        self.s_date = s_date
        self.e_date = e_date
        self.market_run = market_run
        self.iso = self.kwargs.get('iso', 'ERCOT') # TODO: Refactor when porting to multi iso setup
        self.region_config = self.kwargs.get('region_config', None)
        self.upstream_model_results = self.kwargs.get('upstream_model_results', None)
        self.ts_record_id = None
        self._ts_record = None
        self.categorical_features = []
        self.numerical_features = []
        self._df = None #used for creation of ts_record and intermediate processing by external methods
        self._ignore_params = []
        self.setup_mode = False
        self._setup()
    
    def __repr__(self):
        """ A human readable representation of the object"""
        repr_str =  (f"TrainingDataset: {self.id}, s_date: {self.s_date}, "
                     f" e_date: {self.e_date}, market_run: {self.market_run}. "
                     f" created at: {self.created_at}, column_count: {len(self.df.columns)}")
        return repr_str
        
    
    @property
    def ts_record(self):
        """ """
        if self._ts_record is None:
            self._ts_record = TSRecord(id=self.ts_record_id)
        return self._ts_record
    
    @property
    def df(self):
        """ 
        """
        if self.ts_record_id is None:
            return self._df
        return self.ts_record.ts_obj
    
    @df.setter
    def df(self, df):
        """
        """
        if self.ts_record_id is not None:
            raise ValueError("Cannot set df if ts_record is already set")
        self._df = df
    
    def _setup(self):
        """
        """
        if self._id is not None:
            self._from_id()
        # validate dates
        self._validate_dates()
        #validate market run
        if self.market_run not in ['DAM', 'RTM']:
            raise ValueError("Invalid market run")
        # check either region_config is set if upstream model_id is set
        if self.upstream_model_results is not None and self.region_config is None:
            raise ValueError("Region config must be set if upstream model results is set")
    
    def _validate_dates(self):
        """convert dates to datetime.datetime and confirm start date is before end date
        """
        self.s_date = self.u.time_converter(self.s_date, target='dt')
        self.e_date = self.u.time_converter(self.e_date, target='dt')
        if self.s_date >= self.e_date:
            raise ValueError("Start date must be before end date")
    
    def _get_arcus_extract(self) -> pd.DataFrame:
        """
        """
        arcus_extract = ArcusErcotExtractBuilder(
            s_date=self.s_date,
            e_date=self.e_date,
            market_run=self.market_run
        )
        arcus_extract.run()
        return arcus_extract.df
    
    def _create_ts_record(self, df) -> None:
        """
        """
        self._ts_record = TSRecord(ts_obj=df)
        self.ts_record.save()
        self.numerical_features = df.select_dtypes(include=['float', 'int']).columns.tolist()
        # self.categorical_features = df.select_dtypes(include=['object']).columns.tolist()
    
    def subset(self, s_date, e_date, columns=None, inplace=False):
        """subset time series data, wrapper for ts_record.subset
            dates are inclusive

            Args:
            -----
                s_date: str, datetime.datetime
                    start date
                e_date: str, datetime.datetime
                    end date
                columns: list, optional
                    columns to include in subset
                inplace: bool, optional
                    if True, update ts_record with subset
        """
        if inplace:
            self.ts_record.subset(s_date, e_date, columns=columns, inplace=True)
        else:
            df = self.ts_record.subset(s_date, e_date, columns=columns, inplace=False)
            return df
    
    def to_dict(self):
        """
        """
        return {
            's_date': self.s_date,
            'e_date': self.e_date,
            'market_run': self.market_run,
            'ts_record_id': self.ts_record_id,
            'iso': self.iso,
            'categorical_features': self.categorical_features,
            'numerical_features': self.numerical_features,
            'object_type': 'training_dataset',
            'created_at': self.created_at,
        }
    
    def save(self):
        """
        """
        self.ts_record.save()
        self.add_keys(ts_record_id=self.ts_record.id)
        self._save_record()
    
    def _add_upstream_model_results(self, df, upstream_model_results) -> pd.DataFrame:
        """replace columns in existing df with upstream model results
        """
        for col in upstream_model_results.columns:
            df[col] = upstream_model_results[col]
            df[col] = df[col].ffill()
        return df
    
    def get_extract(self, extract_type='arcus'):
        """
        """
        if extract_type == 'arcus':
           df = self._get_arcus_extract()
        else:
            raise ValueError("Invalid extract type")
        if self.upstream_model_results is not None:
            df = self._add_upstream_model_results(df, self.upstream_model_results)
        return df
    
    def create_ts_record(self, df=None):
        """
        """
        if df is None:
            df = self.get_extract()
        self._create_ts_record(df)
        self.save()