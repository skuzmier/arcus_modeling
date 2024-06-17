"""
"""
import os
import sys
import pandas as pd

sys.path.append("/home/ubuntu/ercot_virts/utils")
from utils import Utils
from file_record import FileRecord
from record import Record


class TSRecord(Record):
    """
    """
    def __init__(self, id=None, ts_obj=None, **kwargs):
        """
        Args:
        -----
            id: str, bson.ObjectId uuid of record
            ts_obj: pd.DataFrame, time series data
        """
        super().__init__(id, object_type='ts_record', **kwargs)
        self.kwargs = kwargs
        self._ts_obj = ts_obj
        self.file_id = None
        self._file = None
        self._ignore_params = []
        self.setup_mode = False
        self.s_date = None
        self.e_date = None
        self.features = []
        self.system = None
        self.dt_col = None
        self.dt_col_is_index = False
        self.tgt_file_type = self.kwargs.get('tgt_file_type', 'parquet')
        self._setup()
    
    @property
    def ts_obj(self):
        """ """
        if self._ts_obj is None:
            self._get_ts_obj()
        return self._ts_obj
    
    @ts_obj.setter
    def ts_obj(self, value):
        """ 
        """
        if self.setup_mode:
            self._ts_obj = value
        elif self._ts_obj is not None:
            raise ValueError("Cannot set ts_obj, already set")
        else:
            self._ts_obj = value
    
    @property
    def file(self):
        """ """
        if self._file is None:
            self._get_file()
        return self._file
    
    def _setup(self):
        """ 
        """
        if self._id is not None:
            self._from_id()
        elif self.ts_obj is not None:
            self._from_ts_obj()
        elif len(self.kwargs) > 0:
            self._from_dict()
        else:
            pass
        self.setup_mode = False
    
    def _get_ts_obj(self):
        """ """
        if self.file_id is not None:
            self._get_file()
            self._get_dataframe()
        else:
            raise ValueError("No ts_obj or file_id provided")
    
    def _get_file(self):
        """ 
        """
        if self.file_id is None:
            self._file = FileRecord() # Allow instantion for access to file methods
        else:
            self._file = FileRecord(id=self.file_id)
    
    def _get_dataframe(self):
        """load dataframe from file
        """
        if self.tgt_file_type == 'parquet':
            self.ts_obj = pd.read_parquet(self.file.path)
        elif self.tgt_file_type == 'csv':
            self.ts_obj = pd.read_csv(self.file.path)
    
    def _from_ts_obj(self):
        """setup from a timeseries object, currently dataframe only,
            Plan to add support for xarray, and dask
        """
        if not isinstance(self.ts_obj, pd.DataFrame):
            raise ValueError("ts_obj must be a pandas DataFrame")
        self._get_dt_column()
        self._get_features()
        self._get_dates()
    
    def _get_dt_column(self):
        """check if the ts_obj has a datetime index or a dt column
        """
        #check if index is datetime
        if isinstance(self.ts_obj.index, pd.DatetimeIndex):
            self.dt_col = self.ts_obj.index.name
            self.dt_col_is_index = True
        else:
            # check candidate columns against ts_obj
            date_cols = ["s_date", "intervalstarttime_gmt", "dt", "ldt", "w_date", "dt_local", "time"]
            for col in date_cols:
                if col in self.ts_obj.columns:
                    self.dt_col = col
                    break
        # if no date column found, raise error
        if self.dt_col is None:
            raise ValueError("No date column found in ts_obj")
    
    def _get_features(self):
        """
        """
        if  isinstance(self.ts_obj, pd.DataFrame):
            self.features = self.ts_obj.columns.tolist()
        else:
            raise NotImplementedError("Only pandas DataFrames are supported")
        #elif isinstance(self.ts_obj, xr.Dataset):
        #    self.features = list(self.ts_obj.data_vars.keys())
    
    def _get_dates(self):
        """get start and end dates from ts_obj
        """
        if self.dt_col_is_index:
            self.s_date = self.ts_obj.index.min()
            self.e_date = self.ts_obj.index.max()
        else:
            self.s_date = self.ts_obj[self.dt_col].min()
            self.e_date = self.ts_obj[self.dt_col].max()
    
    def to_dict(self):
        """
        """
        record_dict = {}
        for (k, v) in self.__dict__.items():
            if k not in self._ignore_params and not k.startswith("_"):
                record_dict[k] = v
        return record_dict
    
    def _save_ts_obj(self):
        """
        """
        if self._ts_obj is None:
            raise ValueError("No ts_obj found")
        file_name = f'ts_obj_{self.id}.{self.tgt_file_type}'
        file_path = os.path.join(self.file.cache_dir, file_name)
        if self.tgt_file_type == 'parquet':
            self._ts_obj.to_parquet(file_path)
        elif self.tgt_file_type == 'csv':
            self._ts_obj.to_csv(file_path)
        file_record = FileRecord(path=file_path)
        file_record.save()
        self.add_keys(file_id=file_record.id)
    
    def subset(self, s_date, e_date, columns=None, inplace=False) -> pd.DataFrame:
        """Subset dataframe by date range and columns
            Note: Inclusive of end date
            
            Args:
            -----
                s_date: str, datetime.datetime 
                    start date
                e_date: str, datetime.datetime 
                    end date
                columns: list, optional
                    columns to include in subset
                inplace: bool, optional
                    modify ts_obj in place
        """
        if inplace:
            self.ts_obj = self.ts_obj.loc[s_date:e_date, columns]
        else:
            return self.ts_obj.loc[s_date:e_date, columns]

    def save(self):
        """
        """
        if self._id is None:
            self._save_record()
            self._save_ts_obj()
        else:
            pass
