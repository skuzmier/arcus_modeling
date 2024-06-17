"""
"""

import sys
import pickle
import os

import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler, Normalizer, LabelEncoder
import warnings

from record import Record
from file_record import FileRecord

sys.path.append("/home/ubuntu/ercot_virts/utils")
from utils import Utils


class Scaler(Record):
    """
    """
    object_type = 'data_scaler'

    def __init__(self, id=None, scaler_type=None, config={}, **kwargs):
        """
            Args:
            -----
                id: str, bson.ObjectId uuid of record
                scaler_type: str, type of scaler to use
                config: dict, configuration for scaler
        """
        super().__init__(id, object_type='data_scaler', **kwargs)
        self.scaler_type = scaler_type
        self.kwargs = kwargs
        self.config = config
        self.file_id = None
        self._file = None
        self._scaler = None
        self._ignore_params = []
        self.features = []
        self.parent_id = None # uuid of parent record, if any
        self.is_fit = False
        self._setup()

    def _setup(self):
        """ """
        if self._id is not None:
            self._from_id()
        elif len(self.kwargs) > 0:
            self._from_dict()
            self._get_new_scaler()
        else:
            pass

    @property
    def file(self):
        """Enable lazy loading of file record 
        """
        if self._file is None:
            self._get_file()
        return self._file

    @property
    def scaler(self):
        """enable lazy loading of scaler 
        """
        if self._scaler is None:
            self._get_scaler()
        return self._scaler

    @scaler.setter
    def scaler(self, value):
        """save value to scaler.
           This enables creating a new object from an existing scaler. 
           Evaluate use, may promote bad practice
        """
        if self._scaler is not None:
            raise ValueError("Scaler already set, cannot overwrite")
        if not isinstance(value, (MinMaxScaler, StandardScaler, RobustScaler, Normalizer, LabelEncoder)):
            raise ValueError(f"Invalid scaler type: {type(value)}")
        if self.scaler_type is None:
            raise ValueError("Scaler type must be set before setting scaler")
        # check if the scaler type matches the value type
        if self.scaler_type not in str(type(value)):
            raise ValueError(f"Scaler type mismatch, expected {self.scaler_type}, got {type(value)}")
        self._scaler = value

    def _get_file(self):
        """create the file record object
        """
        if self.file_id is None:
            self._file = FileRecord()
        else:
            self._file = FileRecord(id=self.file_id)

    def _get_scaler(self):
        """
        """
        if self.id is None:
            self._get_new_scaler()
        else:
            self._get_existing_scaler()

    def _get_existing_scaler(self):
        """
        """
        if self.file_id is None:
            raise ValueError("No file record found")
        self._scaler = pickle.load(open(self.file.path, 'rb'))

    def _get_new_scaler(self):
        """create the scaler object
        """
        scaler_map = {
            "MinMaxScaler": MinMaxScaler,
            "StandardScaler": StandardScaler,
            "RobustScaler": RobustScaler,
            "Normalizer": Normalizer,
            "LabelEncoder": LabelEncoder,
        }
        if self.scaler_type not in scaler_map.keys():
            raise ValueError(f"Invalid scaler type: {self.scaler_type}")
        self._scaler = scaler_map[self.scaler_type](**self.config)

    def _save_scaler(self):
        """pickle the scaler, save to file and save file record
        """
        if self._scaler is None:
            raise ValueError("No scaler found")
        file_name = f'scaler_{self.id}.pkl'
        file_path = os.path.join(self.file.cache_dir, file_name)
        pickle.dump(self._scaler, open(file_path, 'wb'))
        file_record = FileRecord(path=file_path)
        file_record.save()
        self.add_keys(file_id=file_record.id)
    
    def fit(self, df) -> None:
        """mimic sk-learn fit method for scaler, while providing saftey checks

            Args:
            -----
                df: pd.DataFrame, dataframe to fit scaler to
        """
        self.columns = list(df.columns)
        if self.scaler_type == 'LabelEncoder':
            self._fit_label_encoder(df)
        else:
            self.scaler.fit(df)
        self.is_fit = True
    
    def transform(self, df) -> pd.DataFrame:
        """ mimic sk-learn transform method for scaler, while providing saftey checks
            Validate that the columns in the dataframe match the columns and order used to fit the scaler

            Args:
            -----
                df: pd.DataFrame, dataframe to transform
        """
        if self.fit is False:
            raise ValueError("Scaler not fit")
        #if df is a series then just transform it
        if isinstance(df, pd.Series):
            return self.scaler.transform(df.values.reshape(-1, 1)).ravel()
        #check if all columns are present in df
        if not all([col in df.columns for col in self.columns]):
            raise ValueError("Columns in dataframe do not match columns used to fit scaler")
        #ensure columns are correctly ordered and exclude any extra columns
        df = df[self.columns]
        return self.scaler.transform(df)
    
    def fit_transform(self, df) -> pd.DataFrame:
        """mimic sk-learn fit_transform method for scaler, while providing saftey checks

            Args:
            -----
                df: pd.DataFrame, dataframe to fit and transform
        """
        if self._scaler is None:
            raise ValueError("No scaler found")
        self.columns = df.columns
        return self._scaler.fit_transform(df)
    
    def inverse_transform(self, df) -> pd.DataFrame:
        """Mimic sk-learn inverse transofrm. 
           Maybe do some cool suff with this later
           Mostly keeping it here for completeness
        """
        if self.is_fit is False:
            raise ValueError("Scaler not fit")
        return self.scaler.inverse_transform(df)

    
    def _fit_label_encoder(self, df) -> None:
        """fit label encoder to dataframe
        """
        raise NotImplementedError("LabelEncoder not yet implemented")

    
    def to_dict(self) -> dict:
        """convert to dictonary for saving
        """
        record_dict = {}
        for (k, v) in self.__dict__.items():
            if k not in self._ignore_params and not k.startswith("_"): # ignore private attributes
                record_dict[k] = v
        return record_dict

    def save(self):
        """
        """
        if self._id is None:
            self._save_record()
            self._save_scaler()
        else:
            warnings.warn("Record already saved, skipping")
