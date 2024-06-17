"""
"""

import sys
import os
import xgboost as xgb
import pandas as pd

from record import Record
from file_record import FileRecord
sys.path.append("/home/ubuntu/ercot_virts/utils")
from utils import Utils


class XGBModel(Record):
    """
    """
    object_type = 'xgb_model'

    def __init__(self, id=None, model_type='regression', model_config={}, **kwargs):
        """
        Args:
        -----
            id: str, bson.ObjectId uuid of record
            config: dict, configuration for model
            model_type: str, type of model to use
                regression or classifier
            m

        """
        super().__init__(id, object_type='xgb_model', **kwargs)
        self.kwargs = kwargs
        self.config = model_config
        self.model_type = model_type
        self.file_id = None
        self._file = None
        self._model = None
        self._ignore_params = []
        self.parent_id = None
        self.api_version = self.kwargs.get('api_version', 'sk-learn')
        self.setup_mode = False
        self.is_fit = False
        self._setup()

    @property
    def file(self):
        """Enable lazy loading of file record 
        """
        if self._file is None:
            self._get_file()
        return self._file

    @property
    def model(self):
        """enable lazy loading of model 
        """
        if self._model is None:
            self._get_model()
        return self._model

    @model.setter
    def model(self, value):
        """ 
        """
        if self.setup_mode:
            self._model = value
        elif self._model is None:
            self._model = value
        else:
            raise ValueError("model already set, cannot overwrite")

    def _setup(self):
        """ """
        self.setup_mode = True
        if self._id is not None:
            self._from_id()
        elif len(self.kwargs) > 0:
            self._from_dict()
        else:
            pass
        self._validate_config()
        self.setup_mode = False

    def _validate_config(self):
        """confirm config is valid"""
        if self.api_version not in ["sk-learn", "xgboost", "lightgbm"]:
            raise ValueError(
                "api_version must be one of ['sk-learn', 'xgboost', 'lightgbm']"
            )
        if self.api_version != "sk-learn":
            raise NotImplementedError("Only sk-learn models are supported")
        if self.model_type not in ["regression", "classifier"]:
            raise ValueError("model_type must be one of ['regression', 'classifier']")
        required_params = ["objective", "n_estimators"]
        missing_params = [p for p in required_params if p not in self.config.keys()]
        if len(missing_params) > 0:
            raise ValueError(f"Missing required parameters: {missing_params}")

    def _get_file(self):
        """
        """
        if self.file_id is None:
            self._file = FileRecord() # Allow instantion for access to file methods
        else:
            self._file = FileRecord(id=self.file_id)

    def _get_model(self):
        """ 
        """
        if self._model is not None: # This is a lazy load method
            return None
        elif self.id is None:
            self._get_new_model()
        else:
            self._get_existing_model()

    def _get_existing_model(self):
        """
        """
        if self.api_version == 'sk-learn':
            if self.model_type == 'regression':
                self._model = xgb.XGBRegressor()
            elif self.model_type == 'classifier':
                self._model = xgb.XGBClassifier()
            self._model.load_model(self.file.path)

    def _get_new_model(self):
        """
        """
        if self.api_version == 'sk-learn':
            self._setup_sklearn_model()
        elif self.api_version == 'xgboost':
            # self._setup_xgboost_regressor() # Not implemented
            pass
        elif self.api_version == 'lightgbm':
            # self._setup_lightgbm_regressor() # Not implemented
            pass

    def _setup_sklearn_model(self):
        """
        """
        if self.model_type == 'regression':
            self._model = xgb.XGBRegressor(**self.config)
        else:
            self._model = xgb.XGBClassifier(**self.config)

    def _save_sklearn_model(self):
        """
        """
        if self._model is None:
            raise ValueError("model not set")
        file_name = f'model_{self.id}.obj'
        file_path = os.path.join(self.file.cache_dir, file_name)
        self.model.save_model(file_path)
        self._file = FileRecord(path=file_path)
        self.file.save()
        self.add_keys(file_id=self.file.id)

    def to_dict(self):
        """
        """
        res = {}
        for (k, v) in self.__dict__.items():
            if k in self._ignore_params or k.startswith("_"):
                continue
            res[k] = v
        if self._id is not None:
            res["_id"] = self._id
        return res

    def fit(self, features, target, **kwargs):
        """Train the model
            TODO: Need to type check the inputs
            Args:
            -----
                features: pd.DataFrame, features to train model on
                target: pd.Series, target to train model on
                kwargs: dict, additional arguments to pass to model fit method
        """
        self.model.fit(features, target, **kwargs)
        self.is_fit = True
    
    def predict(self, features):
        """predict target values
            Args:
            -----
                features: pd.DataFrame, features to predict target values for
        """
        if self.is_fit is False:
            raise ValueError("model not trained")
        return self.model.predict(features)
    
    def save(self):
        """
        """
        self._save_sklearn_model()
        if self.file.key is None:
            raise ValueError("file key not set")
        self._save_record()

