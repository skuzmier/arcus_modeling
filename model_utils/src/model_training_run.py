"""
"""

import sys
import pandas as pd
import subprocess

from record import Record
from test_train_split import TestTrainSplit
from training_dataset import TrainingDataset
from scaler import Scaler
from xgb_model import XGBModel


class ModelTrainingRun(Record):
    """
    """
    object_type = 'model_training_run'

    def __init__(self, id=None, s_date=None, e_date=None, **kwargs):
        """
        TODO: Does not support categorical features.
        NOTE: I think this class is too big, and should be broken up into smaller classes.
        Args:
        -----
            id: str, bson.ObjectId uuid of record
            s_date: str, datetime.datetime 
                start date
            e_date: str, datetime.datetime
                end date

        """
        super().__init__(id, object_type=self.object_type, **kwargs)
        self.kwargs = kwargs
        self.config = kwargs.get('config', {})
        self.s_date = s_date
        self.e_date = e_date
        self.df = pd.DataFrame() # Temporal and column subset
        self.market_run = None
        self.region = None
        self.feature_scaler_id = None #Segregated scalers is best practice
        self.target_scaler_id = None
        self.label_encoder_id = None
        self.model_id = None
        self._feature_scaler = None
        self._target_scaler = None
        self._label_encoder = None #Used for categorical features
        self.dataset_id = None
        self._dataset = None
        self._model = None
        self._ignore_params = ['df']
        self.parent_id = None
        self.split_dates = {} # Test train split dates
        self.feature_cols = []
        self.tgt_col = None
        self._test_train_split = None
        self.valid_from = None
        self.valid_until = None
        self.valid_from = None
        self.setup_mode = False
        self.git_hash = None
        self._setup()

    @property
    def feature_scaler(self):
        """load feature scaler on demand, 
            using property to enable lazy loading, and prevent accidental overwriting
        """
        if self._feature_scaler is not None:
            return self._feature_scaler
        if self._feature_scaler is None:
            self._feature_scaler = Scaler(id=self.feature_scaler_id)
        return self._feature_scaler

    @property
    def target_scaler(self):
        """scaler for target variable
        """
        if self._target_scaler is not None:
            return self._target_scaler
        if self._target_scaler is None:
            self._target_scaler = Scaler(id=self.target_scaler_id)
        return self._target_scaler

    @property
    def label_encoder(self):
        """load label encoder on demand 
        """
        if self._label_encoder is not None:
            return self._label_encoder
        if self.label_encoder_id is None:
            raise ValueError("Label encoder id not set")
        if self._label_encoder is None:
            self._label_encoder = Scaler(id=self.label_encoder_id)
        return self._label_encoder

    @property
    def dataset(self):
        """load dataset on demand
        """
        if self._dataset is None:
            self._dataset = TrainingDataset(id=self.dataset_id)
        return self._dataset

    @property
    def model(self):
        """load model on demand
        """
        if self._model is not None:
            return self._model
        if self.model_id is None:
            self._get_model_from_config()
        elif self._model is None:
            self._model = XGBModel(id=self.model_id)
        return self._model

    def _setup(self):
        """
        """
        if len(self.kwargs) > 2:
            self._from_dict(self.kwargs)
        elif self._id is not None:
            self._from_id()
        elif 'config' in self.kwargs:
            self.config = self.kwargs['config']
            self._from_config()

    def _validate_params(self):
        """check that the config dict contains all required parameters
        """
        required_params = ['model_config', 'test_train_split', 'scaler',
                           's_date', 'e_date', 'region_config']
        missing_params = []
        for param in required_params:
            if param not in self.config:
                missing_params.append(param)
        if len(missing_params) > 0:
            raise ValueError(f"Missing required parameters: {missing_params}")

    def _get_model_from_config(self):
        """instantiate a new model from config dicts
        """
        required_params = ['model_type', 'api_version', 'model_params']
        missing_params = []
        for param in required_params:
            if param not in self.config['model_config']:
                missing_params.append(param)
        if len(missing_params) > 0:
            raise ValueError(f"Missing required parameters: {missing_params}")
        self._model = XGBModel(model_config=self.config['model_config']['model_params'], **self.config['model_config'])

    def _get_df(self):
        """
        """
        df = self.dataset.df.loc[self.s_date:self.e_date]
        cols = self.feature_cols + [self.tgt_col]
        self.df = df[cols]

    def _get_git_hash(self):
        """get the git hash of the current commit
        """
        self.git_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip().decode('utf-8')

    def _get_test_train_split(self):
        """instantiate a new test train split obj, calc split dates
            add split dates to self.test_train_split
            
            Returns:
            --------
                dict: {'train': {'start': datetime.datetime, 'end': datetime.datetime, 'dates': list},
        """
        all_cols = self.feature_cols + [self.tgt_col]
        test_train_split = TestTrainSplit(
            df=self.dataset.df, tgt_col=self.tgt_col, **self.config["test_train_split"]
        )
        test_train_split.run()
        self.split_dates = test_train_split.split_dates
        self._test_train_split = test_train_split

    def _get_new_scalers(self):
        """Instantiate new scalers and set to self._feature_scaler and self._target_scaler
        """
        scaler_config = self.config['scaler']
        if 'numerical_scaler' not in scaler_config:
            raise ValueError("numerical_scaler config not found in scaler config")
        if self._target_scaler is not None:
            raise ValueError("target scaler already set")
        if self._feature_scaler is not None:
            raise ValueError("feature scaler already set")
        self._feature_scaler = Scaler(**scaler_config['numerical_scaler'])
        self._target_scaler = Scaler(**scaler_config['numerical_scaler'])
        if 'label_encoder' in scaler_config:
            self._label_encoder = Scaler(**scaler_config['label_encoder'])

    def _get_col_names(self):
        """extract target column names from the config
        """
        self.tgt_col = self.config['region_config']['tgt_col']
        self.feature_cols = self.config['region_config']['features']
        self.region = self.config['region_config']['region']

    def _fit_scalers(self):
        """fit scalers to training data
        """
        numerical_features = [x for x in self.feature_cols if x in self.dataset.numerical_features]
        categorical_features = [x for x in self.feature_cols if x in self.dataset.categorical_features]
        self.feature_scaler.fit(self.dataset.df[numerical_features])
        self.target_scaler.fit(self.dataset.df[[self.tgt_col]])
        if self._label_encoder is not None: #FIXME: This will not work due to
            self.label_encoder.fit(self.dataset.df[categorical_features])

    def _train_model(self):
        """scale training and test features and target, then train the model
        """
        scaled_training_features = self.feature_scaler.transform(self._test_train_split.train_features)
        scaled_training_target = self.target_scaler.transform(self._test_train_split.train_target)
        scaled_test_features = self.feature_scaler.transform(self._test_train_split.test_features)
        scaled_test_target= self.target_scaler.transform(self._test_train_split.test_target)
        self.model.fit(scaled_training_features, scaled_training_target,
                         eval_set=[(scaled_test_features, scaled_test_target)])

    def _save_objects(self):
        """save all new objects
        """
        if self._feature_scaler is not None and self.feature_scaler.id is None:
            self.feature_scaler.save()
            self.add_keys(feature_scaler_id=self.feature_scaler.id)
        if self._target_scaler is not None and self.target_scaler.id is None:
            self.target_scaler.save()
            self.add_keys(target_scaler_id=self.target_scaler.id)
        if self._label_encoder is not None and self.label_encoder.id is None:
            self.label_encoder.save()
            self.add_keys(label_encoder_id=self.label_encoder.id)
        if self._model is not None and self.model.id is None:
            self.model.save()
            self.add_keys(model_id=self.model.id)

    def _scaler_config_list_to_tuples(self, data) -> dict:
        """JSON can not encode tuples, 
            so lists in config need to be converted back to tuples

            Args:
            -----
                data: dict, list
        """
        if isinstance(data, list):
            return tuple(data)
        if isinstance(data, dict):
            return {k: self._scaler_config_list_to_tuples(v) for (k, v) in data.items()}
        return data

    def _fix_scaler_config(self):
        """
        """
        self.config['scaler'] = self._scaler_config_list_to_tuples(self.config['scaler'])

    def to_dict(self):
        """
        """
        res = {}
        for (k, v) in self.__dict__.items():
            if k in self._ignore_params or k.startswith("_"):
                continue
            if isinstance(v, Record): # Do I need this?
                res[k] = v.to_dict()
            else:
                res[k] = v
        return res

    def _from_config(self):
        """load existing args from config
        """
        for (k, v) in self.config.items():
            if hasattr(self, k):
                setattr(self, k, v)

    def run(self):
        """ """
        self._validate_params()
        self._get_col_names()
        self._get_df()
        self._get_test_train_split()
        self._fix_scaler_config()
        self._get_new_scalers()
        self._fit_scalers()
        self._train_model()
        self._get_git_hash()
        self._save_objects()
        self.save()

        scaled_training_features = mtr.feature_scaler.transform(
            mtr._test_train_split.train_features
        )
        scaled_training_target = mtr.target_scaler.transform(
            mtr._test_train_split.train_target
        )
        scaled_test_features = mtr.feature_scaler.transform(
            mtr._test_train_split.test_features
        )
        scaled_test_target = mtr.target_scaler.transform(
            mtr._test_train_split.test_target
        )
        new_model.model.fit(
            scaled_training_features,
            scaled_training_target,
            eval_set=[(scaled_test_features, scaled_test_target)],
        )
