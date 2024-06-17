"""
"""

import pandas as pd


class TestTrainSplit:
    """
    """

    def __init__(
        self,
        df: pd.DataFrame,
        tgt_col,
        train_pct= None,
        train_days=None,
        test_location="rear",
        **kwargs
    ):
        """ """
        self.df = df
        self.tgt_col = tgt_col
        self.train_pct = train_pct  # percentage of data to train on
        self.train_days = train_days  # number of days to train on
        self.test_location = test_location  # rear, front, random
        self.kwargs = kwargs
        self.min_test_pct = 0.15  # minimum percentage of data to test on
        self.split_dates = {"test": {}, "train": {}}
        self.train_features = pd.DataFrame()
        self.train_target = pd.DataFrame()
        self.test_features = pd.DataFrame()
        self.test_target = pd.DataFrame()
        self._setup()

    def _setup(self) -> None:
        """
        """
        self._validate_config()
        self._check_min_test_pct()
        self._check_for_dt_index()

    def _validate_config(self) -> None:
        """ """
        if self.tgt_col not in self.df.columns:
            raise ValueError("Target column not in dataframe")
        if self.train_pct is None and self.train_days is None:
            raise ValueError("Must provide train_pct or train_days")
        if self.train_pct is not None and self.train_days is not None:
            raise ValueError("Must provide train_pct or train_days, not both")
        if self.train_pct is not None and (self.train_pct < 0 or self.train_pct > 1):
            raise ValueError("train_pct must be between 0 and 1")
        if self.train_days is not None and self.train_days < 1:
            raise ValueError("train_days must be greater than 0")
        if self.test_location not in ['rear', 'front', 'random']:
            raise ValueError("test_location must be one of ['rear', 'front', 'random']")
        if self.test_location == 'random':
            raise NotImplementedError("random test location not yet implemented")
            # required_kwargs = ['seed', 'min_days']
            # missing_kwargs = [k for k in required_kwargs if k not in self.kwargs.keys()]
            # if len(missing_kwargs) > 0:
            #     raise ValueError(f"Missing required kwargs: {missing_kwargs}")

    def _check_min_test_pct(self):
        """Ensure test percentage is reasonable."""
        if self.train_pct is None:
            df_days = pd.date_range(self.df.index[0], self.df.index[-1], freq="D")
            test_days = len(df_days) - self.train_days
            self.train_pct = self.train_days / len(df_days)
        if (1 - self.train_pct) < self.min_test_pct:
            raise ValueError("train_pct must be greater than min_test_pct")

    def _get_split_pct(self) -> None:
        """calculate the nuber of days to train on based on the train_pct
        """
        df_start = self.df.index[0]
        df_end = self.df.index[-1]
        df_dates = pd.date_range(df_start, df_end, freq='D')
        self.train_days = int(len(df_dates) * self.train_pct)
        self._get_split_days()

    def _check_for_dt_index(self) -> None:
        """Confirm that the dataframe has a datetime index. 
        """
        if not isinstance(self.df.index, pd.DatetimeIndex):
            raise ValueError("Dataframe must have a datetime index")
        # This is not necessary
        # if self.df.index.freq is None:
        #    raise ValueError("Dataframe must have a frequency")

    def _get_split_days(self) -> None:
        """calculate the start and end dates for the train and test sets.
        """
        df_days = pd.date_range(self.df.index[0], self.df.index[-1], freq='D')
        if self.train_days > len(df_days):
            raise ValueError("train_days must be less than the length of the dataframe")
        if self.test_location == "rear":
            self.split_dates["train"]["start"] = self.df.index[0]
            self.split_dates["train"]["end"] = self.split_dates["train"][
                "start"
            ] + pd.DateOffset(days=self.train_days, seconds=-1)
            self.split_dates["test"]["start"] = self.split_dates["train"]['end'] + pd.DateOffset(seconds=1)
            self.split_dates["test"]["end"] = self.df.index[-1]
        elif self.test_location == "front":
            self.split_dates["test"]["start"] = self.df.index[0]
            self.split_dates["test"]["end"] = self.split_dates["start"] + pd.DateOffset(
                days=self.train_days, seconds=-1
            )
            self.split_dates["train"]["start"] = self.split_dates["test"][
                "end"
            ] + pd.DateOffset(seconds=1)
            self.split_dates["train"]["end"] = self.df.index[-1]
        else:
            raise NotImplementedError("random test location not yet implemented")

    def _split_data(self) -> None:
        """split the input dataframe into train and test sets.
        """
        target = self.df.loc[:, self.tgt_col]
        features = self.df.drop(columns=[self.tgt_col])
        if self.test_location in ['rear', 'front']:
            self.train_features = features.loc[
                self.split_dates["train"]["start"] : self.split_dates["train"]["end"]
            ]

            self.train_target = target.loc[self.split_dates["train"]["start"] : self.split_dates["train"]["end"]]
            self.test_features = features.loc[self.split_dates["test"]["start"] : self.split_dates["test"]["end"]]
            self.test_target = target.loc[self.split_dates["test"]["start"] : self.split_dates["test"]["end"]]
        else:
            raise NotImplementedError("random test location not yet implemented")

    def run(self) -> dict:
        """calculate split dates and split data into train and test sets.
        """
        if self.train_pct is not None:
            self._get_split_pct()
        elif self.train_days is not None:
            self._get_split_days()
        self._split_data()
