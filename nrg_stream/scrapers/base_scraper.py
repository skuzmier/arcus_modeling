"""
"""
from psycopg2 import sql
import sys
import numpy as np
import pandas as pd

import pytz
import os

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts"))

from utils import Utils
from stream_record import StreamRecord
from nrg_stream import NRGStream
from helpers import Helpers

import pdb


class BaseScraper:
    """ """

    def __init__(self, stream_id, s_date, e_date, **kwargs):
        """ """
        self.u = Utils()
        self.stream_id = stream_id
        self.s_date = s_date
        self.e_date = e_date
        self.kwargs = kwargs
        self.stream_record = None
        self.api = None
        self.hlp = Helpers()
        self.df = None
        self.debug = False
        self.convert_tz = True  # convert from stream tz to gmt, set to false for scrapers using db for tz conversion
        self.expected_tables = []
        self.missing_data_rules = {
            "warning_cols": [],
            "error_cols": [],
            "interpolate": False,
            "limit_area": 3,
        }
        self.missing_ts = []
        # self._setup()

    def _setup(self):
        """ """
        if len(self.expected_tables) == 0:
            raise ValueError("expected tables must be set in subclass")
        self.stream_record = StreamRecord(stream_id=self.stream_id)
        if self.stream_record.id is None:
            raise ValueError(f"no stream record with id {self.stream_id}")
        if self.stream_record.db_info.table not in self.expected_tables:
            raise ValueError(
                f"stream record with id {self.stream_id} is not of the expected type"
            )
        self._validate_dates()
        self.stream_record = StreamRecord(stream_id=self.stream_id)

    def _validate_dates(self):
        """ """
        self.s_date = self.u.time_converter(self.s_date, target="dt")
        self.e_date = self.u.time_converter(self.e_date, target="dt")
        if self.s_date >= self.e_date:
            raise ValueError("start date must be before end date")

    def _data_available(self):
        """check if requested date ends after data availability begins"""
        if self.stream_record.available_from is None:
            return True
        if self.e_date < self.stream_record.available_from:
            return False
        return True

    def run(self):
        """ """
        if not self._data_available():
            print("data not available, skipping")
            return False
        self._get_data()
        self._get_columns()
        self._convert_tz()
        self._find_missing_timestamps()
        self._find_missing_data()
        # print("data scraped")
        # print(len(self.df))
        self._filter_existing_data()
        # print("data filtered")
        # print(len(self.df))
        if not self.debug:
            self._load_data()

    def _find_missing_data(self):
        """check columns for missing data, interpolate if needed"""
        interpolated_dates = []
        if len(self.df) == 0:
            raise ValueError("no data to check")
        for col in self.missing_data_rules["warning_cols"]:
            self.df.loc[self.df[col] == "", col] = np.nan
            if self.df[col].isna().sum() > 0:
                print(f"warning: {col} has {self.df[col].isna().sum()} missing values")
                interp_dates = self.df.loc[self.df[col].isna(), "dt"].unique()
                interpolated_dates.extend(interp_dates)
                self.df[col] = (
                    self.df[col]
                    .astype(float)
                    .interpolate(limit=self.missing_data_rules["limit_area"])
                )
        # filter out rows with missing data
        self.df = self.df.dropna(subset=self.missing_data_rules["warning_cols"])
        for col in self.missing_data_rules["error_cols"]:
            self.df.loc[self.df[col] == "", col] = np.nan
            if self.df[col].isna().sum() > 0:
                raise ValueError(
                    f"error: {col} has {self.df[col].isna().sum()} missing values"
                )
        if len(interpolated_dates) > 0:
            self.stream_record.add_missing_dates(list(set(interpolated_dates)))

    def _find_missing_timestamps(self, dt_col="dt"):
        """find frequency of data, compare timestamps present to expected timestamps"""
        if len(self.df) == 0:
            raise ValueError("no data to check")
        if dt_col not in self.df.columns:
            raise ValueError(f"{dt_col} not in columns")
        freq = pd.infer_freq(self.df[dt_col])
        if freq is None:
            freq = self.api.meta.get("dataInterval", None)
            if freq is None:
                raise ValueError("could not infer frequency")
            freq = str(freq) + "T"
        expected = pd.date_range(
            start=self.df[dt_col].min(), end=self.df[dt_col].max(), freq=freq
        )
        missing = expected[~expected.isin(self.df[dt_col])]
        if len(missing) > 0:
            self.missing_ts = missing
            self.stream_record.add_missing_dates(list(set(missing)))
            # raise ValueError(f'warning: {len(missing)} missing timestamps found')

    def _get_data(self):
        """ """
        self.api = NRGStream(username="spencer", password="nrg4spencer")
        self.api.get_stream(
            self.stream_id, self.s_date, self.e_date, data_format="json"
        )
        self.df = self.api.data.copy()
        if len(self.df) == 0:
            raise ValueError(f"no data returned for stream id {self.stream_id}")

    def _get_columns(self):
        """get column names, add energy, congestion, and loss if needed"""
        columns = []
        raise ValueError("must be implemented in subclass")

    def _filter_existing_data(self):
        """remove data that may have already been loaded"""
        stream_first_date = np.datetime64(
            self._localize_db_dt(self.stream_record.first_date)
        )
        stream_last_date = np.datetime64(
            self._localize_db_dt(self.stream_record.last_date)
        )
        # pdb.set_trace()
        if (stream_first_date is None) ^ (stream_last_date is None):
            raise ValueError(
                "first date and last date must both be set or both be None"
            )
        self.df["dt"] = pd.to_datetime(self.df["dt"], format="mixed")
        if (
            self.stream_record.first_date is not None
            and self.stream_record.last_date is not None
        ):
            self.df = self.df.loc[
                (self.df["dt"] < stream_first_date) | (self.df["dt"] > stream_last_date)
            ]

    def _localize_db_dt(self, dt):
        """localize first and last dates"""
        if dt is None or dt == "":
            return None
        db_tz = pytz.timezone(self.stream_record.db_info.table_tz)
        if self.stream_record.stream_tz == "CPT":
            tz_name = "US/Central"
        else:
            tz_name = self.stream_record.stream_tz
        local_tz = pytz.timezone(tz_name)
        dt = dt.replace(tzinfo=None)
        dt = db_tz.localize(dt)
        dt = dt.astimezone(local_tz).replace(tzinfo=None)
        return dt

    def _load_data(self):
        """ """
        self.u.insert_df(df=self.df, table_name=self.stream_record.db_info.table)
        df_first_date = self.u.time_converter(self.df["dt"].min(), target="dt")
        df_last_date = self.u.time_converter(self.df["dt"].max(), target="dt")
        stream_first_date = self._localize_db_dt(self.stream_record.first_date)
        stream_last_date = self._localize_db_dt(self.stream_record.last_date)
        if (
            stream_first_date is None
            or stream_first_date > df_first_date
        ):
            self.stream_record.first_date = df_first_date
        if (
            stream_last_date is None
            or stream_last_date < df_last_date
        ):
            self.stream_record.last_date = df_last_date

    def _convert_tz(self):
        """convert from stream tz to gmt, some scrapers perform this conversion in the db"""
        if not self.convert_tz:
            return None
        self.df["dt"] = pd.to_datetime(self.df["dt"], format="mixed")
        self.df = self.hlp.convert_tz(
            df=self.df,
            source_tz=self.stream_record.stream_tz,
            target_tz="GMT",
            date_col="dt",
        )
