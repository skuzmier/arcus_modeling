"""
"""
import sys
import os
from psycopg2 import sql
import pandas as pd
import numpy as np

import pytz

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts"))

from utils import Utils
from stream_record import StreamRecord
from nrg_stream import NRGStream
from helpers import Helpers


class PriceScraper:
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
        self.missing_data_rules = {'warning_cols': [], 'error_cols': ['lmp'], 'interpolate': False,
                                   'limit_area': 3}
        self.debug = self.kwargs.get("debug", False)
        self._setup()

    def _setup(self):
        """ """
        self.stream_record = StreamRecord(stream_id=self.stream_id)
        if self.stream_record.id is None:
            raise ValueError(f"no stream record with id {self.stream_id}")
        if self.stream_record.db_info.table not in [
            "da_lmp",
            "rt_lmp",
            "rtpd_lmp",
            "fmm_lmp",
            "rtm_lmp",
            'rtm_settle'
        ]:
            raise ValueError(
                f"stream record with id {self.stream_id} is not a price stream"
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
        self._find_missing_data()
        self._find_missing_timestamps()
        self._filter_existing_data()
        # self._convert_tz() handle this in db
        if not self.debug:
            self._load_data()

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
        for col in self.api.meta["columns"]:
            columns.append(col["name"].lower())
        self.df.columns = columns
        if self.stream_record.iso == "ERCOT":
            self._add_columns_ercot()
        else:
            self._add_columns()
        self.df["iso_id"] = self.stream_record.iso_id
        self.df["node_id"] = self.stream_record.location_id
        self.df = self.df[
            ["iso_id", "node_id", "dt", "lmp", "energy", "congestion", "loss"]
        ]
        self.df['dt'] = pd.to_datetime(self.df['dt'], format='mixed')
    
    def _localize_db_dt(self, dt):
        """localize first and last dates
        """
        if dt is None or dt == '':
            return None
        db_tz = pytz.timezone(self.stream_record.db_info.table_tz)
        if self.stream_record.stream_tz == 'CPT':
            tz_name = 'US/Central'
        else:
            tz_name = self.stream_record.stream_tz
        local_tz = pytz.timezone(tz_name)
        dt = dt.replace(tzinfo=None)
        dt = db_tz.localize(dt)
        dt = dt.astimezone(local_tz).replace(tzinfo=None)
        return dt
        
    def _add_columns_ercot(self):
        """ercot doesn't have energy, congestion, and loss columns add them"""
        self.df.rename(columns={"date/time": "dt", "price": "lmp"}, inplace=True)
        self.df = self.df.loc[self.df["lmp"] != ""]
        self.df.loc[:, "lmp"] = self.df["lmp"].astype(float)
        self.df.loc[:, "energy"] = 0
        self.df.loc[:, "congestion"] = 0
        self.df.loc[:, "loss"] = 0

    def _add_columns(self):
        """fix column names and add energy col for markets which break out prices"""
        self.df.rename(columns={"date/time": "dt", "price": "lmp"}, inplace=True)
        for col in ["lmp", "congestion", "loss"]:
            self.df[col] = self.df[col].astype(float)
        self.df["energy"] = self.df["lmp"] - self.df["congestion"] - self.df["loss"]

    def _filter_existing_data(self):
        """remove data that may have already been loaded"""
        if self.stream_record.first_date is None and self.stream_record.last_date is None:
            pass
        else:
            stream_first_date = np.datetime64(self._localize_db_dt(self.stream_record.first_date))
            stream_last_date = np.datetime64(self._localize_db_dt(self.stream_record.last_date))
            if (stream_first_date is None) ^ (stream_last_date is None):
                raise ValueError("first date and last date must both be set or both be None")
            if stream_first_date is not None and stream_last_date is not None:
                self.df = self.df.loc[
                    (self.df["dt"] < stream_first_date)
                    | (self.df["dt"] > stream_last_date)
                ]

    def _load_data(self):
        """ """
        tmp_table = "lmp_tmp"
        node_id = self.stream_record.location_id
        # clear loading table
        query = sql.SQL("delete from {tmp_table} where node_id = {node_id}").format(
            tmp_table=sql.Identifier(tmp_table), node_id=sql.Literal(node_id)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        # load data
        self.u.insert_df(df=self.df, table_name=tmp_table)
        # move data to main table
        main_table = self.stream_record.db_info.table
        query = sql.SQL(
            """ INSERT INTO {main_table} (iso_id, node_id, dt, lmp, energy, congestion, loss) SELECT iso_id, node_id,
                             dt at time zone 'US/Central' at time zone 'UTC' as dt,
                             lmp, energy, congestion, loss
                         from {tmp_table} where node_id = {node_id}"""
        ).format(
            main_table=sql.Identifier(main_table),
            tmp_table=sql.Identifier(tmp_table),
            node_id=sql.Literal(node_id),
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        # clear loading table
        query = sql.SQL("delete from {tmp_table} where node_id = {node_id}").format(
            tmp_table=sql.Identifier(tmp_table), node_id=sql.Literal(node_id)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        df_first_date = self.u.time_converter(self.df["dt"].min(), target="dt")
        df_last_date = self.u.time_converter(self.df["dt"].max(), target="dt")
        stream_first_date = self._localize_db_dt(self.stream_record.first_date)
        stream_last_date = self._localize_db_dt(self.stream_record.last_date)
        if (
            self.stream_record.first_date is None
            or stream_first_date > df_first_date
        ):
            self.stream_record.first_date = df_first_date
        if (
            self.stream_record.last_date is None
            or stream_last_date < df_last_date
        ):
            self.stream_record.last_date = df_last_date
            
    def _convert_tz(self):
        """convert from stream tz to gmt"""
        self.df["dt"] = pd.to_datetime(self.df["dt"], format="mixed")
        self.df = self.hlp.convert_tz(
            df=self.df,
            source_tz=self.stream_record.stream_tz,
            target_tz="GMT",
            date_col="dt",
        )

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
