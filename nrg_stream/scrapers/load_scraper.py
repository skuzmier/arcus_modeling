"""
"""
import os
import sys
from psycopg2 import sql

import pandas as pd
import numpy as np

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts"))

from base_scraper import BaseScraper

import sys


class LoadScraper(BaseScraper):
    """ """

    def __init__(self, stream_id, s_date, e_date, **kwargs):
        """ """
        super().__init__(stream_id, s_date, e_date, **kwargs)
        self.expected_tables = ["load"]
        self.debug = self.kwargs.get("debug", False)
        self.missing_data_rules = {
            "warning_cols": ["mw"],
            "error_cols": [],
            "interpolate": False,
            "limit_area": 3,
        }
        self.convert_tz = False
        self._setup()

    def _get_columns(self):
        """get column names, add energy, congestion, and loss if needed"""
        columns = []
        for col in self.api.meta["columns"]:
            columns.append(col["name"].lower())
        self.df.columns = columns
        self.df.rename(columns={"date/time": "dt"}, inplace=True)
        self.df["obs_type_id"] = self.stream_record.stream_data["obs_type_id"]
        self.df["stream_id"] = self.stream_record.stream_id
        self.df["iso_id"] = self.stream_record.iso_id
        self.df["location_id"] = self.stream_record.location_id
        self.df = self.df[self.stream_record.db_info.columns]

    def _load_data(self):
        """load data to tmp table and then"""
        tmp_table = self.stream_record.db_info.table + "_test"
        # clear records for tmp_table
        query = sql.SQL("delete from {tmp_table} where stream_id = {stream_id}").format(
            tmp_table=sql.Identifier(tmp_table),
            stream_id=sql.Literal(self.stream_record.stream_id),
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        # load data to tmp table
        self.u.insert_df(self.df, tmp_table)
        # move data to main table and convert tz
        main_table = self.stream_record.db_info.table
        query = sql.SQL(
            """ INSERT INTO {main_table} (dt, stream_id, location_id, obs_type_id, mw)
                            SELECT dt at time zone 'US/Central' at time zone 'UTC' as dt,
                            stream_id, location_id, obs_type_id, mw
                            FROM {tmp_table}
                            WHERE location_id = {location_id}"""
        ).format(
            main_table=sql.Identifier(main_table),
            tmp_table=sql.Identifier(tmp_table),
            location_id=sql.Literal(self.stream_record.location_id),
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        # clear records for tmp_table
        query = sql.SQL("delete from {tmp_table} where stream_id = {stream_id}").format(
            tmp_table=sql.Identifier(tmp_table),
            stream_id=sql.Literal(self.stream_record.stream_id),
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        # update stream record
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
