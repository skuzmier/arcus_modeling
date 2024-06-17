"""
"""
import os
import sys
from psycopg2 import sql
import pandas as pd
import numpy as np

import sys

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))

# from utils import Utils
# from stream_record import StreamRecord
# from nrg_stream import NRGStream
# from helpers import Helpers
from base_scraper import BaseScraper


class WindScraper(BaseScraper):
    """ """

    def __init__(self, stream_id, s_date, e_date, **kwargs):
        """ """
        super().__init__(stream_id, s_date, e_date, **kwargs)
        self.expected_tables = ["wind", "wind_5min"]
        self.missing_data_rules = {
            "warning_cols": ["mw"],
            "error_cols": [],
            "interpolate": False,
            "limit_area": 3,
        }
        self.debug = self.kwargs.get("debug", False)

        self._setup()
        self.convert_tz = False

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
        self.df = self.df[self.stream_record.columns]

    def _load_data(self):
        """load data into test table, convert tz, and insert into main table"""
        # clear loading table
        tmp_table = self.stream_record.db_info.table + "_test"
        location_id = self.stream_record.location_id
        query = sql.SQL(
            "delete from {tmp_table} where location_id = {location_id}"
        ).format(
            tmp_table=sql.Identifier(tmp_table), location_id=sql.Literal(location_id)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        # load data to loading table
        self.u.insert_df(df=self.df, table_name=tmp_table)
        # insert data from loading table to main table
        query = sql.SQL(
            """INSERT INTO {table} (iso_id, location_id, obs_type_id, stream_id, dt, mw)
                            SELECT iso_id, location_id, obs_type_id, stream_id, dt
                            at time zone 'US/Central' at time zone 'UTC' as dt, mw
                            FROM {tmp_table}
                            WHERE location_id = {location_id}"""
        ).format(
            table=sql.Identifier(self.stream_record.db_info.table),
            tmp_table=sql.Identifier(tmp_table),
            location_id=sql.Literal(location_id),
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        # delete data from loading table
        query = sql.SQL(
            "delete from {tmp_table} where location_id = {location_id}"
        ).format(
            tmp_table=sql.Identifier(tmp_table), location_id=sql.Literal(location_id)
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