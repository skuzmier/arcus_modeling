"""
"""
import os
import sys

from psycopg2 import sql
import pandas as pd
import numpy as np

sys.path.append(os.path.expanduser("~/ercot_virts"))
sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))


from base_scraper import BaseScraper

import sys


class ModelRunScraper(BaseScraper):
    """ """

    def __init__(self, stream_id, s_date, e_date, **kwargs):
        """ """
        super().__init__(stream_id, s_date, e_date, **kwargs)
        self.expected_tables = ["nrg_da_fcst", "nrg_rt_fcst"]
        self.debug = self.kwargs.get("debug", False)
        self.missing_data_rules = {
            "warning_cols": ["lmp"],
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
        self.df.rename(columns={"date/time": "dt", "price": "lmp"}, inplace=True)
        self.df["obs_type_id"] = self.stream_record.stream_data["obs_type_id"]
        # self.df["stream_id"] = self.stream_record.stream_id
        self.df["iso_id"] = self.stream_record.iso_id
        self.df["node_id"] = self.stream_record.location_id
        self.df["is_backtest"] = False
        self.df["model_id"] = self.stream_record.db_info.model_id
        self.df = self.df[
            ["model_id", "iso_id", "obs_type_id", "node_id", "dt", "lmp", "is_backtest"]]

    def _load_data(self):
        """custom data loading for model runs"""
        # clear loading table
        tmp_table = self.stream_record.db_info.table + "_test"
        node_id = self.stream_record.location_id
        query = sql.SQL("delete from {tmp_table} where node_id = {node_id}").format(
            tmp_table=sql.Identifier(tmp_table), node_id=sql.Literal(node_id)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
        # load data
        self.u.insert_df(df=self.df, table_name=tmp_table)
        # move data to main table
        main_table = self.stream_record.db_info.table
        query = sql.SQL(
            """ INSERT INTO {main_table} (model_id, iso_id, obs_type_id,
                         node_id, dt, lmp, is_backtest) SELECT model_id, iso_id, obs_type_id, node_id,
                            dt at time zone 'US/Central' at time zone 'UTC' as dt,
                            lmp, is_backtest
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
        # update stream record
        if (
            self.stream_record.first_date is None
            or self.stream_record.first_date > self.df["dt"].min()
        ):
            self.stream_record.first_date = self.u.time_converter(
                self.df["dt"].min(), target="dt"
            )
        if (
            self.stream_record.last_date is None
            or self.stream_record.last_date < self.df["dt"].max()
        ):
            self.stream_record.last_date = self.u.time_converter(
                self.df["dt"].max(), target="dt"
            )
