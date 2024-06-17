"""load price forecas time series
"""
import os
import pandas as pd
import datetime
from psycopg2 import sql
from ts_base import TSBase
import sys

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
from utils import Utils


class PriceFcst(TSBase):
    """ """

    tables = {"DAM": "nrg_da_fcst", "RTM": "nrg_rt_fcst"}

    def __init__(self, s_date, e_date, market_run="all", **kwargs):
        """ """
        super().__init__(
            s_date=s_date, e_date=e_date, market_run=market_run, obs_type=None, **kwargs
        )
        self.table = None
        self.df = None
        self.time = 0
        self.freq = self.kwargs.get("freq", "H")
        self.obs_type_id = self.kwargs.get("obs_type_id", 4)
        self.query = None
        self.is_backtest = self.kwargs.get("is_backtest", None)
        self.model_id = self.kwargs.get("model_id", 3)
        self.da_df = None
        self.rt_df = None
        self.df = None
        self._validate_freq()

    @classmethod
    def get_available_locations(cls, market_run):
        """ """
        u = Utils()
        query = sql.SQL(
            """SELECT distinct name FROM nrg_da_fcst
                        INNER JOIN location l 
                        ON l.id = node_id"""
        )
        res = u.execute_query(query, dict_cursor=False, fetch="all")
        return [x[0] for x in res]

    def run(self):
        """run the process to get data"""
        if self.market_run == "all":
            self._get_data(market_run="DAM")
            self._get_data(market_run="RTM")
            self._merge_df()
        else:
            self._get_data(market_run=self.market_run)

    def _get_table(self, market_run):
        """ """
        try:
            return self.tables[market_run.upper()]
        except KeyError:
            raise ValueError(f"invalid market run {market_run}")
    
    def _get_data(self, market_run):
        """
        """
        query, cols = self._get_query(market_run=market_run)
        dt_col = cols['dt_col']
        lmp_col = cols['lmp_col']
        res = self.u.execute_query(query, dict_cursor=False, fetch="all")
        cols = [dt_col, "node", lmp_col]
        df = pd.DataFrame(res, columns=cols)
        df[dt_col] = pd.to_datetime(df[dt_col])
        # conver df to target freq
        if market_run.upper() == "DAM":
            self.da_df = df
        elif market_run.upper() == "RTM":
            self.rt_df = df

    def _get_query(self, market_run):
        """ """
        # Get innitial data
        table = self._get_table(market_run)
        localize = self.kwargs.get("localize", True)
        if market_run.upper() == "DAM":
            lmp_col = "da_fcst"
        elif market_run.upper() == "RTM":
            lmp_col = "rt_fcst"
        else:
            raise ValueError(f"invalid market run {market_run}")
        # Build query
        if localize:
            query = sql.SQL(
                "SELECT distinct on (dt, name) dt at time zone {local_tz} as dt, "
            ).format(local_tz=sql.Literal(self.u.local_tz))
            dt_col = "ldt"
        else:
            query = sql.SQL("SELECT distinct on (dt, name) dt as dt, ")
            dt_col = "dt"
        query += sql.SQL(
            """ l.name, lmp FROM {table}
                     INNER JOIN location l
                     ON l.id = {table}.node_id
                     WHERE dt >= {s_date} and dt <= {e_date}
                     AND obs_type_id = {obs_type_id}
                     AND model_id = {model_id}
                     ORDER BY 1, 2"""
        ).format(
            table=sql.Identifier(table),
            s_date=sql.Literal(self.s_date),
            e_date=sql.Literal(self.e_date),
            obs_type_id=sql.Literal(self.obs_type_id),
            model_id=sql.Literal(self.model_id),
        )
        if self.is_backtest is not None:
            query += sql.SQL(" and is_backtest = {is_backtest}").format(
                is_backtest=sql.Literal(self.is_backtest)
            )
        if self.locations != "all":
            query += sql.SQL(" and node_id in {tuple(self.location_ids)}").format(
                location_ids=sql.Literal(tuple(self.location_ids))
            )
        cols = {'dt_col': dt_col, 'lmp_col': lmp_col}
        return query, cols
    
    def _merge_df(self):
        """
        """
        if self.kwargs.get('localize', True):
            dt_col = 'ldt'
        else:
            dt_col = 'dt'
        if self.da_df is None or self.rt_df is None:
            raise ValueError("da_df and rt_df must be set")
        if self.freq in ['H', '60T']:
            self.da_df[dt_col] = self.da_df[dt_col].dt.floor('H')
            self.rt_df[dt_col] = self.rt_df[dt_col].dt.floor('H')
            self.rt_df = self.rt_df.groupby([dt_col, 'node']).mean().reset_index()
            self.df = self.rt_df.merge(self.da_df, how='left', on=[dt_col, 'node'])
        elif self.freq == '15T':
            self.rt_df['dt_hourly'] = self.rt_df[dt_col].dt.floor('H')
            # Join da data on to rt_df col dt_hourly to expand da data to 15 min
            self.df = self.rt_df.merge(self.da_df, how='left', left_on=['dt_hourly', 'node'], right_on=[dt_col, 'node'])
            self.df = self.df.drop(columns=['dt_hourly'])

            



