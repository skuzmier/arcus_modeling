"""base timeseries class
"""

import sys
import datetime

import pandas as pd
from psycopg2 import sql

sys.path.append("/home/ubuntu/ercot_virts/utils")
from utils import Utils


class TSBase:
    """ """

    def __init__(self, s_date, e_date, obs_type, market_run=None, **kwargs):
        """ """
        self.u = Utils()
        self.s_date = s_date
        self.e_date = e_date
        self.market_run = market_run
        self.obs_type = obs_type
        self.kwargs = kwargs
        self.obs_type_id = None
        self.locations = kwargs.get("locations", "all")
        self.location_ids = []
        self.freq = self.kwargs.get("freq", "H")
        self.df = None
        self._setup()

    def _setup(self):
        """ """
        for k, v in self.kwargs.items():
            setattr(self, k, v)
        self._validate_dates()
        self._validate_freq()
        self._validate_obs_type()
        self._validate_market_run()
        self._validate_locations()
        self._get_location_ids()
        self._convert_local_to_UTC()

    def _validate_dates(self):
        """ """
        self.s_date = self.u.time_converter(self.s_date, target="dt")
        self.e_date = self.u.time_converter(self.e_date, target="dt")
        if self.s_date >= self.e_date:
            raise ValueError("start date must be before end date")
    
    def _validate_freq(self):
        """
        """
        if self.freq not in ['H', 'D', '5T', '15T', '60T']:
            raise ValueError(f'invalid freq {self.freq}')
    
    def _convert_local_to_UTC(self):
        """
        """
        if not self.kwargs.get('is_utc', False):
            self.s_date = self.u.local_to_UTC(self.s_date)
            self.e_date = self.u.local_to_UTC(self.e_date)

    def _validate_obs_type(self):
        """confirm obs_type parameter exists in table obs_type"""
        if self.obs_type is None:
            return None
        query = sql.SQL("select * from obs_type where name = {}").format(
            sql.Literal(self.obs_type)
        )
        res = self.u.execute_query(query, dict_cursor=True, fetch="one")
        if res is None:
            raise ValueError(f"obs_type {self.obs_type} not found")
        self.obs_type_id = res["id"]

    def _validate_market_run(self):
        """check if market run is in name of valid market runs"""
        if self.market_run is None:
            return None
        valid_market_runs = ["da", "rt", "rtm", "dam", 'all']
        if self.market_run not in valid_market_runs:
            raise ValueError(f"market_run must be one of {valid_market_runs}")

    def _validate_locations(self):
        """confirm location parameter exists in table location"""
        if self.locations == "all":
            return None
        if not isinstance(self.locations, list):
            self.locations = [self.locations]
            # query database to find locations in list and not in table location
        location_values = ",".join([f"'{loc}'" for loc in self.locations])

        # Query database to find locations in list and not in table location
        query = sql.SQL(
                f"""SELECT unnested_value FROM unnest(ARRAY[{location_values}])
                    AS unnested_value WHERE unnested_value NOT IN (SELECT name FROM location)"""
                )
        res = self.u.execute_query(query, dict_cursor=True, fetch="all")
        if len(res) != 0:
            raise ValueError(f"locations {res} not found in table location")
            
    def _get_location_ids(self):
        """
        """
        if self.locations == 'all':
            return None
        else:
            query = sql.SQL("""select id from location where name = ANY({location_names})""").format(
                location_names=sql.Literal(self.locations)
            )
            res = self.u.execute_query(query, dict_cursor=True, fetch="all")
            self.location_ids = [x['id'] for x in res]
    
    # def _convert_freq(self, df, tgt_freq, dt_col):
    #     """convert data frequency to allign da and rt data

    #         Args:
    #         ------
    #             df (pd.DataFrame): data to convert
    #             tgt_freq (str): target frequency
    #             dt_col (str): name of datetime column
            
    #         Returns:
    #         --------
    #             pd.DataFrame
    #     """
    #     if dt_col not in df.columns:
    #         raise ValueError(f'{dt_col} not in df columns')
    #     #create index of dt and node
    #     df['index_col'] = df['node'] + '__' + df[dt_col].dt.strftime('%Y-%m-%d %H:%M:%S')
    #     df = df.set_index('index_col')
    #     #convert to target frequency
    #     if tgt_freq == '15T':
    #         df = df.resample(tgt_freq).mean()
    #     elif tgt_freq in ['H', '60T']:
    #         df = df.resample(tgt_freq).ffill()
    #     #reset index and split index_col
    #     df = df.reset_index()
    #     df['node'] = df['index_col'].apply(lambda x: x.split('__')[0])
    #     df[dt_col] = df['index_col'].apply(lambda x: x.split('__')[1])
    #     df[dt_col] = pd.to_datetime(df[dt_col])
    #     return df
    
