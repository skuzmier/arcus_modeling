"""scrape feature forecast data from the nrgstream api
"""
import pandas as pd
from feature_scraper import FeatureScraper
from nrg_stream import NRGStream
from psycopg2 import sql


class FeatureForecastScraper(FeatureScraper):
    """
    """
    def __init__(self, **kwargs):
        """
        """
        super().__init__(**kwargs)
    
    def _setup(self):
        """
        NOTE: Overrides FeatureScraper._setup
        """
        super()._setup()
        self._validate_data_option()
    
    def _validate_data_option(self):
        """all forecast scrapers must have a non null data option
        """
        if self.feature_record.data_option is None:
            raise ValueError('feature record must have a data option')
    
    def _validate_data(self):
        """check column colunt
            NOTE: override FeatureScraper._validate_data
        """
        if self.data is None:
            raise ValueError('no data to validate')
        if len(self.data.columns) != 3:
            raise ValueError(f'expected 3 columns, got {len(self.data.columns)}')
    
    def _format_data(self):
        """add columns, reorder columns, format dt
        Note: override FeatureScraper._format_data
        """
        self.data.colums = ['fcst_dt', 'obs_dt', 'value']
        self.data['feature_id'] = self.feature_record.pg_id
        self.data = self.data[['feature_id', 'fcst_dt', 'obs_dt', 'value']]
        self.data = self.data.loc[self.data['value'] != '']
        self.data['fcst_dt'] = pd.to_datetime(self.data['fcst_dt'], format='mixed')
        self.data['obs_dt'] = pd.to_datetime(self.data['obs_dt'], format='mixed')
    
    def _clear_staging_table(self):
        """clear the staging table to prevent duplicate data
        """
        query = """ DELETE FROM feature_forecast_staging WHERE feature_id = {feature_id} """
        query = sql.SQL(query).format(feature_id=sql.Literal(self.feature_record.pg_id))
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
    
    def _find_missing_data(self):
        """NOTE: How do I do this for forecast data? There are multiple versions for the same timestamp,
                 Expected date range shifts for each fcst_dt, I would like to do this with a single db call.
                   This is a placeholder
        """
        pass
    
    def _insert_to_production_table(self):
        """insert data into the production table
        """
        query = """
            INSERT INTO {main_table} (feature_id, fcst_dt, obs_dt, value)
            SELECT feature_id, (fcst_dt at time zone {stream_tz} at time zone 'UTC'), 
            (obs_dt at time zone {stream_tz} at time zone 'UTC'), value
            FROM feature_forecast_staging
            WHERE feature_id = {feature_id}
            ON CONFLICT (feature_id, fcst_dt, obs_dt) DO NOTHING -- We don't want to update forecast data
        """
        query = sql.SQL(query).format(
            main_table=sql.Identifier(self.feature_record.table),
            stream_tz=sql.Literal(self.feature_record.source_tz),
            feature_id=sql.Literal(self.feature_record.pg_id)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
    
    def _insert_handle_cardinality_violation(self):
        """handle cardinality violation, typically caused by DST conversion issue
        """
        query = """ INSERT INTO {main_table} (feature_id, fcst_dt, obs_dt, value)
                    SELECT feature_id, (fcst_dt at time zone {stream_tz} at time zone 'UTC'),
                    (obs_dt at time zone {stream_tz} at time zone 'UTC'), value
                    ROW NUMBER() OVER (PARTITION BY feature_id, fcst_dt, obs_dt) as rn
                    FROM feature_forecast_staging
                    WHERE feature_id = {feature_id}) as sub
                    WHERE sub.rn = 1
                    ON CONFLICT (feature_id, fcst_dt, obs_dt) DO NOTHING"""
        query = sql.SQL(query).format(
            main_table=sql.Identifier(self.feature_record.table),
            stream_tz=sql.Literal(self.feature_record.source_tz),
            feature_id=sql.Literal(self.feature_record.pg_id)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)



import datetime
from utils import Utils

def find_start_date(min_start, end_date, stream_id, data_option, nrg_steram_api=None):
    """
    """
    u = Utils()

    if nrg_stream_api is None:
        nrg_stream_api = NRGStream()
    mid_point = min_start + (end_date - min_start) / 2
    s_date = mid_point
    e_date = mid_point + datetime.timedelta(days=1)
    while min_start < end_date:
        nrg_stream_api.get_data(stream_id, data_option, start_date=s_date, end_date=e_date)
        if len(nrg_stream_api.data) > 0:
            return s_date
        s_date = e_date
        e_date = e_date + datetime.timedelta(days=1)
    nrg_stream_api.get_data(stream_id, data_option, start_date=mid_point)
    #if data is found move to the left of the mid point
    if len(nrg_stream_api.data) > 0:
        return find_start_date(min_start, mid_point, stream_id, data_option, nrg_stream_api)
    #if no data is found move to the right of the mid point
    if len(nrg_stream_api.data) == 0:
        return find_start_date(mid_point, end_date, stream_id, data_option, nrg_stream_api)
    else:

