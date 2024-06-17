"""
"""

import os
import sys
import pandas as pd
import numpy as np

import psycopg2
from psycopg2 import sql

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))

from utils import Utils
from helpers import Helpers
from nrg_stream import NRGStream
from feature_record import FeatureRecord
from feature_scraper_job import FeatureScraperJob


class FeatureScraper:
    """
    """
    def __init__(self, scraper_job_id=None, **kwargs):
        """
        """
        self.u = Utils()
        self.scraper_job_id = scraper_job_id
        self.scraper_job = kwargs.get('scraper_job', None)
        self.feature_record = kwargs.get('feature_record', None)
        self.data = None
        self.debug = False
        self.api = None
    
    def _setup(self):
        """
        """
        self._get_scraper_job()
        self._get_feature_record()
    
    def run(self):
        """
        """
        self._setup()
        self.scraper_job.status = 'running'
        try:
            self._get_data()
            self._format_data()
            self._load_data()
            self._update_feature_record()
            self.feature_record.sync_params.update_from_sync(api_metadata=self.api.meta, has_error=False)
            self.feature_record.save()
            self.scraper_job.status = 'complete'
        except Exception as e:
            self.scraper_job.status = 'failed'
            self.feature_record.sync_params.update_from_sync(has_error=True)
            self.feature_record.save()
            raise e
    
    def _get_scraper_job(self):
        """
        """
        if isinstance(self.scraper_job, FeatureScraperJob):
            return None
        self.scraper_job = FeatureScraperJob(id=self.scraper_job_id)
        if self.scraper_job.id is None:
            raise ValueError(f"no scraper job with id {self.scraper_job_id}")
    
    def _get_feature_record(self):
        """
        """
        if isinstance(self.feature_record, FeatureRecord):
            return None
        self.feature_record = FeatureRecord(id=self.scraper_job.feature_id)
        if self.feature_record is None:
            raise ValueError(f"no feature record with id {self.scraper_job.feature_id}")
    
    def _get_data(self):
        """
        """
        self.api = NRGStream(username="spencer", password="nrg4spencer")
        self.api.get_stream(stream_id=self.feature_record.stream_id, 
                              s_date=self.scraper_job.s_date,
                              e_date=self.scraper_job.e_date,
                              data_option=self.feature_record.data_option,
                              data_format='json')
        self.data = self.api.data
        self._validate_data()
    
    def _validate_data(self):
        """basic data validation
        """
        if len(self.data) == 0:
            raise ValueError("no data returned")
        if len(self.data.columns) != 2:
            raise ValueError("data must have 2 columns")
    
    def _format_data(self):
        """
        """
        self.data.columns = ['dt', 'value']
        #add feature id as postgres id
        self.data['feature_id'] = self.feature_record.pg_id
        self.data = self.data[['feature_id', 'dt', 'value']]
        self.data = self.data.loc[self.data['value'] != '']
        self.data['dt'] = pd.to_datetime(self.data['dt'], format='mixed')
        self.data['value'] = pd.to_numeric(self.data['value'])
    
    def _update_feature_record(self):
        """
        """
        self.feature_record.first_date = self.data['dt'].min()
        self.feature_record.last_date = self.data['dt'].max()

    def _load_data(self) -> None:
        """
        """
        if self.debug:
            return None
        if self.data is None or len(self.data) == 0:
            raise ValueError("no data to load")
        self._clear_staging_table()
        #load new data into staging table
        self.u.insert_df(df=self.data, table_name='feature_loader')
        #find missing timestamps
        self._find_missing_timestamps()
        #insert new data into main table, first try to insert, if cardinality violation, handle it
        try:
            self._insert_to_production_table()
        #handle cardinality violation or runtime error
        except (psycopg2.errors.UniqueViolation, RuntimeError, psycopg2.errors.UniqueViolation):
            self._insert_handle_cardinality_violation()
        except Exception as e:
            self._clear_staging_table()
            self._clear_missing_data_on_failure()
            raise e
        # delete data from staging table
        self._clear_staging_table()

    def _clear_staging_table(self):
        """
        """
        query = "DELETE FROM feature_loader WHERE feature_id = {feature_id}"
        query = sql.SQL(query).format(
            feature_id=sql.Literal(self.feature_record.pg_id)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
    
    def _clear_missing_data_on_failure(self):
        """clear any missing data records for the feature and date range on failure
        """
        query = """DELETE FROM missing_data where feature_id = {feature_id}
                   and dt between {s_date} and {e_date}"""
        query = sql.SQL(query).format(
            feature_id=sql.Literal(self.feature_record.pg_id),
            s_date=sql.Literal(self.scraper_job.s_date),
            e_date=sql.Literal(self.scraper_job.e_date)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
    
    def _find_missing_timestamps(self):
        """find missing data in the feature loader table and insert timestamps into missing_data table
        """
        #don't count dates not yet available as missing
        e_date = self.u.time_converter(self.data['dt'].max(), target='dt') 
        query = """with missing_data as (
                    select generate_series({s_date}, {e_date}, interval {data_freq}) as dt
                    except
                    select dt from feature_loader where feature_id = {feature_id}
                    )
                    insert into missing_data (feature_id, dt)
                    select {feature_id}, dt from missing_data"""
        query = sql.SQL(query).format(
            s_date=sql.Literal(self.scraper_job.s_date),
            e_date=sql.Literal(e_date),
            data_freq=sql.Literal(self.feature_record.freq),
            feature_id=sql.Literal(self.feature_record.pg_id)
        )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
    
    def _insert_to_production_table(self):
        """
        """
        query = """ INSERT INTO {main_table} (feature_id, dt, value)
                    SELECT feature_id, (dt at time zone {stream_tz}) at time zone 'UTC', value
                    from feature_loader
                    WHERE feature_id = {feature_id}
                    ON CONFLICT (feature_id, dt) DO 
                    UPDATE SET value = EXCLUDED.value,
                    updated_at = now()"""
        query = sql.SQL(query).format(
                main_table=sql.Identifier(self.feature_record.table),
                stream_tz=sql.Literal(self.feature_record.source_tz),
                feature_id=sql.Literal(self.feature_record.pg_id)
            )
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)
    
    def _insert_handle_cardinality_violation(self):
        """handle cardinality violation by inserting the new data into the main table
             This is almost always a timezone conversion issue around DST
        """
        query = """ INSERT INTO {main_table} (feature_id, dt, value)
                        SELECT feature_id, dt, value
                        FROM (
                            SELECT feature_id, (dt at time zone {stream_tz}) at time zone 'UTC' as dt, value,
                            ROW_NUMBER() OVER(PARTITION BY feature_id, (dt at time zone {stream_tz}) at time zone 'UTC' ORDER BY dt) as rn
                            FROM feature_loader
                            WHERE feature_id = {feature_id}
                        ) sub
                        WHERE sub.rn = 1
                        ON CONFLICT (feature_id, dt) DO 
                        UPDATE SET value = EXCLUDED.value,
                        updated_at = now()
                        """
        query = sql.SQL(query).format(
                main_table=sql.Identifier(self.feature_record.table),
                stream_tz=sql.Literal(self.feature_record.source_tz),
                feature_id=sql.Literal(self.feature_record.pg_id))
        self.u.execute_query(query, dict_cursor=False, fetch=None, commit=True)


