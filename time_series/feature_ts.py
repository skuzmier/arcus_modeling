"""
"""
import os
import sys
import datetime
import pytz
import json

import pandas as pd
from psycopg2 import sql
sys.path.append(os.path.expanduser('~/ercot_virts/utils'))
sys.path.append(os.path.expanduser('~/ercot_virts/nrg_stream/scrapers'))
from utils import Utils
from feature_record import FeatureRecord


def stream_mapping(stream_id, stream_name, data_option=None, feature_id=None):
    """
    """
    return {
        "stream_id": stream_id,
        "stream_name": stream_name,
        "data_option": data_option,
        "feature_id": feature_id,
    } 


class FeatureTS:
    """object for retrieving feature data as timeseries
    """
    def __init__(self, s_date, e_date, feature_mappings, **kwargs):
        """
            Args:
            -----
            s_date: str, datetime.datetime
                start date for the time series (inclusive)
            e_date: str, datetime.datetime
                end date for the time series (exclusive)
            feature_mappings: list of dicts
                list of dicts with stream_id, stream_name, data_option, and feature_id
                    Either stream_id, stream_name, and data_option are required
                    OR
                    feature_id and stream_name are required
            kwargs: dict
        """
        self.u = Utils()
        self.s_date = s_date
        self.e_date = e_date
        self.feature_mappings = feature_mappings
        self.kwargs = kwargs
        self.tgt_freq = self.kwargs.get('tgt_freq', None)
        self.is_utc = self.kwargs.get('is_utc', False) #are the start and end dates in UTC, base assumption is that they are in local time
        self.interp_method = self.kwargs.get('interp_method', 'cubic') # interpolation method
        self.df = None
        self.source_tz = None
        self.query_time = None
        self.freq = None
        self._setup()

    def _setup(self) -> None:
        """validate the input and get the feature_ids
        """
        self._validate_dates()
        self._remove_duplicate_feature_mappings()
        self._get_features_mappings_with_aliases()
        self._get_feature_ids()
        self._validate_single_table()
        self._get_source_tz()
        overmapped_features = self._get_overmapped_features()
        if len(overmapped_features) > 0:
            raise ValueError(f"stream_names mapped to multiple feature_ids: {overmapped_features}")

    def _convert_to_pivoted_df(self, res):
        """convert the query result to a pivoted dataframe
        """
        self.df = pd.DataFrame(res)
        self.df.columns = ['feature_id', 'ldt', 'value']
        rename_map_df = pd.DataFrame(self.feature_mappings)
        rename_map_df = rename_map_df[['feature_id', 'stream_name']]
        # convert feature_id to string
        rename_map_df = rename_map_df.astype({'feature_id': str})
        self.df = self.df.astype({'feature_id': str})
        # merge the feature_id to stream_name mapping
        self.df = self.df.merge(rename_map_df, on='feature_id', how='left')
        # drop feature_id column
        self.df.drop(columns='feature_id', inplace=True)
        # pivot the dataframe to get the feature values as columns
        self.df = self.df.pivot(index='ldt', columns='stream_name', values='value')
        self.df.reset_index(inplace=True)
        self.df.columns.name = None

    def _validate_dates(self) -> None:
        """convert date inputs, and validate that start date is before end date
        """
        self.s_date = self.u.time_converter(self.s_date, target="dt")
        self.e_date = self.u.time_converter(self.e_date, target="dt")
        if self.s_date >= self.e_date:
            raise ValueError("start date must be before end date")

    def _remove_duplicate_feature_mappings(self) -> None:
        """convert stream_mappings to a list of dicts, and remove duplicates
        """
        self.feature_mappings = [
            json.loads(i)
            for i in set(json.dumps(d, sort_keys=True) for d in self.feature_mappings)
        ]

    def _get_features_mappings_with_aliases(self) -> None:
        """handle feature mapping inputs where multiple names 
            are provided for the same stream_id and data options. 
            Create a list of feature mappings with aliases, reverse this later
        """
        # create a results dict with concatenated stream_id and data_option as key
        results = {}
        for mapping in self.feature_mappings:
            key = f"{mapping['stream_id']}_{mapping['data_option']}"
            if key not in results:
                results[key] = mapping
                results[key]['aliases'] = []
            else:
                results[key]['aliases'] = results[key]['aliases'].append(mapping['stream_name'])
        self.feature_mappings = list(results.values())

    def _convert_to_continuous(self) -> None:
        """convert the dataframe to a continuous timeseries, and estimate the frequency
        """
        # get the minimum timedelta
        timedelta = self.df['ldt'].diff().min()
        # get frequency string
        components = timedelta.components
        if components.days > 0:
            self.freq = f'{components.days}D'
        elif components.hours > 0:
            self.freq = f'{components.hours}H'
        elif components.minutes > 0:
            self.freq = f'{components.minutes}T'
        # convert to continuous timeseries
        self.df = self.df.set_index('ldt')
        self.df = self.df.asfreq(self.freq, method=None) 
        # self.df.reset_index(inplace=True)

    def _get_feature_ids(self) -> None:
        """get the feature ids from postgres, these are unique on stream_id, data_option if a stream_id exists
            This does not need to return anything due to the way the feature_mappings is structured
            In cases where feature_id is provided in the feature_mappings, we will use that feature_id
            #FIXME: This will not work if there are multiple feature_ids for the same stream_id. 
                    Need to handle the multiple data options case
        """
        tgt_streams = [x for x in self.feature_mappings if x['feature_id'] is None]
        # handle case where all feature_ids are provided
        if len(tgt_streams) == 0:
            return None
        tgt_streams_dict = {int(x['stream_id']): x for x in tgt_streams}
        # Construct the query to get the feature_id, perspective, and table_name
        composed_list = [
            sql.SQL("""SELECT stream_id, id as feature_id, perspective, table_name 
                        FROM features 
                        WHERE (stream_id, COALESCE(data_option, 'null')) 
                        IN (SELECT stream_id, 
                            COALESCE(data_option, 'null') 
                        FROM ( VALUES """) ]
        # construct the values part of the query loop through the tgt_streams
        for (ix, val) in enumerate(tgt_streams):
            if ix == len(tgt_streams) - 1: # dont add comma to last element
                query_part = sql.SQL(" ({stream_id}, {data_option}) ")
            else:
                query_part = sql.SQL(" ({stream_id}, {data_option}), ")
            query_part = query_part.format(
                stream_id=sql.Literal(val['stream_id']),
                data_option=sql.Literal(val['data_option'])
            )
            composed_list.append(query_part)
        # add the closing part of the query and execute
        query_part = sql.SQL(" ) AS subquery(stream_id, data_option) )")
        composed_list.append(query_part)
        query = sql.Composed(composed_list)
        res = self.u.execute_query(query, dict_cursor=True, fetch='all')
        # update the feature_mappings with the feature_id, perspective, and table_name
        for x in res:
            # tgt_streams_dict[x['stream_id']].update(x)
            tgt_streams_dict[x['stream_id']]['feature_id'] = x['feature_id']
            tgt_streams_dict[x['stream_id']]['perspective'] = x['perspective']
            tgt_streams_dict[x['stream_id']]['table_name'] = x['table_name']

    def _get_source_tz(self):
        """get the timezone of the source data, from a feature record
        """
        feature_record = FeatureRecord(id=self.feature_mappings[0]['feature_id'])
        self.source_tz = feature_record.source_tz

    def _get_overmapped_features(self) -> dict:
        """get stream_names that are mapped to multiple feature_ids
           and return a dict with stream_name as key and feature_ids as value.
           Mapping one name to multiple features is NOT supported and should NEVER occur as it creates ambiguity
        """
        stream_name_feature_id_map = {}
        for mapping in self.feature_mappings:
            if mapping['stream_name'] in stream_name_feature_id_map:
                stream_name_feature_id_map[mapping['stream_name']].append(mapping['feature_id'])
            else:
                stream_name_feature_id_map[mapping['stream_name']] = [mapping['feature_id']]
        return {k: v for k, v in stream_name_feature_id_map.items() if len(v) > 1}

    def _construct_feature_table_query(self):
        """
        """
        if not self.is_utc:
            # localize s_date and e_date to source_tz
            s_date = pytz.timezone(self.source_tz).localize(self.s_date)
            e_date = pytz.timezone(self.source_tz).localize(self.e_date)
        else:
            s_date = self.s_date
            e_date = self.e_date
        table = self.feature_mappings[0]['table_name']
        feature_ids = tuple([x['feature_id'] for x in self.feature_mappings])
        query = sql.SQL("""SELECT feature_id, 
                        dt at time zone {source_tz} as ldt, 
                        value 
                        FROM {table_name} 
                        WHERE 
                        feature_id in {feature_ids} 
                        AND dt >=
                            ({s_date} at TIME ZONE 'UTC')
                        AND dt <
                            ({e_date} at TIME ZONE 'UTC')
                        """).format(
            table_name=sql.Identifier(table),
            feature_ids=sql.Literal(feature_ids),
            s_date=sql.Literal(s_date),
            e_date=sql.Literal(e_date),
            source_tz=sql.Literal(self.source_tz)
        )
        return query

    def _validate_single_table(self):
        """querying across multiple tables is not supported yet
           check if all the features are in the same table
        """
        table_names = [x['table_name'] for x in self.feature_mappings]
        if len(set(table_names)) > 1:
            raise ValueError("querying across multiple tables is not supported yet")

    def _create_alias_columns(self):
        """rebuild columns where one stream has multiple result names.
            Long term we can remove this as one to many mapping is bad practice
            In the medium term this remains for compatability.  
        """
        for mapping in self.feature_mappings:
            if mapping.get('aliases', None) is not None and len(mapping['aliases']) > 0:
                for alias in mapping['aliases']:
                    self.df[alias] = self.df[mapping['stream_name']]

    def _interpolate(self, **kwargs) -> None:
        """interpolate missing values
            NOTE: Default is cubic as it keeps the shape of the curve, with both first and second derivates having the same sign
        """
        #get innterpolation limit
        freq_str = self.df.index.freqstr
        freq_num = self.df.index.freq.n
        if freq_str is None:
            raise ValueError("Frequency string is None")
        if freq_str[-1] == 'T':
            limit = 60 // freq_num * 3 # 3 hours
        elif freq_str[-1] == 'H':
            limit = 3
        elif freq_str[-1] == 'D':
            limit = 1
        else:
            raise ValueError("Invalid frequency string, evaluate the limit manually and update the code") #What data would need to be evaluated here?
        # interpolate missing values
        self.df = self.df.interpolate(method=self.interp_method , **kwargs, limit=limit)
        self.df = self.df.ffill().bfill() # fill any remaining missing values

    def run(self) -> pd.DataFrame:
        """ """
        query = self._construct_feature_table_query()
        query_start = datetime.datetime.now()
        res = self.u.execute_query(query, dict_cursor=False, fetch="all")
        query_end = datetime.datetime.now()
        self.query_time = query_end - query_start
        self._convert_to_pivoted_df(res)
        self._create_alias_columns()
        self._convert_to_continuous()  # create continuous timeseries. Missing data will be interpolated later
        self._interpolate()
        return self.df
