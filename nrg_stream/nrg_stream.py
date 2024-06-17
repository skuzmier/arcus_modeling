import requests
import pandas as pd
import datetime
from auth import BearerAuth, NRGAuth
from helpers import Helpers


class NRGStream:
    """
    """
    def __init__(self, username=None, password=None):
        """
        """
        self.auth = NRGAuth(username, password)
        self.server = 'https://api.nrgstream.com'
        self.response = None
        self.headers = None
        self.data = None
        self.hlp = Helpers()
        self.meta = None
        self._setup()
    
    def _setup(self):
        """
        """
        self.data_formats = {'csv': 'text/csv', 'json': 'application/json'}
    
    def get_stream(self, stream_id, s_date, e_date, data_format='json', data_option=None):
        """get data for a single stream
        """
        data_format = self.data_formats[data_format]
        params = self._remove_unused_params(fromDate=s_date, 
                                            toDate=e_date,
                                            dataFormat=data_format,
                                            dataOption=data_option)
        url = self.server + f'/api/StreamData/{stream_id}'
        try:
            self.response = requests.get(url, params=params, auth=BearerAuth(self.auth.token))
        except Exception as e:
            print(e)
            self.auth.release_token()
        if self.response.status_code == 200:
            if 'csv' in data_format:
                self.data = pd.read_csv(self.response.content)
            elif 'json' in data_format:
                json_data = self.response.json()
                self.meta = json_data['metaData']
                self.data = pd.DataFrame(json_data['data'])
        self.auth.release_token()
    
    def _remove_unused_params(self, **kwargs) -> dict:
        """parse kwargs to remove None values

            Returns:
            --------
                dict: dictionary of non-None values
        """
        res = {}
        for (key, val) in kwargs.items():
            if val is not None and val != '':
                res[key] = val
        return res
    
    def get_stream_list(self, market=None) -> list:
        """get list of streams
                
            Args:
            -----
                market: str
            
            Returns:
            --------
                list: list of streams
        """
        data_format = self.data_formats['json']
        params = self._remove_unused_params(dataFormat=data_format)
        url = self.server + '/api/StreamList'
        try:
            self.response = requests.get(url, params=params, auth=BearerAuth(self.auth.token))
        except Exception as e:
            print(e)
            self.auth.release_token()
        if self.response.status_code == 200:
            self.data = self.response.json()
        else:
            raise ValueError(f'failed to get stream list: {self.response.status_code} - {self.response.reason}')
        if market is not None:
            self.data = [x for x in self.data if self.hlp.get_market_common_name(x['regionTypeCode']) == market]
        self.auth.release_token()
        return self.data
    
    def get_folder_list(self) -> list:
        """
        """
        data_format = self.data_formats['json']
        params = self._remove_unused_params(dataFormat=data_format)
        url = self.server + '/api/FolderList'
        try:
            self.response = requests.get(url, params=params, auth=BearerAuth(self.auth.token))
        except Exception as e:
            print(e)
            self.auth.release_token()
        if self.response.status_code == 200:
            self.data = self.response.json()
        else:
            raise ValueError(f'failed to get folder list: {self.response.status_code} - {self.response.reason}')
        self.auth.release_token()
        return self.data
    
    def get_streams_in_folder(self, folder_id) -> list:
        """
            Args:
            -----
                folder_id: int
        """
        data_format = self.data_formats['json']
        url = self.server + f'/api/StreamDataUrl'
        params = self._remove_unused_params(folderId=folder_id)
        try:
            self.response = requests.get(url, params=params, auth=BearerAuth(self.auth.token))
        except Exception as e:
            print(e)
            self.auth.release_token()
        if self.response.status_code == 200:
            self.data = self.response.json()
        else:
            raise ValueError(f'failed to get stream list: {self.response.status_code} - {self.response.reason}')
        self.auth.release_token()
        return self.data
    
    def get_stream_data_options(self, stream_id, data_format='json'):
        """
        """

        url = self.server + f'/api/StreamDataOptions/{stream_id}'
        try:
            self.response = requests.get(url, auth=BearerAuth(self.auth.token))
        except Exception as e:
            print(e)
            self.auth.release_token()
        if self.response.status_code == 200:
            self.data = self.response.json()
        else:
            raise ValueError(f'failed to get stream data options: {self.response.status_code} - {self.response.reason}')
        self.auth.release_token()
        return self.data
    
    

# def clear_stream_records(stream_id):
#     """
#     """
#     stream_record = StreamRecord(stream_id=stream_id)
#     stream_record.first_date = None
#     stream_record.last_date = None
#     stream_record.missing_dates = []
#     stream_record.save()
#     #test clearing worked
#     stream_record = StreamRecord(stream_id=stream_id)
#     if len(stream_record.missing_dates) > 0:
#         raise ValueError('failed to clear missing dates')


# def get_ercot_price_streams(streams, market, market_run):
#     """
#     """
#     res = []
#     for stream in streams:
#         stream_desc = stream['streamName'].split(' ')
#         if stream_desc[0] == 'ERC' and stream_desc[3] == market_run and stream_desc[4] == 'LMP' and 'Hub' not in stream_desc:
#             res.append(stream)
#     return res


# def get_ercot_wind_streams(streams, market):
#     """
#     """
#     res = []
#     for stream in streams:
#         if stream['streamName'].split(' ')[0] == 'ERC' and 'Wind' in stream['streamName']:
#             res.append(stream)
#     return res


# def get_ercot_load_streams(streams, market):
#     """
#     """
#     res = []
#     for stream in streams:
#         if stream['streamName'].split(' ')[0] == 'ERC' and 'Load' in stream['streamName']:
#             res.append(stream)
#     return res


# def get_ercot_pwr_streams(streams):
#     """
#     """
#     res = []
#     for stream in streams:
#         if 'ERCOT' in stream['streamName'] and 'pwrstream' in stream['streamName'].lower():
#             res.append(stream)
#         # if stream['streamName'].split(' ')[0] == 'Pwrstream' and 'ERCOT' in stream['streamName']:
#         #     res.append(stream)
#     return res

# wind_streams = []


# def parse_price_stream_record(stream_record):
#     """
#     """
#     res = {}
#     res['stream_id'] = int(stream_record['streamId'])
#     res['iso'] = 'ERCOT'
#     res['location'] = stream_record['nodeId']
#     res['location_type'] = 'node'
#     res['available_from'] = datetime.datetime.strptime(stream_record['availFromDate'] , '%b-%d-%Y %H:%M:%S')
#     res['scraper'] = 'price'
#     stream_data = {}
#     stream_data['market_run'] = stream_record['streamName'].split(' ')[3]
#     stream_data['raw_data'] = stream_record
#     res['stream_data'] = stream_data
#     db_info = {}
#     db_info['table'] = 'da_lmp'
#     db_info['date_col'] = 'dt'
#     db_info['table_tz'] = 'GMT'
#     db_info['columns'] = ['iso_id', 'dt', 'node_id', 'lmp', 'energy', 'congestion', 'loss']
#     res['db_info'] = db_info
#     stream_record = StreamRecord(**res)
#     return stream_record




# def parse_wind_stream_record(stream_record, obs_type, region_name):
#     """
#     """
#     res = {}
#     res['stream_id'] = int(stream_record['streamId'])
#     res['iso'] = 'ERCOT'
#     res['location'] = stream_record['nodeId']
#     res['location_type'] = 'node'
#     res['available_from'] = datetime.datetime.strptime(stream_record['availFromDate'] , '%b-%d-%Y %H:%M:%S')
#     stream_data = {}
#     stream_data['obs_type'] = obs_type
#     stream_data['region_name'] = region_name
#     stream_data['raw_data'] = stream_record
#     res['stream_data'] = stream_data
#     db_info = {}
#     db_info['table'] = 'wind'
#     db_info['date_col'] = 'dt'
#     db_info['table_tz'] = 'GMT'
#     db_info['columns'] = ['iso_id', 'dt', 'location_id', 'obs_type_id', 'mw']
#     res['db_info'] = db_info
#     stream_record = StreamRecord(**res)
#     return stream_record



# class ErcotWindStreamRecordParser:
#     """
#     """
#     ignore_streams = [244606, 244604, 244607, 
#                       335529, 244603, 322578,
#                       102240, 140638]
#     def __init__(self, stream_record):
#         """
#         """
#         self.stream_record = stream_record
#         self.res = {}
#         self.run()
    
#     def run(self):
#         """
#         """
#         if self._check_ignore_stream():
#             return None
#         self._get_base_stream_info()
#         self._get_db_info()
#         return StreamRecord(**self.res)
    
#     def _get_region(self, stream_name):
#         """
#         """
#         stream_name = stream_name.lower()
#         total_synonyms = ['total', 'system wide']
#         if 'north' in stream_name:
#             return 'north'
#         elif 'houston' in stream_name:
#             return 'houston'
#         elif 'south' in stream_name:
#             return 'south'
#         elif 'west' in stream_name:
#             return 'West'
#         elif 'coast' in stream_name:
#             return 'coast'
#         elif 'panhandle' in stream_name:
#             return 'panhandle'
#         elif any([x in stream_name for x in total_synonyms]):
#             return 'total'
#         else:
#             raise ValueError(f'unknown region in stream name: {stream_name}')
    
#     def _get_table(self, stream_name):
#         """
#         """
#         stream_name = stream_name.lower()
#         if '5 min' in stream_name:
#             return 'wind_5min'
#         elif 'hour' in stream_name:
#             return 'wind'
#         else:
#             raise ValueError(f'unknown table in stream name: {stream_name}')
    
#     def _check_ignore_stream(self):
#         """Filter out non-target streams
#         """
#         if int(self.stream_record['streamId']) in self.ignore_streams:
#             return True
#         elif 'intra' in self.stream_record['streamName'].lower():
#             return True
#         elif 'cop-hsl' in self.stream_record['streamName'].lower():
#             return True
#         elif 'lmp' in self.stream_record['streamName'].lower():
#             return True
#         elif 'change' in self.stream_record['streamName'].lower():
#             return True
#         elif 'percent' in self.stream_record['streamName'].lower():
#             return True
#         elif '1 min' in self.stream_record['streamName'].lower():
#             return True
#         elif 'solar' in self.stream_record['streamName'].lower():
#             return True
#         elif 'wind output' in self.stream_record['streamName'].lower():
#             return True
#         else:
#             return False
    
#     def _get_obs_type(self):
#         """
#         """
#         stream_name = self.stream_record['streamName'].lower()
#         if 'stwpf' in stream_name:
#             return 'fcst-stw'
#         elif 'wgr':
#             return 'fcst-wgr'
#         else:
#             return 'actual'
    
#     def get_obs_type_id(self, obs_type):
#         """
#         """
#         u = Utils()
#         obs_types = u.execute_query('SELECT * FROM obs_type', dict_cursor=True, fetch='all')
#         obs_types = {x['name']: x['id'] for x in obs_types}
#         if obs_type not in obs_types:
#             raise ValueError(f'unknown obs type: {obs_type}')
#         return obs_types[obs_type] 
    
#     def _get_base_stream_info(self):
#         """
#         """
#         self.res['stream_id'] = int(self.stream_record['streamId'])
#         self.res['iso'] = 'ERCOT'
#         self.res['location'] = self._get_region(self.stream_record['streamName'])
#         self.res['location_type'] = 'region'
#         self.res['available_from'] = datetime.datetime.strptime(self.stream_record['availFromDate'] , '%b-%d-%Y %H:%M:%S')
#         self.res['scraper'] = 'wind'
#         stream_data = {}
#         stream_data['raw_data'] = self.stream_record
#         stream_data['obs_type'] = self._get_obs_type()
#         stream_data['obs_type_id'] = self.get_obs_type_id(stream_data['obs_type'])
#         stream_data['raw_data'] = self.stream_record
#         self.res['stream_data'] = stream_data
    
#     def _get_db_info(self):
#         """
#         """
#         db_info = {}
#         db_info['table'] = self._get_table(self.stream_record['streamName'])
#         db_info['date_col'] = 'dt'
#         db_info['table_tz'] = 'GMT'
#         db_info['columns'] = ['iso_id', 'dt', 'location_id', 'obs_type_id', 'mw']
    
# tgt_wind_streams = []
# errs = []
# for stream in wind_streams:
#     try:
#         sr = ErcotWindStreamRecordParser(stream)
#         tgt_wind_streams.append(sr)
#     except Exception as e:
#         print(e)
#         errs.append(stream)



#Data Streams:
# 92073 - lmp node.
# 115798 - lmp hub.
# 327197  - ancillary services
# 159280 - solar
# 193002 - wind


# Data stream enrichment.
# aggregation?
#  region_name
# forecast vs actual
