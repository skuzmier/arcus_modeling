"""collection of stream parser stuff
"""



#### PRICES ####

class ERCOTPriceStreamRecordParser:
    """
    """
    tgt_streams = [91407, 91408, 91409, 91405, 242293, 91419, 91421, 91422, 91417, 91415, 91416, 91418, 91420]

    def __init__(self, stream_record):
        """
        """
        self.stream_record = stream_record
        self.res = {}
    
    def run(self):
        """
        """
        if int(self.stream_record['streamId']) not in self.tgt_streams:
            raise ValueError(f'invalid stream id: {self.stream_record["streamId"]}')
        self._get_base_stream_info()
        self._get_db_info()
        return StreamRecord(**self.res)
    
    def _get_base_stream_info(self):
        """
        """
        self.res['stream_id'] = int(self.stream_record['streamId'])
        self.res['iso'] = 'ERCOT'
        self.res['location'] = self.stream_record['nodeId']
        self.res['location_type'] = 'node'
        self.res['available_from'] = datetime.datetime.strptime(self.stream_record['availFromDate'] , '%b-%d-%Y %H:%M:%S')
        self.res['scraper'] = 'price'
        self.res['is_settle'] = True
        stream_data = {}
        stream_data['raw_data'] = self.stream_record
        stream_data['obs_type'] = 'actual'
        stream_data['obs_type_id'] = 1
        stream_data['raw_data'] = self.stream_record
        self.res['stream_data'] = stream_data
    
    def _get_db_info(self):
        """
        """
        db_info = {}
        db_info['table'] = 'rtm_settle'
        db_info['date_col'] = 'dt'
        db_info['table_tz'] = 'GMT'
        db_info['columns'] = ['iso_id', 'dt', 'node_id', 'lmp', 'energy', 'congestion', 'loss']
        self.res['db_info'] = db_info






rtm_settle_prices = [{'stream_id':91407 , 'stream_name': 'Settle_North_HZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91408 , 'stream_name': 'Settle_South_HZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91409 , 'stream_name': 'Settle_West_HZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91405 , 'stream_name': 'Settle_Houston_HZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':242293 , 'stream_name': 'Settle_PAN_HZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91419 , 'stream_name': 'Settle_North_LZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91421 , 'stream_name': 'Settle_South_LZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91422 , 'stream_name': 'Settle_West_LZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91417 , 'stream_name': 'Settle_Houston_LZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91415 , 'stream_name': 'Settle_AEN_LZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91416 , 'stream_name': 'Settle_CPS_LZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91418 , 'stream_name': 'Settle_LCRA_LZ_price', 'default_val': None, 'obs_type': 'actual'},
                     {'stream_id':91420 , 'stream_name': 'Settle_RAYB_LZ_price', 'default_val': None, 'obs_type': 'actual'}]

#### WIND ####

def get_ercot_wind_streams(streams, market):
    """
    """
    res = []
    for stream in streams:
        if stream['streamName'].split(' ')[0] == 'ERC' and 'Wind' in stream['streamName']:
            res.append(stream)
    return res


class ErcotWindStreamRecordParser:
    """
    """
    ignore_streams = [244606, 244604, 244607, 
                      335529, 244603, 322578,
                      102240, 140638]
    def __init__(self, stream_record):
        """
        """
        self.stream_record = stream_record
        self.res = {}
    
    def run(self):
        """
        """
        if self._check_ignore_stream():
            print('ignored stream record %s' % self.stream_record['streamName'])
            return None
        else:
            self._get_base_stream_info()
            self._get_db_info()
            return StreamRecord(**self.res)
    
    def _get_region(self, stream_name):
        """
        """
        stream_name = stream_name.lower()
        if 'north' in stream_name:
            return 'north'
        elif 'houston' in stream_name:
            return 'houston'
        elif 'south' in stream_name:
            return 'south'
        elif 'west' in stream_name:
            return 'West'
        elif 'coast' in stream_name:
            return 'coast'
        elif 'panhandle' in stream_name:
            return 'panhandle'
        elif any([x in stream_name for x in total_synonyms]):
            return 'total'
        else:
            raise ValueError(f'unknown region in stream name: {stream_name}')
    
    def _get_table(self, stream_name):
        """
        """
        stream_name = stream_name.lower()
        if '5 min' in stream_name:
            return 'wind_5min'
        elif 'hour' in stream_name:
            return 'wind'
        else:
            raise ValueError(f'unknown table in stream name: {stream_name}')
    
    def _check_ignore_stream(self):
        """Filter out non-target streams
        """
        if int(self.stream_record['streamId']) in self.ignore_streams:
            return True
        elif 'intra' in self.stream_record['streamName'].lower():
            return True
        elif 'cop-hsl' in self.stream_record['streamName'].lower():
            return True
        elif 'lmp' in self.stream_record['streamName'].lower():
            return True
        elif 'change' in self.stream_record['streamName'].lower():
            return True
        elif 'percent' in self.stream_record['streamName'].lower():
            return True
        elif '1 min' in self.stream_record['streamName'].lower():
            return True
        elif 'solar' in self.stream_record['streamName'].lower():
            return True
        elif 'wind output' in self.stream_record['streamName'].lower():
            return True
        else:
            return False
    
    def _get_obs_type(self):
        """
        """
        stream_name = self.stream_record['streamName'].lower()
        if 'stwpf' in stream_name:
            return 'fcst-stw'
        elif 'wgr' in stream_name:
            return 'fcst-wgr'
        else:
            return 'actual'
    
    def get_obs_type_id(self, obs_type):
        """
        """
        u = Utils()
        obs_types = u.execute_query('SELECT * FROM obs_type', dict_cursor=True, fetch='all')
        obs_types = {x['name']: x['id'] for x in obs_types}
        if obs_type not in obs_types:
            raise ValueError(f'unknown obs type: {obs_type}')
        return obs_types[obs_type] 
    
    def _get_base_stream_info(self):
        """
        """
        self.res['stream_id'] = int(self.stream_record['streamId'])
        self.res['iso'] = 'ERCOT'
        self.res['location'] = self._get_region(self.stream_record['streamName'])
        self.res['location_type'] = 'region'
        self.res['available_from'] = datetime.datetime.strptime(self.stream_record['availFromDate'] , '%b-%d-%Y %H:%M:%S')
        self.res['scraper'] = 'wind'
        stream_data = {}
        stream_data['raw_data'] = self.stream_record
        stream_data['obs_type'] = self._get_obs_type()
        stream_data['obs_type_id'] = self.get_obs_type_id(stream_data['obs_type'])
        stream_data['raw_data'] = self.stream_record
        self.res['stream_data'] = stream_data
    
    def _get_db_info(self):
        """
        """
        db_info = {}
        db_info['table'] = self._get_table(self.stream_record['streamName'])
        db_info['date_col'] = 'dt'
        db_info['table_tz'] = 'GMT'
        db_info['columns'] = ['iso_id', 'dt', 'location_id', 'obs_type_id', 'mw']
        self.res['db_info'] = db_info
        


class ErcotWindObsTypeUpdater:
    """fixed stream record observation type and obs_type_id
        bug caused all records to have fcst-stw as obs type
    """
    def __init__(self):
        """
        """
        self.u = Utils()
        self.obs_type = None
        self.obs_type_id = None
        self.sr = None
        self.modified_records = []
        self.u.mongo.collection = 'stream_records'
    
    def run(self, stream_id):
        """
        """
        self.sr = StreamRecord(stream_id=stream_id)
        if self.sr.db_info.table not in  ['wind', 'wind_5min']:
            raise ValueError(f'invalid table: {sr.db_info.table}')
        self._get_new_obs_type()
        self._get_obs_type_id()
        self._update_stream_record()
    
    def _get_new_obs_type(self):
        """
        """
        stream_name = self.sr.stream_data['raw_data']['streamName'].lower()
        if 'stwpf' in stream_name:
            self.obs_type = 'fcst-stw'
        elif 'wgr' in stream_name:
            self.obs_type = 'fcst-wgr'
        else:
            self.obs_type = 'actual'
    
    def _get_obs_type_id(self):
        """lookup obs type id from db
        """
        u = Utils()
        obs_types = u.execute_query('SELECT * FROM obs_type', dict_cursor=True, fetch='all')
        obs_types = {x['name']: x['id'] for x in obs_types}
        if self.obs_type not in obs_types:
            raise ValueError(f'unknown obs type: {self.obs_type}')
        self.obs_type_id = obs_types[self.obs_type]
    
    def _update_stream_record(self):
        """
        """
        mongo_id = self.sr._id
        stream_data = self.sr.stream_data
        if self.obs_type != stream_data['obs_type']:
            self.modified_records.append(self.sr.stream_id)
        stream_data['obs_type'] = self.obs_type
        stream_data['obs_type_id'] = self.obs_type_id
        self.u.m_conn.update_one({'_id': bson.ObjectId(mongo_id)}, {'$set': {'stream_data': stream_data}})



class AESOWindStreamRecordParser:
    """
    """
    ignored_streams = []

    def __init__(self, stream_records):
        """
        """
        self.stream_records = stream_records
        self.res = {}
    
    def _filter_stream_records(self):
        """
        """
        for stream in self.stream_records:
            if stream['regionCode'] != 'AB':
                self.stream_records.remove(stream)
            if stream['marketTypeCode'] != 'GEN':
                self.stream_records.remove(stream)
            if 'avg' in stream['streamName'].lower():
                self.stream_records.remove(stream)
            if '1 min' in stream['streamName'].lower():
                self.stream_records.remove(stream)
            if 'reserve' in stream['streamName'].lower():
                self.stream_records.remove(stream)
            
    
    def _get_location(self, stream_record):
        """get location name, remove word wind, and format upper case with underscores
        """
        location_words = stream_record['plantCode'].split(' ')
        location_words = [x for x in location_words if x != 'Wind']
        location = '_'.join(location_words).upper()
        return location
    
    def _get_base_stream_info(self, stream_record):
        """
        """
        self.res['stream_id'] = int(stream_record['streamId'])
        self.res['iso'] = 'AESO'
        self.res['location'] = self._get_region(stream_record)
        self.res['location_type'] = 'plant'
        self.res['available_from'] = datetime.datetime.strptime(self.stream_record['availFromDate'] , '%b-%d-%Y %H:%M:%S')
        self.res['scraper'] = 'wind'
        stream_data = {}
        stream_data['raw_data'] = self.stream_record
        stream_data['obs_type'] = 'actual'
        stream_data['obs_type_id'] = self.get_obs_type_id(stream_data['obs_type'])
        stream_data['raw_data'] = self.stream_record
        self.res['stream_data'] = stream_data
    
    def _get_obs_type_id(self):
        """
        """
        

        


def get_wind_streams(streams):
    """
    """
    res = []
    for stream in streams:
        if 'wind' in stream['streamName'].lower() and stream['marketTypeCode'] == 'GEN':
            if 'avg' in stream['streamName'].lower():
                continue
            if '1 min' in stream['streamName'].lower():
                continue
            else:
                res.append(stream)
    return res






def update_obs_type(stream_record):
    """
    """
    stream_name = stream_record['streamName'].lower()
    if 'stwpf' in stream_name:
        obs_type = 'fcst-stw'
    elif 'wgr' in stream_name:
        'fcst-wgr'
    else:
        return 'actual'





tgt_wind_streams = []
errs = []
for stream in wind_streams:
    try:
        srp = ErcotWindStreamRecordParser(stream)
        sr = srp.run()
        if sr is not None:
            tgt_wind_streams.append(sr)
    except Exception as e:
        print(e)
        errs.append(stream)


#### LOAD ####


class ErcotLoadStreamRecordParser:
    """
    """
    ignore_streams = [198154, 976890, 2704, 2705]
    def __init__(self, stream_record):
        """
        """
        self.stream_record = stream_record
        self.res = {}
    
    def run(self):
        """
        """
        self._get_base_stream_info()
        self._get_db_info()
        return StreamRecord(**self.res)
    
    def _get_region(self, stream_record):
        """
        """
        zone_name_mapping = {'101': 'total', '304': 'coast',
                             '29742': 'wz_east', '306': 'wz_far_west',
                             '311': 'north_central', '307': 'wz_north',
                             '309': 'wz_south_central', '308': 'wz_south',
                             '310': 'wz_west', '182': 'LZ_HOUSTON', '174': 'LZ_NORTH',
                             '341': 'north_east', '175': 'LZ_SOUTH', '176': 'LZ_WEST',
                             }
        zone_id = self.stream_record['zoneId']
        if zone_id not in zone_name_mapping:
            raise ValueError(f'unknown zone id: {zone_id}')
        return zone_name_mapping[zone_id]
    
    def _get_obs_type(self):
        """
        """
        stream_name = self.stream_record['streamName'].lower()
        if '9am' in stream_name:
            return 'trade_fcst'
        elif 'actual' in stream_name:
            return 'actual'
        else:
            return 'final_fcst'
    
    def get_obs_type_id(self, obs_type):
        """
        """
        u = Utils()
        obs_types = u.execute_query('SELECT * FROM obs_type', dict_cursor=True, fetch='all')
        obs_types = {x['name']: x['id'] for x in obs_types}
        if obs_type not in obs_types:
            raise ValueError(f'unknown obs type: {obs_type}')
        return obs_types[obs_type] 
    
    def _get_base_stream_info(self):
        """
        """
        self.res['stream_id'] = int(self.stream_record['streamId'])
        self.res['iso'] = 'ERCOT'
        self.res['location'] = self._get_region(self.stream_record['streamName'])
        self.res['location_type'] = 'region'
        self.res['available_from'] = datetime.datetime.strptime(self.stream_record['availFromDate'] , '%b-%d-%Y %H:%M:%S')
        self.res['scraper'] = 'load'
        stream_data = {}
        stream_data['raw_data'] = self.stream_record
        stream_data['obs_type'] = self._get_obs_type()
        stream_data['obs_type_id'] = self.get_obs_type_id(stream_data['obs_type'])
        stream_data['raw_data'] = self.stream_record
        self.res['stream_data'] = stream_data
    
    def _get_db_info(self):
        """
        """
        db_info = {}
        db_info['table'] = 'load'
        db_info['date_col'] = 'dt'
        db_info['table_tz'] = 'GMT'
        db_info['columns'] = ['dt', 'stream_id', 'location_id', 'obs_type_id', 'mw']
        self.res['db_info'] = db_info


class NRGErcotStreamParser:
    """
    """
    ignore_streams = [338135, 350127, 338107, 
                      337912, 338133, 338134,
                      337904, 337912, 337902, 
                      337903]
    def __init__(self, stream_record):
        """
        """
        self.stream_record = stream_record
        self.res = {}
        self.ignored_streams = []
    
    def run(self):
        """
        """
        if self._check_ignore_stream():
            print('ignored stream record %s' % self.stream_record['streamName'])
            self.ignored_streams.append(self.stream_record['streamName'])
            return None
        else:
            self._get_base_stream_info()
            self._get_db_info()
            self._get_model_id()
            return StreamRecord(**self.res)
    
    def _get_region(self, stream_name):
        """
        """
        stream_name = stream_name.lower()
        stream_name_elements = stream_name.split(' ')
        return stream_name_elements[3]
    
    def _get_table(self, stream_name):
        """
        """
        stream_name = stream_name.lower()
        if 'settle' in stream_name:
            return 'nrg_rt_fcst'
        elif 'dam' in stream_name:
            return 'nrg_da_fcst'
        else:
            raise ValueError(f'unknown table in stream name: {stream_name}')
    
    def _check_ignore_stream(self):
        """Filter out non-target streams
        """
        if int(self.stream_record['streamId']) in self.ignore_streams:
            return True
    
    def _get_obs_type(self):
        """
        """
        stream_name = self.stream_record['streamName'].lower()
        if '10am' in stream_name:
            return 'trade_fcst'
        else:
            return 'final_fcst'
    
    def get_obs_type_id(self, obs_type):
        """
        """
        u = Utils()
        obs_types = u.execute_query('SELECT * FROM obs_type', dict_cursor=True, fetch='all')
        obs_types = {x['name']: x['id'] for x in obs_types}
        if obs_type not in obs_types:
            raise ValueError(f'unknown obs type: {obs_type}')
        return obs_types[obs_type] 
    
    def _get_location_type(self):
        """
        """
        location = self.stream_record['streamName'].split(' ')[3]
        if 'HB' in location:
            return 'hub'
        elif 'LZ' in location:
            return 'load_zone'
        else:
            raise ValueError(f'unknown location type: {location}')

    
    def _get_base_stream_info(self):
        """
        """
        self.res['stream_id'] = int(self.stream_record['streamId'])
        self.res['iso'] = 'ERCOT'
        self.res['location'] = self._get_region(self.stream_record['streamName'])
        self.res['location_type'] = self._get_location_type()
        self.res['available_from'] = datetime.datetime.strptime(self.stream_record['availFromDate'] , '%b-%d-%Y %H:%M:%S')
        self.res['scraper'] = 'nrg_ercot_fcst'
        stream_data = {}
        stream_data['raw_data'] = self.stream_record
        stream_data['obs_type'] = self._get_obs_type()
        stream_data['obs_type_id'] = self.get_obs_type_id(stream_data['obs_type'])
        stream_data['raw_data'] = self.stream_record
        self.res['stream_data'] = stream_data
    
    def _get_db_info(self):
        """
        """
        db_info = {}
        db_info['table'] = self._get_table(self.stream_record['streamName'])
        db_info['date_col'] = 'dt'
        db_info['table_tz'] = 'GMT'
        db_info['model_id'] = 1
        db_info['columns'] = ['model_id', 'iso_id', 'obs_type_id', 'dt', 'lmp']
        self.res['db_info'] = db_info
    
    def _get_model_id(self):
        """
        """
        if self.res['db_info']['table'] == 'nrg_da_fcst':
            return 1
        elif self.res['db_info']['table'] == 'nrg_rt_fcst':
            return 2
        else:
            raise ValueError(f'unknown model id for table {self.res["db_info"]["table"]}')






results = []
errs = []
for sr in tgt_streams:
    try:
        res = NRGErcotStreamParser(sr).run()
        results.append(res)
    except Exception as e:
        print(e)
        errs.append(sr)

results = [x for x in results if x is not None]


ignore_stream_names = ['BUS', 'Agg', 'Avail', 'Control', 'HSL', 
                       'Resource', 'RES', 'Year', '4pm', 'Hist', 
                       'Capacity', 'Current', '5 min', '1 min', 'NMFLAT', 'NMLIGHT',
                       'SCED DSR']
foo = [x for x in load_streams if not any([y in x['streamName'] for y in ignore_stream_names])]

def get_location(stream_name):
    """
    """
    stream_name = stream_name.lower()
    location_names = {'coastal': ['coast'],
                      'east': ['east'],
                      'far_east': ['far east'],
                      'far_west': ['far west'],
                      'north_central': ['north central', 'n central'],
                      'north': ['north'],}

def get_zone_info(streams):
    """
    """
    res = {}
    for stream in streams:
        zone_id = stream['zoneId']
        zone_name = stream['zoneCode']
        res[zone_id] = zone_name
    return res

def get_target_data(zone_id, streams):
    """
    """
    if isinstance(zone_id, int):
        zone_id = str(zone_id)
    res = []
    for stream in streams:
        if stream['zoneId'] == zone_id:
            res.append(stream)
    return res



from utils import Utils
u = Utils()
u.mongo.collection = 'stream_records'
wind_streams = u.mongo.find_all({'scraper':}