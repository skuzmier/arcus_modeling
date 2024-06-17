"""
"""


import math as m
import datetime
import sys
import bson
import os

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))

from utils import Utils
from nrg_stream import NRGStream
from feature_record import FeatureRecord


class UpdateScheduleInference:
    u = Utils()

    def __init__(self, id=None, feature_record=None, **kwargs):
        self._id = id
        self._feature_record = feature_record
        self.kwargs = kwargs
        self.last_run = None
        self.next_run = datetime.datetime(2020, 1, 1) # set to a date in the past to trigger the first run
        self.estimated_update_freq = None
        self.estimated_days_ahead = None
        self.stream_id = None
        self.feature_record_id = None
        self.meta_data = None
        self.last_updated_obs = []
        self.last_effective_obs = []
        self.active = True
        self.freq_agreement_cnt = 0  # number of times the update frequency has been agreed upon
        self.days_agreement_cnt = 0  # number of times the days ahead has been agreed upon
        self.created_at = None
        self.updated_at = None
        self.object_type = "update_schedule_inference"
        self._setup()

    @classmethod
    def get_records(cls, **kwargs):
        """Enables full mongo search with kwargs
        for equivilency pass standard as args,
        for full search write pymongo query as a dict and pass in as kwargs
        """
        cls.u.mongo.collection = 'update_schedule_inference'
        if len(kwargs) == 0:
            kwargs = {"object_type": "update_schedule_inference"}
        res = cls.u.m_conn.find(kwargs)
        res = [cls(**x) for x in res]
        return res

    @property
    def feature_record(self):
        """return the feature record object, if not loaded
        """
        if self._feature_record is not None:
            return self._feature_record
        else:
            self._feature_record = FeatureRecord(id=self.feature_record_id)
            return self._feature_record
    
    @property
    def id(self):
        """return the id of the object
        """
        return self._id

    def _setup(self):
        """
        """
        self.u.mongo.collection = "update_schedule_inference"
        if len(self.kwargs) > 0:
            self._from_dict(self.kwargs)
        elif self._id is not None:
            self._from_id()
        elif self.feature_record is not None:
            self._from_feature_record()
        else:
            raise ValueError("feature_record or _id is required")

    def _from_feature_record(self):
        """create the object from a feature record object
           set the stream_id, estimated_update_freq, estimated_days_ahead, and feature_record_id
        """
        if not isinstance(self.feature_record, FeatureRecord):
            raise ValueError("feature_record must be a FeatureRecord object")
        existing_id = self._check_existing()
        if existing_id:
            self._id = existing_id
            self._from_id()
            return None
        self.stream_id = self.feature_record.stream_id
        self.estimated_update_freq = self.feature_record.freq_minutes
        self.estimated_days_ahead = 0
        self.feature_record_id = self.feature_record.id
        self.created_at = datetime.datetime.now(datetime.timezone.utc)

    def _from_id(self):
        """
        """
        if isinstance(self._id, str):
            self._id = bson.ObjectId(self._id)
        query = {"_id": self._id, "object_type": self.object_type}
        res = self.u.m_conn.find_one(query)
        if res is None:
            raise ValueError(f"no record found with id: {self._id}")
        self._from_dict(res)

    def _from_dict(self, config_dict):
        """
        """
        if not isinstance(config_dict, dict):
            raise ValueError("config_dict must be a dict")
        if "feature_record_id" not in config_dict:
            raise ValueError("feature_record_id is required")
        for (k, v) in config_dict.items():
            setattr(self, k, v)

    def _get_metadata(self):
        # Make an API call to NRGStream to get the metadata for the stream
        api = NRGStream()
        api.get_stream(
            s_date="04-01-2024", e_date="04-05-2024", stream_id=self.stream_id
        )
        self.meta_data = api.meta
        return self.meta_data

    def _infer_update_frequency(self):
        """estimate the update frenquency of the stream
            Store unique lastUpdatedUTC values in a list and calculate the average time between updates
            Round the average time to the nearest natural interval (1, 5, 15, 60, 1440)
        """
        if self.meta_data is None:
            self._get_metadata()
        last_updated_utc = self.u.time_converter(self.meta_data["lastUpdatedUTC"], target='dt')
        if last_updated_utc not in self.last_updated_obs:
            self.last_updated_obs.append(last_updated_utc)

        if len(self.last_updated_obs) > 1:
            time_diffs = []
            for i in range(1, len(self.last_updated_obs)):
                t1 = self.u.time_converter(self.last_updated_obs[i - 1], target='dt').replace(tzinfo=None) #This is need because I loaded some data as strings in the past
                t2 = self.u.time_converter(self.last_updated_obs[i], target='dt').replace(tzinfo=None)
                diff = t2 - t1
                time_diffs.append(diff.total_seconds() / 60)  # Convert to minutes

            avg_diff = sum(time_diffs) / len(time_diffs)
            new_interval = self._round_to_nearest_interval(avg_diff)
            has_new_data = self._has_new_data()
            if new_interval != self.estimated_update_freq:
                self.agreement_cnt = 0 # reset agreement count if the interval estimate changes
            elif has_new_data:
                self.agreement_cnt += 1 # increment agreement count if the interval estimate stays the same and new data is available

    def _infer_days_ahead(self):
        """Estimate the number of days ahead the data is available, if any
        """
        if self.meta_data is None:
            self._get_metadata()
        
        last_effective = self.u.time_converter(self.meta_data["last_effective"], target='dt').replace(tzinfo=None)
        if last_effective not in self.last_effective_obs:
            self.last_effective_obs.append(last_effective)
        self.last_effective_obs.append(last_effective)

        if len(self.last_effective_obs) > 1:
            seconds_ahead = (
                self.last_effective_obs[-1] - self.last_effective_obs[-2]
            ).total_seconds()  
            new_days_ahead = m.ceil(seconds_ahead / 86400)  # Convert to days
            if new_days_ahead > self.estimated_days_ahead:
                self.days_agreement_cnt = 0
                self.estimated_days_ahead = new_days_ahead
            else:
                self.days_agreement_cnt += 1

    def _round_to_nearest_interval(self, minutes):
        intervals = [
            1,
            5,
            15,
            60,
            1440,
        ]  # 1 minute, 5 minutes, 15 minutes, 1 hour, 1 day
        return min(intervals, key=lambda x: abs(x - minutes))

    def _has_new_data(self):
        """check if the last_updated_utc is different from the last time the data was updated"""
        if self.meta_data is None:
            self._get_metadata()
        last_updated_utc = self.meta_data["lastUpdatedUTC"]
        if last_updated_utc not in self.last_updated_obs:
            self.last_updated_obs.append(last_updated_utc)
            return True
        return False

    def _schedule_next_run(self):
        """
        """
        if self._check_data_stable():
            self._update_sync_params()
            self.active = False
            return None
        # Determine the next run time based on the estimated update frequency
        self.next_run = datetime.datetime.now() + datetime.timedelta(
            minutes=self.estimated_update_freq
        )
    
    def _check_data_stable(self):
        """the update frequency is stable if the agreement count is greater than 4
        """
        return self.days_agreement_cnt > 4 and self.freq_agreement_cnt > 4
    
    def _update_sync_params(self):
        """check if values are different and update the sync params if they are
        """
        if self.feature_record.sync_params.stream_update_freq != self.estimated_update_freq:
            self.feature_record.sync_params.stream_update_freq = self.estimated_update_freq
        if self.feature_record.sync_params.days_ahead != self.estimated_days_ahead:
            self.feature_record.sync_params.days_ahead = self.estimated_days_ahead
    
    def _check_complete(self):
        """if the data is stable, update the sync params and set the active flag to False
        """
        if self._check_data_stable():
            self._update_sync_params()
            self.active = False
            return True
        return False
    
    def _check_existing(self):
        """check if a a record exists with the same feature_record_id
        """
        query = {"feature_record_id": self.feature_record_id}
        res = self.u.m_conn.find_one(query)
        if res is not None:
            return res["_id"]
        return False
    
    def to_dict(self):
        """
        """
        res = {}
        for k in self.__dict__.keys():
            if k.startswith("_") or k in ["u", "meta_data"]:
                continue
            res[k] = getattr(self, k)
        if self._id is not None:
            res["_id"] = self._id
        return res
    
    def save(self):
        """
        """
        self.updated_at = datetime.datetime.now(datetime.timezone.utc)
        if self._id is not None: #update existing record
            query = {"_id": self._id}
            self.u.m_conn.update_one(query, {"$set": self.to_dict()})
        else:
            self._id = self.u.m_conn.insert_one(self.to_dict()).inserted_id

    def run(self):
        self._get_metadata()
        self._infer_update_frequency()
        self._infer_days_ahead()
        self._update_sync_params()
        self._schedule_next_run()
        # Perform data loading and storage using pymongo
        # Update self.last_run with the current timestamp
        self.last_run = datetime.datetime.now(datetime.timezone.utc)
        self.save()


"""

#Notes on update schedule inferencing:
  - I want to know how often a stream is updated on the NRGStream side
    - The possible update frequencies are 1 minute, 5 minutes, 15 minutes, 1 hour, 1 day
    - The data update will always be greater than or equal to the freq_minutes property of the feature record
  - I want to know how far in advance the data is available if if is a forecast
  - I want to know what offset to apply for checking the sync, i.e. if the data comes out on the hour, how many minutes before NRGStream has it loaded
    - 
  - This will be added to sync params once finished
  - The lastUpdatedUTC field in the metadata 
  - the lastEffective date in the metadata is the last date the data is available for
  - I can store data in the parameters of this class to record prior updates. 
  - This class should determine when it will next be called based upon its assessment of the update frequency.
  - an api call will be made to the NRGStream API to get the metadata for the stream
  - core data will be loaded and stored using pymongo

  An example of the stream metadtata is below:
  {'timeGenerated': '2024-04-21T21:11:45.5100134Z',
 'streamName': 'PWRStream - ERCOT LZ_WEST Settle Forecast Price 10am',
 'streamId': '352722',
 'streamDescription': 'PWRStream - ERCOT LZ_WEST Settle Forecast Price',
 'timezone': 'CPT',
 'dataInterval': '15',
 'fromDate': '04-01-2024',
 'toDate': '04-20-2024',
 'recordCount': '1824',
 'userMessage': None,
 'columns': [{'name': 'Date/Time', 'unitOfMeasure': ''},
  {'name': 'Price', 'unitOfMeasure': 'USD/MWh'}],
 'last_effective': '2024-04-23T00:00:00',
 'displayId': '0',
 'dataOption': None,
 'lastUpdatedUTC': '2024-04-21T19:57:58.393Z',
 'useUserTimeZone': False,
 'userTimeZone': 'MPT'}

"""
