"""
"""
import os
import sys
import bson
import datetime

import re

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))

from utils import Utils
from helpers import Helpers


class StreamRecord:
    """ """

    collection = "nrg_stream_records"
    u = Utils()
    object_type = "nrg_stream_record"

    def __init__(self, id=None, stream_id=None, stream_data=None, **kwargs):
        """ """
        self._id = id
        self.stream_data = stream_data
        self.kwargs = kwargs
        self.stream_id = stream_id
        self.iso = None
        self.iso_id = None
        self._perspective = None
        self.has_forecast_ts = self.kwargs.get("has_forecast_ts", False) # has multiple predictions for single timestamp
        self.available_from = None
        self.updated_at = None
        self.active = True
        self.freq = None
        self.missing_dates = []
        self.feature_ids = []
        self.data_options = []
        self.setup_mode = False
        self.stream_tz = None
        self.default_val = self.kwargs.get("default_val", None)
        self._setup()
        self.stream_name = self.kwargs.get("stream_name", None) # Not sure why this needs to go after setup but it does. 

    def __repr__(self):
        """ """
        return f"StreamRecord(id={self._id}, stream_id={self.stream_id}, stream_name={self.stream_name})"

    @classmethod
    def get_all(cls, iso=None):
        """get all objects of this type"""
        cls.u.mongo.collection = cls.collection
        query = {"object_type": cls.object_type}
        if iso is not None:
            query["iso"] = iso
        res = cls.u.m_conn.find(query)
        return [cls(**x) for x in res]

    def get_all_ids(cls, iso=None):
        """get the id of for all objects with object_type = cls.object_type"""
        cls.u.mongo.collection = cls.collection
        query = {"object_type": cls.object_type}
        if iso is not None:
            query["iso"] = iso
        res = cls.u.m_conn.find(query)
        return [x["_id"] for x in res]

    @property
    def id(self):
        """ """
        return self._id

    @property
    def perspective(self):
        """ """
        return self.__perspective

    @perspective.setter
    def perspective(self, value):
        """only allow perspective to take on certain values"""
        if self.setup_mode:
            self.__perspective = value
            return None
        if value not in [
            None,
            "forecast",
            "actual",
            "trade_forecast",
            "final_forecast",
        ]:
            raise ValueError(
                f"perspective must be one of [None, 'forecast', 'actual', 'trade_forecast', 'final_forecast']"
            )
        self.__perspective = value

    def _setup(self):
        """ """
        self.u.mongo.collection = self.collection
        if len(self.kwargs) > 2:
            self._from_dict(config_dict=self.kwargs)
        elif self._id is not None:
            self._from_id()
        elif self.stream_data is not None:
            self._from_stream_data()
        elif self.stream_id is not None:
            self._from_stream_id()
        else:
            raise ValueError("id or stream_id, or stream_data is required")
        if self.stream_id is None:
            raise ValueError("stream_id is required")
        self.stream_id = int(self.stream_id)

    def _from_stream_data(self):
        """build record from stream data provided py Arcus"""
        helpers = Helpers()
        self.stream_id = self.stream_data["streamId"]
        self.freq = str(int(self.stream_data["dataInterval"])) + "min"
        self.iso = helpers.get_market_common_name(self.stream_data["regionTypeCode"])
        self.available_from = datetime.datetime.strptime(
            self.stream_data["availFromDate"], "%b-%d-%Y %H:%M:%S"
        )
        self.scraper = "base_stream"
        self.active = self._get_is_active()
        self.iso_id = self._get_iso_id(name=self.iso)
        self._get_tz()
        self._get_perspective()

    def _get_is_active(self):
        """check if the NRGStream data has been updated in the last 7 days
            Note: This is only run at object creation
        """
        last_update = self.u.time_converter(
            self.stream_data["lastUpdatedUTC"], target="dt"
        )
        # strip tz info
        last_update = last_update.replace(tzinfo=None)
        if (datetime.datetime.now() - last_update).days > 7:
            return False
        else:
            return True

    def _get_tz(self) -> None:
        """get the native timezone of the stream
           convert NRGStream timezone format to valid database timezone
        """
        tz_mappings = {
            "MPT": "US/Mountain",
            "CPT": "US/Central",
            "EPT": "US/Eastern",
            "PPT": "US/Pacific",
            'GMT': 'UTC',
        }
        self.stream_tz =  tz_mappings[self.stream_data["dataTimeZone"]]

    def _get_perspective(self) -> None:
        """
        """
        # check if stream has future data
        if self.stream_data['marketTypeCode'] == 'DAM' and 'forecast' not in self.stream_data['streamName'].lower():
            self.perspective = 'actual'
        elif self.u.time_converter(self.stream_data['latestData'], 'dt') > datetime.datetime.now():
            self.perspective = 'final_forecast'
        # check for 9am or 10am in stream name
        pattern = r"\b(?:9|10)\s*\.?\s*a\.?m\.?\b"
        if re.search(pattern, self.stream_data['streamName']):
            self.perspective = 'trade_forecast'
        # otherwise, assume actual data
        else:
            self.perspective = 'actual'

    def _from_stream_id(self):
        """ """
        if self.stream_id is None:
            raise ValueError("stream_id is None")
        self.stream_id = int(self.stream_id)
        res = self.u.m_conn.find_one({"stream_id": self.stream_id})
        if res is None or len(res) == 0:
            # allow for new object creation with stream_id defined
            if len(self.kwargs) > 2:
                self._from_dict()
            else:
                raise ValueError(f"no record found for stream_id: {self.stream_id}")
        # remove this later
        if 'object_type' not in res:
            res['object_type'] = self.object_type
            self.u.m_conn.update_one({"_id": res["_id"]}, {"$set": {"object_type": self.object_type}})
        if res["object_type"] != self.object_type:
            raise ValueError(
                f"record with stream_id: {self.stream_id} is not a {self.object_type}"
            )
        self.setup_mode = True
        for k, v in res.items():
            setattr(self, k, v)
        self.setup_mode = False

    def _from_id(self):
        """ """
        if self._id is None:
            raise ValueError("id is None")
        if isinstance(self._id, str):
            self._id = bson.ObjectId(self._id)
        res = self.u.m_conn.find_one({"_id": self._id})
        if res is None or len(res) == 0:
            raise ValueError(
                f"no record found for id: {self._id}, are you in the right collection?"
            )
        if res["object_type"] != self.object_type:  # catch object type mismatch
            raise ValueError(f"record with id: {self._id} is not a {self.object_type}")
        self._from_dict(config_dict=res)

    def _check_for_existing(self):
        """check to see if a stream record with this id already exists"""
        res = self.u.m_conn.find_one({"stream_id": self.stream_id})
        if res is not None:
            # set attributes from existing record
            for key, value in res.items():
                setattr(self, key, value)
            return True
        else:
            return False

    def to_dict(self):
        """convert to a dict

        Returns:
        --------
            dict
        """
        skip_args = [
            "kwargs",
            "u",
            "collection",
            "db_info",
            "_id",
            "setup_mode",
            "create_mode",
        ]
        res = {}
        for k, v in self.__dict__.items():
            if k not in skip_args:
                res[k] = v
        res['object_type'] = self.object_type
        return res

    def _from_dict(self, config_dict=None):
        """convert from a dict

        Args:
        -----
            config_dict: dict
        """
        # turn on setup mode to auto save to db
        self.setup_mode = True
        for k, v in config_dict.items():
            setattr(self, k, v)
        self.setup_mode = False

    def save(self):
        """save to db"""
        # self._setup()
        if self._id is None:
            already_exists = self._check_for_existing()
            if already_exists:
                print("record already exists, skipping")
            else:
                self._id = self.u.m_conn.insert_one(self.to_dict()).inserted_id
        else:
            self.u.m_conn.update_one({"_id": self._id}, {"$set": self.to_dict()})

    def _get_iso_id(self, name):
        """ """
        if self.iso_id is not None:
            return self.iso_id
        res = self.u.execute_query(
            "select id, name from iso", dict_cursor=True, fetch="all"
        )
        res = {x["name"]: x["id"] for x in res}
        if name not in res:
            raise ValueError(f"iso {name} not found")
        self.iso_id = res[name]
        return self.iso_id

    def _update_attrs(self, attrs) -> None:
        """update attr in db

            Args:
            -----
                attrs: dict
        """
        if not isinstance(attrs, dict):
            raise ValueError("attrs must be a dict")
        attrs['updated_at'] = datetime.datetime.now()
        self.u.m_conn.update_one({"_id": self._id}, {"$set": attrs})
