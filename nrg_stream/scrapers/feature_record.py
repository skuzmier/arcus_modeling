"""
Feature records are used to track individual data streams.
If these are from NRGStream they are unique on stream_id, perspective, and data_option


create a feature record from a stream record
            Defaults to the perspective and data_option of the stream record.
            If a record already exists for the stream_id, perspective, and data_option
            it will be loaded unless replace is True

            To create a new record with a different perspective or data_option
            set the perspective and data_option kwargs. This will not overwrit
"""

import os
import sys
import bson
import datetime

import re
from psycopg2 import sql
import pandas as pd

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))
sys.path.append(os.path.expanduser("~/ercot_virts"))


from utils import Utils
from nrg_stream_record import StreamRecord
from helpers import Helpers
from data_validation import DataValidation


class FeatureRecord:
    """
    """

    collection = "feature_records"
    u = Utils()
    object_type = "feature_record"

    def __init__(self, id=None, **kwargs):
        """
        """
        self._id = id
        self.kwargs = kwargs
        self.stream_id = kwargs.get("stream_id", None)
        self.data_option = kwargs.get("data_option", None)
        self.description = kwargs.get("description", None)
        self.iso = kwargs.get("iso", None)
        self.pg_id = kwargs.get("pg_id", None) #is of postgres record
        self.perspective = kwargs.get("perspective", None)
        self.stream_name = kwargs.get("stream_name", None)
        self.source_tz = kwargs.get("source_tz", None)
        self._table = kwargs.get("table", None)
        self.freq = None
        self.notes = None
        self.created_at = None
        self.updated_at = None
        self.scraper = None
        self.sync = False #sync with NRGStream
        self._first_date = None
        self._last_date = None
        self.setup_mode = False
        self.data_validation_id = None
        self._data_validation = None
        self.related_streams = []
        self.missing_data_defaults = MissingDataDefaults(**kwargs)
        self.sync_params = None
        self._stream_record = None
        self.debug = kwargs.get("debug", False)
        self._setup()

    @classmethod
    def get_records(cls, **kwargs):
        """Enables full mongo search with kwargs
           for equivilency pass standard as args,
           for full search write pymongo query as a dict and pass in as kwargs
        """
        cls.u.mongo.collection = cls.collection
        if len(kwargs) == 0:
            raise ValueError("must provide search params")
        res = cls.u.m_conn.find(kwargs)
        res = [x for x in res] # unpack cursor
        if len(res) == 0:
            raise ValueError("no records found")
        res =[cls(**x) for x in res] #create objects from results
        if len(res) == 1:
            return res[0]
        return res
    
    @classmethod
    def search(cls, **kwargs):
        """search for records in mongo"""

    @property
    def id(self):
        """ """
        return self._id

    @property
    def stream_record(self):
        """only load stream record when needed to avoid unnecessary db calls 
        """
        if self._stream_record is not None:
            return self._stream_record
        if self.stream_id is None:
            return None
        self._stream_record = StreamRecord(stream_id=self.stream_id)
        return self._stream_record

    @property
    def first_date(self):
        """first date downloaded from the stream"""
        return self._first_date

    @first_date.setter
    def first_date(self, value):
        """ """
        value = self.u.time_converter(value, target="dt")
        if self.setup_mode:
            self._first_date = value
            return None
        if not isinstance(value, datetime.datetime):
            value = self.u.time_converter(value, target="dt")
        if self._first_date is None or value < self._first_date:
            self._first_date = value
            self._update_attrs({"first_date": self._first_date})
        else:
            pass
            # raise ValueError(
            #    f"new first_date must be earlier than current first_date: {self._first_date}"
            # )

    @property
    def last_date(self):
        """last date downloaded from the stream"""
        return self._last_date

    @last_date.setter
    def last_date(self, value):
        """update last date if value is later than current last date"""
        value = self.u.time_converter(value, target="dt")
        if self.setup_mode:
            self._last_date = value
            return None
        if not isinstance(value, datetime.datetime):
            value = self.u.time_converter(value, target="dt")
        if self._last_date is None or value > self._last_date:
            self._last_date = value
            self._update_attrs({"last_date": self._last_date})
        else:
            pass 
            # raise ValueError(
            #    f"new last_date must be later than current last_date: {self._last_date}"
            # )

    @property
    def table(self):
        """ """
        return self._table

    @table.setter
    def table(self, value):
        """update record to new table for downlaod results.
            This prevents the pg record and mongo record from getting out of sync
            
            Args:
            -----
                value: str
        """
        if not isinstance(value, str):
            raise ValueError("table must be a string")
        if self.setup_mode:
            self._table = value
            return None
        if value == self._table:
            return None
        self._table = value
        if self.pg_id is not None:
            self.u.update_record(table="features", data={"table": self._table}, id=self.pg_id)
        if self._id is not None:
            self._update_attrs({"table": self._table})
        
    @property
    def data_validation(self):
        """ """
        if self.data_validation_id is None:
            return None
        if self._data_validation is not None:
            return self._data_validation
        self._data_validation = DataValidation(id=self.data_validation_id)
        return self._data_validation
    
    @property
    def freq_minutes(self):
        """
        """
        if self.freq is None:
            return None
        m = re.search(r'\d+', self.freq)
        if m:
            return int(m.group())
        return None

    def _setup(self):
        """
        """
        self.u.mongo.collection = self.collection
        if 'stream_record' in self.kwargs:
            self._from_stream_record(self.kwargs['stream_record'])
        elif len(self.kwargs) > 2:
            self._from_dict(self.kwargs)
        elif self._id is not None:
            self._from_id(self._id)

    def _from_stream_record(self, replace=False):
        """create a feature record from a stream record
            Defaults to the perspective and data_option of the stream record.
            If a record already exists for the stream_id, perspective, and data_option
            it will be loaded unless replace is True

            To create a new record with a different perspective or data_option
            set the perspective and data_option kwargs. This will not overwrite an existing record

        """
        stream_record = self.kwargs['stream_record']
        data_option = self.kwargs.get("data_option", None)
        perspective = self.kwargs.get("perspective", stream_record.perspective)
        if not isinstance(stream_record, StreamRecord):
            raise ValueError("stream_record must be a StreamRecord object")
        existing = self._get_existing_id(stream_record.stream_id, perspective, data_option)
        if existing is not None:
            self._from_id(existing)
            if replace:
                return None
        self.stream_id = stream_record.stream_id
        self.stream_name = stream_record.stream_name
        self.iso = stream_record.iso
        self.source_tz = stream_record.stream_tz
        self.freq = stream_record.freq
        self.perspective = perspective
        self.data_option = data_option
        self._get_table_name()
        self._get_sync_params()
        self.missing_data_defaults = MissingDataDefaults(**self.kwargs)

    def _get_existing_id(self, stream_id, perspective, data_option):
        """check if a record exists for a given stream_id, perspective, and data_option
           returns the id if it exists
        """
        res = self.u.m_conn.find_one(
            {"stream_id": stream_id, "perspective": perspective, "data_option": data_option}
        )
        if res is not None:
            return res["_id"]
        return None

    def _update_attrs(self, attrs) -> None:
        """update attr in db

        Args:
        -----
            attrs: dict
        """
        if not isinstance(attrs, dict):
            raise ValueError("attrs must be a dict")
        attrs["updated_at"] = datetime.datetime.now()
        self.u.m_conn.update_one({"_id": self._id}, {"$set": attrs})

    def _get_iso_id(self, name) -> int:
        """get the iso_id for the feature record"""
        res = self.u.execute_query(
            "select id, name from iso", dict_cursor=True, fetch="all"
        )
        res = {x["name"]: x["id"] for x in res}
        if name not in res:
            raise ValueError(f"iso {name} not found")
        return res[name]

    def _get_table_name(self) -> str:
        """get table name for the feature record
        """
        if self.table is not None:
            return self.table
        iso = self.iso.lower()
        if self.perspective == "forecast":
            self.table = f"{iso}_fcst"
        else:
            self.table = f"{iso}_feature"

    def _create_pg_record(self):
        """save the record to postgres, this is seperate than the main record which lives in mongo
        """
        if self.id is None:
            self._id = bson.ObjectId()
        if self.pg_id is not None:
            return None #record already exists
        iso_id = self._get_iso_id(self.iso)
        pg_record = {'iso_id': iso_id,
                     'mongo_id': str(self.id),
                     'stream_id': self.stream_id,
                     'table_name': self.table,
                     'data_option': self.data_option,
                     'perspective': self.perspective}
        self.pg_id = self.u.insert_dict_record(table="features", record_dict=pg_record, return_id=True)

    def _from_id(self, id):
        """setup from an id, either the mongo id or the postgres id
        """
        if isinstance(id, str) and len(self._id) == 24:
            id = bson.ObjectId(self._id)
        if isinstance(id, bson.ObjectId):
            res = self.u.m_conn.find_one({"_id": id})
        else:
            try:
                id = int(id)
            except:
                raise ValueError(f"invalid id: {id}")
            res = self.u.m_conn.find_one({"pg_id": id})
        if res is None:
            raise ValueError(f"no record found for id: {id}")
        self._from_dict(res)

    def _from_dict(self, config_dict):
        """
        """
        self.setup_mode = True
        for key, value in config_dict.items():
            if key in ["first_date", "last_date"]:
                value = self.u.time_converter(value, target="dt")
            if key in ['sync_params']:
                value = SyncParams(**value)
            if key in ['missing_data_defaults']:
                value = MissingDataDefaults(**value)
            setattr(self, key, value)
        self.setup_mode = False

    def save(self):
        """
        """
        if self.pg_id is None:
            self._create_pg_record()
            record = self.to_dict(include_id=True)
            self.u.m_conn.insert_one(record)
        else:
            record = self.to_dict()
            self.u.m_conn.update_one({"_id": self._id}, {"$set": record})

    def to_dict(self, str_repr=False, include_id=False):
        """convert record to dict
        """
        skip_args = [
            "kwargs",
            "u",
            "_id",
            "collection",
            "setup_mode",
            "stream_record",
            "missing_data_defaults",
            "sync_params",
            "debug",
        ]
        res = {}
        for k, v in self.__dict__.items():
            if k not in skip_args:
                if str_repr and isinstance(v, datetime.datetime):
                    v = self.u.time_converter(v, target="str")
                else:
                    res[k] = v
            if k in ['sync_params', 'missing_data_defaults']:
                res[k] = v.to_dict(str_repr=str_repr)
        if include_id:
            res["_id"] = self._id
        return res

    def _get_sync_params(self) -> dict:
        """
        """
        kwargs = self.kwargs
        stream_record = kwargs['stream_record']
        if 'sync_params' in kwargs:
            kwargs.update(kwargs['sync_params'])
        tgt_first_date = max(
            datetime.datetime(2015, 1, 1), stream_record.available_from
        )
        kwargs['sync_active'] = stream_record.active
        if 'tgt_first_date' not in kwargs:
            kwargs['tgt_first_date'] = tgt_first_date
        if 'sync_every' not in kwargs:
            kwargs["sync_every"] = self._get_sync_freq()
        if 'allow_sync' not in kwargs:
            kwargs['allow_sync'] = stream_record.active
        self.sync_params = SyncParams(**kwargs)

    def _get_sync_freq(self, **kwargs):
        """
        """
        if 'sync_every' in kwargs:
            return kwargs['sync_every']
        # regex integer extraction
        m = re.search(r'\d+', self.freq)
        if m:
            return int(m.group())
        return 60
    
    def get_data(self, s_date=None, e_date=None):
        """Get data from postgres database
            DO NOT USE for primary data access. This is for debugging and exploration only
            FIXME: This will not work for foreacst data
            FIXME: This doesn't account for input tz
        """
        if s_date is None:
            s_date = self.first_date
        if e_date is None:
            e_date = self.last_date
        if self.pg_id is None:
            raise ValueError("no pg_id found")
        query = """ SELECT 
                    dt at time zone {source_tz} as ldt,
                    value 
                    FROM {table} 
                    WHERE dt >= {s_date} 
                    AND dt <= {e_date}
                    AND feature_id = {pg_id} """
        query = sql.SQL(query).format(
            source_tz=sql.Literal(self.source_tz),
            table=sql.Identifier(self.table),
            s_date=sql.Literal(s_date),
            e_date=sql.Literal(e_date),
            pg_id=sql.Literal(self.pg_id)
        )
        res = self.u.execute_query(query, dict_cursor=True, fetch="all")
        return pd.DataFrame(res) 


class SyncParams:
    """params for syncing feature records with NRGStream
    """
    u = Utils()

    def __init__(self, **kwargs):
        """
        """
        if kwargs is None:
            kwargs = {}
        if 'sync_params' in kwargs:
            kwargs,update(kwargs['sync_params'])
        self.tgt_first_date = kwargs.get("tgt_first_date", None)
        self.sync_active = kwargs.get("sync_active", True) #Should sync after catchup
        self.sync_every = kwargs.get("sync_every", 60) #minutes
        self.priority = kwargs.get("priority", 2) #0 is highest priority
        self.last_job_status = kwargs.get("last_job_status", None)
        self.last_sync = kwargs.get("last_sync", datetime.datetime(1970, 1, 1))
        self.last_error = kwargs.get("last_error", datetime.datetime(1970,1,1))
        self.next_sync = kwargs.get("next_sync", datetime.datetime(2020, 1, 1))
        self.nrg_last_updated_utc = kwargs.get("nrg_last_updated_utc", datetime.datetime(1970, 1, 1))
        self.pending_sync = kwargs.get("pending_sync", False) #sync is scheduled, but not completed
        self.catchup_complete = kwargs.get("catchup_complete", False)
        self.days_ahead = kwargs.get("days_ahead", 0)
        self.stream_update_freq = kwargs.get("stream_update_freq", None)
        self.estimation_complete = kwargs.get("estimation_complete", False)
        # self.allow_catchup = kwargs.get("allow_catchup", True)
        # self.catchup_complete = kwargs.get("catchup_complete", False)
        # self.catchup_active = kwargs.get("catchup_active", False)

    def to_dict(self, str_repr=False):
        """
        """
        res = {}
        for (k, v) in self.__dict__.items():
            if str_repr and isinstance(v, datetime.datetime):
                v = str(v)
            res[k] = v
        return res

    def update_from_sync(self, api_metadata=None, has_error=False):
        """
        """
        self.pending_sync = False
        self.last_sync = datetime.datetime.now(datetime.timezone.utc)
        if has_error:
            self.last_error = datetime.datetime.now(datetime.timezone.utc)
            self.last_job_status = 'error'
            self.last_sync = datetime.datetime.now(datetime.timezone.utc)
            return None
        if self.has_new_data(api_metadata):
            self.nrg_last_updated_utc = self.u.time_converter(
                api_metadata["lastUpdatedUTC"], target="dt"
            )
        self._get_next_sync()
        self.last_job_status = 'success'

    def has_new_data(self, api_metadata) -> bool:
        """ """
        if (
            self.u.time_converter(api_metadata["lastUpdatedUTC"], target="dt").replace(
                tzinfo=None
            )
            > self.nrg_last_updated_utc
        ):
            return True
        return False

    def _get_next_sync(self):
        """add sync_every minutes to current timestamp utc
        """
        self.next_sync = datetime.datetime.now(
            datetime.timezone.utc
        ) + datetime.timedelta(minutes=self.sync_every)


class MissingDataDefaults:
    """default behavior for handing missing data. This is used at extraction time
    """
    def __init__(self, **kwargs):
        """
        """
        if 'missing_data_defaults' in kwargs:
            kwargs.update(kwargs['missing_data_defaults'])
        self.fill_method = kwargs.get("fill_method", "ffill")
        self.max_periods = kwargs.get("max_periods", 4)
        self.default_value = kwargs.get("default_value", None)
    
    def to_dict(self, **kwargs):
        """
        """
        res = {}
        for (k, v) in self.__dict__.items():
            res[k] = v
        return res


# from nrg_stream import NRGStream
# api = NRGStream()
# def get_stream_info(api, feature_record):
#     """
#     """
#     stream_id = feature_record.stream_id
#     api.get_stream(s_date='04-01-2024', e_date='04-20-2024', stream_id=stream_id)
#     meta_data = api.meta
#     return meta_data

# records = []
# cnt = 0
# for fr in feature_records:
#     meta_data = get_stream_info(api, fr)
#     records.append(meta_data)
#     cnt += 1
#     if cnt % 5 == 0:
#         print(f"{cnt} records processed")

