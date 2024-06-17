import sys
import os
import datetime
import warnings

import re

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))


from utils import Utils
from nrg_stream import NRGStream
from nrg_stream_record import StreamRecord
from feature_record import FeatureRecord
from update_schedule_inference import UpdateScheduleInference


class DataStreamManager:
    """ """

    def __init__(self, **kwargs):
        """
        Args:
        ------
        stream_data: list, dict
            required dict keys: stream_id, common_name
        """
        self.u = Utils()
        self.nrg_stream_list = []
        self.stream_data = []
        self.tgt_streams = []
        self.stream_records = []
        self.feature_records = []
        self.sync_inference_records = []
        self.kwargs = kwargs
        self.existing_streams = []  # list of streams with nrg_stream_records
        self.api = NRGStream(username="spencer", password="nrg4spencer")
        self.stream_list = []  # list of target stream base records from api
        self.stream_descriptions = []  # combined metadata and data
        self.debug = kwargs.get("debug", False)
        self.tgt_stream_ids = []
        self.combined_stream_data = []
        self._setup()

    def _setup(self):
        """ """
        self.u.mongo.collection = "nrg_stream_records"
        for k, v in self.kwargs.items():
            setattr(self, k, v)
        self._get_existing_streams()
        self.nrg_stream_list = self.api.get_stream_list()

    def get_stream_records(self, stream_data):
        """gets stream records for new streams,
            this creates the stream record or retrieves it if it already exists
            doesn't save the record

            Args:
            -----
            stream_data: list, dict
        """
        if not isinstance(stream_data, list):
            stream_data = [stream_data]
        self.stream_data = stream_data
        self.tgt_stream_ids = [x["stream_id"] for x in self.stream_data]
        self._get_target_streams()
        self._validate_stream_data()
        for stream_dict in self.stream_data:
            stream_id = stream_dict["stream_id"]
            if stream_id in self.existing_streams:
                self.stream_records.append(StreamRecord(stream_id=stream_id, stream_name=stream_dict["stream_name"]))
            else:
                self._get_stream_info(stream_data=stream_dict)
        for rec in self.combined_stream_data:
            stream_record = StreamRecord(stream_data=rec)
            self.stream_records.append(stream_record)

    def get_single_stream_record(self, stream_id, common_name, **kwargs):
        """
        """
        stream_data = {"stream_id": stream_id, "common_name": common_name, "stream_name": common_name}
        stream_data.update(kwargs)
        self.create_new_stream_records(stream_data)

    def _get_existing_streams(self):
        """get existing basic stream records"""
        self.existing_streams = [x["stream_id"] for x in self.u.m_conn.find()]

    def _validate_stream_data(self):
        """confirm stream_data is a list of dicts with required keys"""
        for rec in self.stream_data:
            if not isinstance(rec, dict):
                raise ValueError("stream_data must be a list of dicts")
            for val in ["stream_id", "stream_name"]:
                if val not in rec:
                    raise ValueError(
                        f"{val} is required, only found keys: {rec.keys()}"
                    )

    def _get_target_streams(self):
        """get target dataset"""
        self.stream_list = [
            x for x in self.nrg_stream_list if int(x["streamId"]) in self.tgt_stream_ids
        ]

    def _get_stream_info(self, stream_data):
        """some data is only availble when scraped, get it and combine with metadata"""
        # get a small ammount data so metadata is loaded
        stream_id = stream_data["stream_id"]
        self.api.get_stream(
            stream_id=int(stream_id),
            s_date="01-01-2024",
            e_date="01-03-2024",
            data_format="json",
        )
        meta_data = self.api.meta
        base_stream_data = [
            x for x in self.stream_list if int(x["streamId"]) == stream_id
        ][0]
        combined_data = {**meta_data, **base_stream_data, **stream_data}
        self.combined_stream_data.append(combined_data)
    
    def track_stream(self, stream_id, common_name, data_option=None, scraper='nrg_feature'):
        """
        """
        #clear out any existing records
        self.stream_records = []
        self.feature_records = []
        stream_data = {"stream_id": stream_id, "common_name": common_name, 
                       "stream_name": common_name, "data_option": data_option}
        self.get_stream_records(stream_data)
        self.save(stream_records=True, feature_records=False)
        self.get_feature_records(scraper=scraper)
        self.save(stream_records=False, feature_records=True)
        #self.get_sync_inference_records()

    def get_feature_records(self, scraper, stream_ids=None, **kwargs):
        """create new feature records for a stream record

            Args:
            -----
            scraper: str
                scraper name
            stream_ids: int, list, None
                stream ids to create feature records for
                if None, create feature records for all members of self.stream_records
            kwargs: dict
        """
        if stream_ids is None:
            stream_ids = [x.stream_id for x in self.stream_records]
        if isinstance(stream_ids, int):
            stream_ids = [stream_ids]
        stream_records = [x for x in self.stream_records if x.stream_id in stream_ids]
        if len(stream_records) == 0:
            raise ValueError("no stream records found")
        if len(stream_records) != len(stream_ids):
            raise ValueError("some stream ids not found in stream records")
        for stream_record in stream_records:
            if stream_record.id is None:
                 raise ValueError(f"stream record {stream_record.stream_id} not saved")
            feature_record = FeatureRecord(stream_record=stream_record, scraper=scraper, **kwargs)
            self.feature_records.append(feature_record)
    
    def get_sync_inference_records(self):
        """ """
        for feature_record in self.feature_records:
            inference_record = UpdateScheduleInference(feature_id=feature_record.id)
            if inference_record.id is None:
                inference_record.save() 
            self.sync_inference_records.append(inference_record)

    def save(self, stream_records=True, feature_records=True):
        """ 
        """
        if stream_records:
            for record in self.stream_records:
                record.save()
        if feature_records:
            for record in self.feature_records:
                record.save()
        return True
    
    def run(self, scraper, stream_data=None):
        """ 
        """
        if len(self.stream_data) > 0 and stream_data is None:
            stream_data = self.stream_data
        elif stream_data is None:
            raise ValueError("no stream data provided")
        self.get_stream_records(stream_data)
        self.save(stream_records=True, feature_records=False)
        self.get_feature_records(scraper=scraper)
        self.save(stream_records=False, feature_records=True)
        self.get_sync_inference_records()
        return True

