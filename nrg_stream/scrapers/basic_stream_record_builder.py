"""build new basic stream records from a stream_id or list of stream_ids
"""
import sys
import os
import datetime
import warnings

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream"))
sys.path.append(os.path.expanduser("~/ercot_virts/nrg_stream/scrapers"))

from utils import Utils
from basic_stream_record import BasicStreamRecord
from nrg_stream import NRGStream
from nrg_stream_record import StreamRecord


class BasicStreamRecordBuilder:
    """ """

    def __init__(self, stream_data, **kwargs):
        """
        Args:
        ------
        stream_data: list, dict
            required dict keys: stream_id, common_name
        """
        self.u = Utils()
        self.stream_data = stream_data
        self.basic_stream_records = []
        self.kwargs = kwargs
        self.existing_streams = []  # list of streams with basic records
        self.api = NRGStream(username="spencer", password="nrg4spencer")
        self.stream_list = []  # list of target stream base records from api
        self.stream_descriptions = []  # combined metadata and data
        self.debug = kwargs.get("debug", False)
        self.tgt_stream_ids = []
        self._setup()

    def _setup(self):
        """ """
        self.u.mongo.collection = "basic_stream_records"
        if not isinstance(self.stream_data, list):
            self.stream_data = [self.stream_data]
        for k, v in self.kwargs.items():
            setattr(self, k, v)
        self._validate_stream_data()
        self._get_existing_streams()
        self.tgt_stream_ids = [x["stream_id"] for x in self.stream_data]

    def _validate_stream_data(self):
        """confirm stream_data is a list of dicts with required keys
        """
        for rec in self.stream_data:
            if not isinstance(rec, dict):
                raise ValueError("stream_data must be a list of dicts")
            for val in ["stream_id", "common_name"]:
                if val not in rec:
                    raise ValueError(f"{val} is required, only found keys: {rec.keys()}")

    def run(self):
        """ """
        self._get_target_streams()
        self._get_new_basic_stream_records()
        if not self.debug:
            self._save_records()

    def _get_target_streams(self):
        """get target dataset"""
        stream_list = self.api.get_stream_list()
        self.stream_list = [
            x for x in stream_list if int(x["streamId"]) in self.tgt_stream_ids
        ]

    def _get_stream_data(self, stream_data):
        """some data is only availble when scraped, get it and combine with metadata"""
        # get a small ammount data so metadata is loaded
        stream_id = stream_data["stream_id"]
        self.api.get_stream(
            stream_id=int(stream_id), s_date="01-01-2024", e_date="01-03-2024", data_format="json"
        )
        meta_data = self.api.meta
        base_stream_data = [
            x for x in self.stream_list if int(x["streamId"]) == stream_id
        ][0]
        combined_data = {**meta_data, **base_stream_data, **stream_data}
        self.stream_descriptions.append(combined_data)

    def _get_existing_streams(self):
        """get existing basic stream records"""
        self.existing_streams = [x["stream_id"] for x in self.u.m_conn.find()]

    def _get_new_basic_stream_records(self):
        """
        """
        for d in self.stream_data:
            stream_id = d["stream_id"]
            if stream_id in self.existing_streams:
                warnings.warn(f"{stream_id} already exists in basic_stream_records")
                continue
            self._get_stream_data(stream_data=d)
        for stream_description in self.stream_descriptions:
            self.basic_stream_records.append(BasicStreamRecord(stream_id=stream_description['stream_id'],
                                                               common_name=stream_description['common_name'],
                                                               stream_data=stream_description))
        self.basic_stream_records

    def _save_records(self):
        """
        """
        for record in self.basic_stream_records:
            record.save()
        print("records saved")
        return True




class DataStreamManager:
    """
    """

    def __init__(self):
        """
        """


""" Objectives for this class:
Get a list of tracked streams
Create new stream records for a batch
    create a new feature record for each stream using the data params
Create a new streram record 


"""
