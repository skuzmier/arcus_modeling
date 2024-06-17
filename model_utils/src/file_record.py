"""base class for data cacheing record
"""

import sys
import bson
import os
import shutil
import boto3

import pandas as pd
import xarray as xr
import datetime
import hashlib

sys.path.append(os.path.expanduser("~/ercot_virts/utils"))

from utils import Utils
from record import Record


class FileRecord(Record):
    """Base class to handle s3 file storage, retrival, anc local caching 
    """

    u = Utils()
    bucket = "cosine-ercot-cache"
    object_type = "file_record"

    def __init__(self, id=None, path=None, **kwargs):
        """ """
        super().__init__(id, object_type='file_record', **kwargs)
        self.kwargs = kwargs
        self._path = path
        self.dates = None
        self.location = None
        self.cache_dir = kwargs.get("working_dir", None)
        self.human_name = self.kwargs.get("human_name", None) # human readable name for the file
        self.file_type = kwargs.get("file_type", None)
        self.source = None
        self.key = None
        self.parent_id = None  # uuid of parent record, if any
        self.base_name = None
        self.extension = None
        self.system = None
        self.is_folder = False # flag for folder cacheing, not implemented
        self.ignore_params = [
            "_id",
            "kwargs",
            "u",
            "path",
            "cache_dir",
            "cache_info",
            "setup_mode",
        ]
        self.setup_mode = False
        self.md5_hash = None
        self.expires_at = None
        self._setup()

    @property
    def id(self):
        """ """
        return self._id

    @property
    def path(self):
        """return path or download data if not set
        """
        if self._path is None:
            self._get_data()
        return self._path

    @path.setter
    def path(self, path):
        """
        """
        if self.setup_mode:
            self._path = path
        elif self._path is not None:
            raise ValueError("path already set")
        else:
            self._path = path
            self._from_path()

    def _setup(self):
        """ """
        self.u.mongo.collection = "file_records"
        self.setup_mode = True # allow setting of locked attributes
        if self._id is not None:
            self._from_id()
        elif self._path is not None:
            self._from_path()
        self._get_cache_dir()
        self.setup_mode = False

    def save(self, replace=False):
        """save record to mongo and upload, or update keys if record exists

        Args:
        -----
            replace: bool
                relace the file on s3. if overwrite if False existing file will be versioned
        """
        if replace:
            raise NotImplementedError("replace not implemented")
        if self.key is None:
            self._upload_object()
        if self._id is None:
            if self.key is None:
                raise ValueError("key not set")
            self._id = self.u.m_conn.insert_one(self.to_dict()).inserted_id
        else:
            if self.key is None:
                raise ValueError("key not set")
            self.u.m_conn.update_one({"_id": self._id}, {"$set": self.to_dict()})

    def replace_file(self, target_file, overwrite=False):
        """replace a cache on s3

        Args:
        -----
            overwrite: bool
                if true, overwrite existing file and do not update version number

        """
        if not os.path.isfile(target_file):
            raise ValueError(f"invalid file: {target_file}")
        self._update_params(target_file)
        self._replace_cache()
        print("cache replaced")

    def _update_params(self, target_file):
        """ """
        raise ValueError("Must be implemented in parent class")

    def to_dict(self):
        """convert to a dictionary"""
        res = {}
        for key, val in self.__dict__.items():
            if key.startswith("_") or key in self.ignore_params:
                continue
            else:
                res[key] = val
        if self._id is not None:
            res["_id"] = self._id
        return res

    def add_keys(self, **kwargs):
        """add a new key to the object and save it to mongodb

        Args:
        -----
            kwargs: dict  {key: value}
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
        if self._id is None:
            return True
        query = {"_id": self._id}
        update = {"$set": kwargs}
        self.u.m_conn.update_one(query, update)

    def _from_path(self):
        """setup from a path to the cache"""
        if self.path is None:
            raise ValueError("path not set")
        self.md5_hash = self._get_md5_hash(self.path)
        self.base_name = os.path.basename(self.path)
        self.extension = self.base_name.split(".")[-1]

    def _set_kwargs(self):
        """set kwarg values for any existing attribute"""
        for key, value in self.kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def _get_md5_hash(self, path):
        """get the md5 has of a file"""
        md5_hash = hashlib.md5()
        with open(path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()

    def _get_cache_dir(self):
        """check if top level data directory exists, if so use that
        otherwise create cache dir in home directory
        """
        if os.path.exists("/data"):
            self.cache_dir = "/data/cache"
            try:
                self.u.path_validator(self.cache_dir)
            except PermissionError:
                self.cache_dir = os.path.join(os.path.expanduser("~"), "cache")
        else:
            self.cache_dir = os.path.join(os.path.expanduser("~"), "cache")
        self.u.path_validator(self.cache_dir)
        if self.system is not None:
            self.cache_dir = os.path.join(self.cache_dir, self.system)
        if not os.path.exists(self.cache_dir):
            self.u.path_validator(self.cache_dir)

    def _from_id(self):
        """ """
        self.setup_mode = True
        if isinstance(self._id, str):
            self._id = bson.ObjectId(self._id)
        res = self.u.m_conn.find_one({"_id": self._id})
        if res is None:
            raise ValueError(f"no record found for id: {self._id}")
        for key, val in res.items():
            setattr(self, key, val)

    def _get_data(self):
        """download data from s3 to local drive"""
        self._get_cache_dir()
        self._path = os.path.join(self.cache_dir, self.key)
        if self.is_folder:
            self._download_folder()
        else:
            if not os.path.exists(self._path):
                s3 = boto3.client("s3")
                s3.download_file(Bucket=self.bucket, Key=self.key, Filename=self._path)
        
    def _download_folder(self):
        """download all files into a folder form s3"""
        raise NotImplementedError("folder cacheing not implemented")
        # s3 = boto3.client("s3")
        # path = os.path.join(self.cache_dir, self.key)
        # self.u.path_validator(path)
        # bucket = s3.Bucket(self.bucket)
        # for obj in bucket.objects.filter(Prefix=self.key):
        #     if not os.path.exists(os.path.join(path, obj.key)):
        #         s3.download_file(self.bucket, obj.key, os.path.join(path, obj.key))

    def delete_local(self):
        """delete local copy of data"""
        # check data exists
        if not os.path.exists(self.path):
            return True
        # check if it is a folder
        if os.path.isdir(self.path):
            shutil.rmtree(self.path)
        else:
            os.remove(self.path)

    def _create_file_key(self, path):
        """modify folder or file to include a random string
        to prevent overwriting existing files, and allow multiple feature sets / versions to co-exist
        """
        if self.is_folder:
            raise NotImplementedError("folder cacheing not implemented")
            # new_dir = os.path.join(
            #    path, self.cache_params.file_type + "_" + self.u.random_string(3)
            # )
            # os.rename(path, new_dir)
            # self.path = new_dir
        else:
            natural_key = os.path.basename(self.path)
            extension = natural_key.split(".")[-1]
            # if len(split_key) > 2: # handles cases where there are multiple periods in the file name
            self.key = self.u.random_string(10) + "." + extension

    def _replace_cache(self):
        """delete existing s3 file and load a new one"""
        s3 = boto3.client("s3")
        s3.delete_object(Bucket=self.bucket, Key=self.key)
        self._upload_object()

    def _upload_object(self):
        """upload new file or folder to s3"""
        s3 = boto3.client("s3")
        self._create_file_key(self.path)
        s3.upload_file(self.path, self.bucket, self.key)
