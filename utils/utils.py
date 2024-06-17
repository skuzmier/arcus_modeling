"""shared basic utilities library
"""

import datetime
import json
import os
import sys
import random
import string
import tempfile
import time

import boto3
import dateutil.parser
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import pytz
import redis
import sentry_sdk
from psycopg2 import sql
from mongo import MongoConn

sentry_sdk.init("https://c88a22cdaa3b498eb81b8cc8ee6b1af4@sentry.io/1884626")


class Utils():
    """
    Purpose:
      Base class to share basic functionality with other classes
    """
    # local_tz = 'US/Pacific'
    local_tz = 'US/Central'  # this is GMT-8 (no DST) sign flip is intended

    def __init__(self):
        self._conn = None
        self._engine = None
        self.mongo = MongoConn()
    
    def __del__(self):
        """close the postgres connection when the object is deleted
        """
        if self._conn:
            self._conn.close()

    @property
    def conn(self):
        """
        Purpose:
          Connection manager for psycopg2
           database access
        """
        self._check_conn(self._conn)
        return self._conn

    @conn.setter
    def conn(self, object):
        """
        Purpose:
          Check connection object validity and set it
        """
        self._check_conn(object)

    @conn.deleter
    def conn(self):
        """
        Purpose:
          Close psycopg2 connection and replace
           with None if the property is being deleted
        """
        self.conn.close()
        self._conn = None

    @property
    def m_conn(self):
        """
        """
        return self.mongo.conn

    @property
    def dict_cursor(self):
        return self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    @property
    def s3(self):
        return boto3.client('s3')

    def execute_query(self, sql, params=None, dict_cursor=False, fetch=None, commit=False):
        """execute sql statement and hanle exceptions
        Args:
            sql (str) : sql statement to execute
            dict_cursor (bool) : use dictionary cursor or standard cursor
            fetch: str
                all, one, None
        Returns:
            cursor if no error
            res if fetch is not none
            error message if error
        """
        cur = self._execute_query(sql=sql,
                                  params=params,
                                  dict_cursor=dict_cursor,
                                  commit=commit)
        if fetch is None and commit:
            cur.close()
            self.conn.close()
            return None
        if not fetch:
            res = cur
        if fetch == 'all':
            res = cur.fetchall()
            cur.close()
            self.conn.close()
        elif fetch == 'one':
            res = cur.fetchone()
            cur.close()
            self.conn.close()
        return res

    def _execute_query(self, sql, params=None, dict_cursor=False, commit=False):
        """execute sql statement and handle exceptions
        Args:
            sql (str) : sql statement to execute
            dict_cursor (bool) : use dictionary cursor or standard cursor
        Returns:
            cursor if no error
            error message if error
        
            #FIXME: The rollback command is not working as expected, and the connection is not being closed
        """
        if dict_cursor:
            cur = self.dict_cursor
        else:
            cur = self.conn.cursor()

        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            if commit:
                self.conn.commit()
            return cur
        except Exception as e:
            self.conn.rollback()
            cur.close()
            self.conn.close()
            sentry_sdk.capture_exception(e)
            raise RuntimeError("Query Failed to execute: %s" % e)

    def insert_record(self, table, columns, values, return_id=False):
        """ insert record into table
            Args:
                table: str
                columns: list
                values: list
        """
        if len(columns) != len(values):
            raise ValueError("columns and values must be same length")
        statement = self._create_insert_statement(table=table,
                                                  columns=columns,
                                                  return_id=return_id)
        id = self._insert_record(insert_statement=statement,
                                 values=values,
                                 return_id=return_id)
        return id

    def _create_insert_statement(self, table, columns, return_id):
        """
        """
        statement = "INSERT INTO %s " % table
        cols = "("
        vals = "("
        for col in columns:
            cols = cols + col + ","
            vals = vals + "%s,"
        cols = cols[:-1] + ")"
        vals = vals[:-1] + ")"
        statement = statement + cols + "VALUES" + vals
        if return_id:
            statement = statement + " RETURNING id"
        return statement

    def _insert_record(self, insert_statement, values, return_id):
        """
            Args:
                insert_statement:str
                values: list
                return_id: boolean
                  should id of new value be returned

            Returns:
            --------
                res: None or int
                    value depends on if return_id flag is set
        """
        res = None
        cur = self.conn.cursor()
        try:
            cur.execute(insert_statement, tuple(values))
            if return_id:
                res = cur.fetchone()[0]
            self.conn.commit()
            cur.close()
        except Exception as e:
            sentry_sdk.capture_exception(e)
            self.conn.rollback()
            cur.close()
        return res

    def _create_conn(self):
        """
        Purpose:
          Create psycopg2 connection
        Params:
          none
        Returns:
          none
        """
        attempts = 0
        while attempts < 3:
            try:
                connect_str = "dbname='%s' user='%s' host='%s' password='%s'"\
                              % (os.environ['TSDATABASE'],
                                 os.environ['TSUSER'],
                                 os.environ['TSHOST'],
                                 os.environ['TSPASSWORD'])
                # use our connection values to establish a connection
                conn = psycopg2.connect(connect_str)
                if isinstance(conn, psycopg2.extensions.connection):
                    return conn
            except Exception as e:
                attempts += 1
                print(
                    "Connection error check for invalid dbname, user or password."
                )
                print(e)
                sentry_sdk.capture_exception(e)
                time.sleep(0.25)

    def _check_conn(self, conn):
        """
        Purpose:
          check to see if connection string
           is valid psycopg2 connection string
           create valid connection if it is not
        Params:
          conn (psycopg2.extensions.connection or None)
        Returns:
          conn valid psycopg2 connnection string
        """
        if not isinstance(conn, psycopg2.extensions.connection):
            conn = self._create_conn()
        if conn.closed:
            conn = self._create_conn()
        self._conn = conn

    def time_converter(self, obj, target="default", source_tz=None, target_tz=None, strip_tz=False):
        """
        Purpose:
          convert date object to a different format
        Params:
            obj: str, datetime.date, datetime,datetime, np.datetime64
            target: str
        Returns:
          Object correctly formatted
        Note:
          If target set to default then it assumes it should be flipped
        """
        if target == "default":
            if isinstance(obj, (datetime.datetime, np.datetime64, datetime.date)):
                target = "str"
            else:
                target = "dt"
        if isinstance(obj, np.datetime64):
            obj = pd.to_datetime(obj).strftime("%m-%d-%Y %H:%M:%S")
        if target == "dt" and isinstance(obj, datetime.datetime):
            return obj
        if target == "date" and (
            isinstance(obj, datetime.date) and not isinstance(obj, datetime.datetime)
        ):
            return obj
        if target in ["dt", "date"]:
            if isinstance(obj, str):
                try:
                    obj = dateutil.parser.parse(obj)
                except Exception:
                    raise ValueError("invalid date string: ", obj)
            elif isinstance(obj, datetime.date):
                obj = datetime.datetime.combine(obj, datetime.time(0, 0))
            if target == "date":
                return obj.date()
            else:
                return obj

        elif target == "str":
            if isinstance(obj, str):
                return obj
            else:
                obj = obj.strftime("%m-%d-%Y %H:%M:%S")
                return obj
        else:
            raise ValueError("Unknown target type, acceptable types are dt and str")
    
    def _convert_tz(self, dt, source_tz=None, target_tz=None) -> datetime.datetime:
        """
        """
        if target_tz is None and source_tz is None:
            return dt
        if source_tz is None and dt.tzinfo is None:
            raise ValueError("source_tz must be provided if dt is timezone naive")
        if not isinstance(dt, datetime.datetime):
            raise TypeError("dt must be a datetime object")
        if source_tz == 'local':
            source_tz = self.local_tz
        if target_tz == 'local':
            target_tz = self.local_tz
        if isinstance(source_tz, str):
            source_tz = pytz.timezone(source_tz)
        if isinstance(target_tz, str):
            target_tz = pytz.timezone(target_tz)
        #add timezone info if it is not present
        if source_tz is not None:
            source_tz = pytz.timezone(source_tz)
            dt = source_tz.localize(dt)
        if target_tz is not None:
            target_tz = pytz.timezone(target_tz)
            dt = dt.astimezone(target_tz)
        return dt
        
        
    def _strip_tz(self, dt) -> datetime.datetime:
        """
            Args:
                dt: datetime.datetime
        """
        return dt.replace(tzinfo=None)


    def UTC_to_PST(self, dt) -> datetime.datetime:
        """
        Purpose:
          Localize a datetime.datetime object from UTC to PST
        Params:
          dt (datetime.datetime) object to convert. Either timezone naieve or UTC
        Returns:
          dt (datetime.datetime) with tzinfo set to PST
        NOTE; To Decprecate. Use UTC_to_Local instead
        """
        dt = dt.replace(tzinfo=pytz.utc).astimezone(
            pytz.timezone('US/Pacific'))
        return dt

    def _get_table_column_names(self, table):
        """get a list of column names for a table

            Args:
            -----
                table: str

            Returns:
            --------
                columns: list
        """
        query = "select * from {table_name} limit 0"
        query = psycopg2.sql.SQL(query).format(
            table_name=psycopg2.sql.Identifier(table))
        cur = self.execute_query(query)
        columns = [desc[0] for desc in cur.description]
        cur.close()
        return columns

    def _validate_columns(self, record_dict, table):
        """check all keys in dictionary to insert exist as columns in table

            Args:
            -----
                record_dict: dict
                table:str

            Raises:
            --------
                KeyError: if record_dict contains invalid column
        """
        target_cols = []
        table_cols = self._get_table_column_names(table=table)
        for (col, value) in record_dict.items():
            target_cols.append(col)
        for col in target_cols:
            if col not in table_cols:
                raise KeyError("Column %s not in table" % col)

    def insert_dict_record(self, table, record_dict, return_id=False):
        """insert a dictionary to a table

            Args:
            -----
                record_dict: dict
                table: str

            Returns:
            --------
                new_record_id: int
        """
        columns = []
        values = []
        self._validate_columns(record_dict=record_dict, table=table)
        for (col, value) in record_dict.items():
            columns.append(col)
            values.append(value)
        new_record_id = self.insert_record(table=table,
                                           columns=columns,
                                           values=values,
                                           return_id=return_id)
        if return_id:
            return new_record_id

    def UTC_to_local(self, dt):
        """
        Purpose:
          Localize a datetime.datetime object from UTC to local tz as defined above or in config.py (not yet built)
        Params:
          dt (datetime.datetime) object to convert. Either timezone naieve or UTC
        Returns:
          dt (datetime.datetime) with tzinfo set
          to local time zone
        """
        local_tz = pytz.timezone(self.local_tz)
        # If no timezone is set assume it is UTC and set it
        if dt.tzinfo is None:
            utc = pytz.timezone('UTC')
            dt = utc.normalize(utc.localize(dt))
        dt = dt.astimezone(local_tz)
        return dt

    def local_to_UTC(self, dt):
        """
        Purpose:
          Localize a datetime.datetime object from UTC to local tz as defined above or in config.py (not yet built)
        Params:
          dt (datetime.datetime) object to convert.
        Returns:
          dt (datetime.datetime) with tzinfo set to UTC
        """
        utc = pytz.timezone('UTC')
        # If no timezone is set assume it is local_tz and set it
        if isinstance(dt, str):
            dt = self.time_converter(dt)
        if dt.tzinfo is None:
            local_tz = pytz.timezone(self.local_tz)
            dt = local_tz.normalize(local_tz.localize(dt))
        dt = dt.astimezone(utc)
        return dt

    def to_sql_input(self, object):
        """
        Purpose:
          Prepare input for use in postgresql query with an in statement
        Params:
          object (str or list) params to feed into query
        Returns:
          res (n-tuple)
        """
        assert type(object) in [str, list,
                                set], "object must be type str, list, or set"
        if isinstance(object, str):
            object = [object]
        elif isinstance(object, set):
            object = list(object)
        return tuple((x for x in object))

    def mem_optimize_df(self, df):
        """down sample column sizes of dataframe
        DEPRECATED. WILL Remove later
            Args:
            -----
            df: pd.DataFrame

            Returns:
            --------
            df: pd.DataFrame
        """
        # ints = df.select_dtypes(include=['int64']).columns.tolist()
        # df[ints] = df[ints].apply(pd.to_numeric, downcast='integer')
        # floats = df.select_dtypes(include=['float64']).columns.tolist()
        # df[floats] = df[floats].apply(pd.to_numeric, downcast='float')
        return df

    def large_query_to_df(self, query, index_col=None):
        """retrieve a large query and convert to a dataframe.
            10x memory, 5x speed improvement from pd.read_sql_query
        """
        try:
            cur = self.conn.cursor()
            with tempfile.TemporaryFile() as tmp:
                cur.copy_expert(query, tmp)
                tmp.seek(0)
                df = pd.read_csv(tmp,
                                 engine='c',
                                 index_col=index_col,
                                 parse_dates=True)
                return df
        except Exception as e:
            self.conn.rollback()
            sentry_sdk.capture_exception(e)
            raise e

    def path_validator(self, path):
        """
        Purpose:
          Check if path exists and create it if not
        Params:
          path (str)
        Returns:
          none
        """
        if not os.path.exists(path):
            os.makedirs(path)

    def set_tz(self, dt, tz_name):
        """
        Purpose:
          Set tz_info on a datetime.datetime object
        Params:
          dt (datetime.datetime) object to add tz_info to
          tz_name (str) a timezone name valid from pytz
        Returns:
          dt (datetime.datetime) localized
        """
        if not isinstance(tz_name, str):
            raise TypeError("tz_name must be a string not %s" % type(tz_name))
        if tz_name not in pytz.all_timezones:
            raise ValueError("%s is not a valid timezone")
        return pytz.timezone(tz_name).localize(dt)

    def to_cartesian(self, lat, lng):
        """
        Purpose:
          Convert lat long cartesian tripple
        Params:
          lat (float)
          lon (float)
        Returns:
          (3-tuple)
        """
        R = 6367  # radius of the Earth in kilometers
        x = R * np.cos(lat) * np.cos(lng)
        y = R * np.cos(lat) * np.sin(lng)
        z = R * np.sin(lat)
        return x, y, z

    def create_task_record(self, task_name, task_params):
        """
        """
        task_params = json.dumps(task_params)
        sql = "INSERT INTO task_records (task, task_params) VALUES (%s, %s) RETURNING task_id"
        try:
            cur = self.conn.cursor()
            cur.execute(sql, (task_name, task_params))
            res = cur.fetchone()[0]
            self.conn.commit()
            return res
        except Exception as e:
            self.conn.rollback()
            sentry_sdk.capture_exception(e)
            raise e

    def update_task_record(self, task_id, started=None, completed=None):
        """
        """
        try:
            cur = self.conn.cursor()
            if started:
                sql = "UPDATE task_records set started = %s where task_id = %s"
                cur.execute(sql, (started, task_id))
            elif completed:
                sql = "UPDATE task_records set completed = %s where task_id = %s"
                cur.execute(sql, (completed, task_id))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            sentry_sdk.capture_exception(e)
            raise e

    def check_task_record(self, task_id):
        sql = "SELECT * from task_records where task_id = %s"
        res = self.execute_query(sql, params=([task_id]), dict_cursor=True)
        return json.loads(json.dumps(res.fetchone(), default=str))

    def json_to_s3(self, json_str, bucket, file_name=None):
        """upload json str to s3

            Args:
            -----
                json_str: str
                bucket: str
                file_name: None or str

            Returns:
            --------
                file_name
        """
        if not file_name:
            file_name = self.random_string(8) + '.json'
        with open(file_name, 'w') as f:
            f.write(json_str)
        self.upload_to_s3(bucket=bucket, file_name=file_name)
        os.remove(file_name)
        return file_name

    def download_from_s3(self, bucket_name, object_name, file_name=None):
        """
        """
        if not file_name:
            file_name = os.path.basename(object_name)
        s3 = boto3.client('s3')
        obj = s3.download_file(bucket_name, object_name, file_name)
        return True

    def delete_from_s3(self, bucket_name, object_name):
        """
        """
        client = boto3.client('s3')
        client.delete_object(Bucket=bucket_name, Key=object_name)
        return True

    def read_s3_json(self, bucket_name, object_name):
        """get an object from s3 json file
        """
        s3 = boto3.resource('s3')
        content_object = s3.Object(bucket_name, object_name)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        return json_content

    def str_from_s3(self, bucket_name, object_name):
        """read a file from S3 and return a string
            Args:
            -----
                bucket_name: str
                  name of s3 bucket
                object_name: str
                  name of s3 object

        """
        s3 = boto3.resource('s3')
        content_object = s3.Object(bucket_name, object_name)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        return file_content

    def df_from_s3(self, bucket_name, object_name):
        """get data from s3 and convert csv to a dataframe
            Args:
            -----
                bucket_name: str
                  name of s3 bucket
                object_name: str
                  name of s3 object
            Raises:
            -------
                TypeError: Invalid file type if object name does not end with .csv
        """
        if not object_name.endswith('.csv'):
            raise TypeError("object must be a csv and end with .csv")
        with tempfile.TemporaryFile() as tmp:
            csv_str = self.str_from_s3(bucket_name=bucket_name,
                                       object_name=object_name)
            tmp.write(str.encode(csv_str))
            tmp.seek(0)
            df = pd.read_csv(tmp, engine='c', parse_dates=True)
        return df

    def upload_to_s3(self, file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        if not os.path.exists(file_name):
            raise FileNotFoundError("The target file could not be located")
        if os.path.isdir(file_name):
            raise ValueError("File name %s is a directory, not a file")

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        try:
            s3_client = boto3.client('s3')
        except Exception as e:
            sentry_sdk.capture_exception("S3 Login Error %s" % e)
        response = s3_client.upload_file(file_name, bucket, object_name)
        return True

    def get_common_col_names(self, current_names, attribute, market_run):
        """
        Purpose:
          create a dictionary to rename dataframe columns to match
          a consistent format.

          naming format is all lowercase: Attribute_MarketRun_CurrentName
        Params:
          current_names (list) list of current column names
          attribute (str) name of attribute measured
          market_run (str) name of market run
        Returns:
          names (dict) in format {'old_name': 'new_name'}
        """
        res = {}
        if not isinstance(current_names, list):
            raise TypeError("current names must be provided as a list")
        if not isinstance(attribute, str):
            raise TypeError("attribute name must be a string")
        if not isinstance(market_run, str):
            raise TypeError("Market run must be a string")
        for old_name in current_names:
            new_name = '_'.join(
                [market_run.lower(),
                 attribute.lower(),
                 old_name.lower()])
            res.update({old_name: new_name})
        return res

    def rename_columns(self, df, attribute, market_run):
        """
        Purpose:
          Replace current column names with formattted common column names
          generated by get_common_col_names
        Params:
          df (Dataframe) with old col names
        Returns:
          df (Dataframe) with new col names
        """
        col_names = df.columns.tolist()
        remapping_dict = self.get_common_col_names(col_names, attribute,
                                                   market_run)
        df = df.rename(index=str, columns=remapping_dict)
        return df

    def random_string(self, size):
        """
        Purpose: create a random string of a defined length
        Created to prefent file naming collisions
        Params: size - integer
        Returns: s - string
        """
        if size <= 0:
            raise ValueError(
                "string size must be greater than or equal to one")
        if not isinstance(size, int):
            raise TypeError("size must be a integer")
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for x in range(size))

    def insert_df(self, df, table_name, columns=None):
        """
        Purpose: Manage process to insert dataframe directly into database
        Params: dataframe and tablename, accepts psycopg2 connection if provided
        Returns: None
        NOTE: This process does not handle any uperserts and can create dupes
        """
        columns = columns or list(df.columns)
        if not isinstance(columns, list):
            raise TypeError("columns must be a list")
        fname = table_name + self.random_string(8) + '_tmp.csv'
        fname = os.path.join(os.getcwd(), fname)
        df.to_csv(fname, index=False, header=False)
        self.insert_csv_file(fname, table_name, colnames=columns)
        self.conn.close()
        os.remove(fname)

    def insert_csv_file(self, path, table_name, colnames):
        ###
        # Purpose: insert csv file to staging table
        # Params: Psycopg2 connection, filepath
        # Returns: None
        ###
        try:
            with open(path) as f:
                cursor = self.conn.cursor()
                cursor.copy_from(file=f,
                                 table=table_name,
                                 columns=colnames,
                                 sep=',',
                                 size=int(2**18))
                self.conn.commit()
                cursor.close()
                self.conn.close()
        except Exception as err_msg:
            cursor.close()
            self.conn.rollback()
            raise Exception(err_msg)

    def sum_dict(self, target_dict):
        """sum up values in a dictionary

            Args:
            -----
                target_dict: dict

            Returns:
            --------
                res: float
        """
        res = 0
        for (key, value) in target_dict.items():
            res += value
        return res

    def update_record(self, table, data, id):
        """update existing record

            Args:
            -----
                table: str
                    name of table to update
                data: dict
                    key, value pairs of things to be updated
                id: int
                    id value of record to update
        """
        query = self._compose_update_statement(table=table, data=data, id=id)
        try:
            cur = self.execute_query(query)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        finally:
            cur.close()
            self.conn.close()

    def _compose_update_statement(self, table, data, id):
        """create update statement

            Args:
            -----
                table: str
                data: dict
                    key, value pairs of things to be updated
                id: int
                    id value of record to update

            Returns:
            --------
                query: str
        """
        query = sql.SQL("UPDATE {table} SET {data} WHERE id = {id}").format(
            table=sql.Identifier(table),
            data=sql.SQL(', ').join(
                sql.Composed(
                    [sql.Identifier(k),
                     sql.SQL(" = "),
                     sql.Literal(v)]) for (k, v) in data.items()),
            id=sql.Literal(id))
        return query

    def get_shared_credentials(self, service, user):
        """get the credentials for a given rabbitmq user
        """
        query = """SELECT * from shared_creds
                  WHERE service = {service}
                 AND username = {username}"""
        query = sql.SQL(query).format(service=sql.Literal(service),
                                      username=sql.Literal(user))
        res = self.execute_query(query, dict_cursor=True, fetch='one')
        return res

    def get_records(self, table, columns=None, where=None, limit=None):
        """get records from a table

        Args:
            table (str): name of table to query
            columns (list): list of columns to return
            where (dict): key, value pairs of conditions to apply

        Returns:
            list: list of records
        """
        query = self._compose_select_statement(table, columns, where, limit)
        res = self.execute_query(query, dict_cursor=True, fetch='all')
        return res

    def _compose_select_statement(self, table, columns=None, where=None, limit=None):
        """compose a select statement

        Args:
            table (str): name of table to query
            columns (list): list of columns to return
            where (dict): key, value pairs of conditions to apply

        Returns:
            str: select statement
        """
        if columns:
            query = sql.SQL("SELECT {columns} FROM {table}").format(
                columns=sql.SQL(', ').join(sql.Identifier(col) for col in columns),
                table=sql.Identifier(table))
        else:
            query = sql.SQL("SELECT * FROM {table}").format(table=sql.Identifier(table))
        if where:
            query += sql.SQL(" WHERE {where}").format(
                where=sql.SQL(' AND ').join(
                    sql.Composed(
                        [sql.Identifier(k),
                         sql.SQL(" = "),
                         sql.Literal(v)]) for (k, v) in where.items()))
        if limit:
            query += sql.SQL(" LIMIT {limit}").format(limit=sql.Literal(limit))
        return query

    def get_redis_conn(self, db):
        """get a redis connection
        """
        if isinstance(db, str):
            db = int(db)
        r_con = redis.Redis(host=os.environ['REDISHOST'],
                            port=os.environ['REDISPORT'],
                            db=db)
        return r_con
    
    def is_s3(self):
        """check if the code is running in an AWS environment
        """
        if os.environ.get('aws_default_region', None):
            return True
        return False
