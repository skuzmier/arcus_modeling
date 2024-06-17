from mongo_pool import MongoPool
import os

class MongoConn:
    """ Class to connect to MongoDB
    """
    def __init__(self, db_name=None):
        """
        """
        self._pool = MongoPool()
        self._client = None
        self._db_name = db_name or os.environ.get('MONGO_DB_NAME', 'main') # allow db name to be passed in or set in env
        self._collection = 'main'
        self._conn = None

    def __repr__(self):
        """
        """
        return f'MongoConn(db={self.db})'

    @property
    def collections(self):
        """
        """
        return self.db.list_collection_names()

    @property
    def client(self):
        """
        """
        return self._pool.client

    @property
    def db(self):
        """
        """
        return self.client[self.db_name]
        
    @property
    def db_name(self):
        """
        """
        return self._db_name

    @db_name.setter
    def db_name(self, db_name):
        """
        """
        self._db_name = db_name

    @property
    def collection(self):
        """
        """
        return self._collection

    @collection.setter
    def collection(self, collection):
        """change collection name
        """
        self._collection = collection

    @property
    def conn(self):
        """get connection to specific collection
        """
        return self.db[self.collection]

    def save_record(self, record, collection):
        """
        """
        result = self.db[collection].insert_one(record)
        return result.inserted_id
