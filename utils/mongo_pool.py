import pymongo
import os


class MongoPool:
    # create a singleton class to pool all mongo connections at env level.
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MongoPool, cls).__new__(cls)
            # check if running in aws, if so use the srv connection string for atlas, otherwise use the standard connection string
            #if os.environ.get('AWS_DEFAULT_REGION', None):
            if os.environ.get('USE_ATLAS', "True") == "True": #Check if using MonngoDB Atlas (Cosine Production)
                uri = "mongodb+srv://"
            else:
                uri = "mongodb://"
            uri += os.environ['MONGO_USER'] + ':'
            uri += os.environ['MONGO_PASS'] + '@'
            uri += os.environ['MONGO_HOST'] + '/'
            uri += '?retryWrites=true&w=majority&appName=CosineMongo'
            cls._instance.client = pymongo.MongoClient(uri, 
                                                       server_api=pymongo.server_api.ServerApi('1'),
                                                       maxPoolSize=50)

        return cls._instance