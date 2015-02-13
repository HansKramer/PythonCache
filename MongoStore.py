#! /usr/bin/env python
#
# \brief     MongoDB store implementation
# \author    Hans Kramer
# \version   0.1
# \date      Feb 2015
#
# \details
#            Implementation of a connector between the Cache Pipe and MongoDB back-end store
#
#            The MongoStore object inherits from CacheImpl and is a Mixin class for the
#            CachePipe class. After it it attached to the CachePipe object it ends the pipeline
#            

import pymongo
from   bson.objectid import ObjectId
from   syslog        import syslog, LOG_INFO, LOG_PERROR, LOG_USER
from   Cache         import CacheImpl



class MongoStore(CacheImpl):
   
    """
        Example implementation of how to implement a connection to a back-end store
        for the Cache Library
    """

    def __init__(self, host = '127.0.0.1', port = 27017, db = "test", collection = "test"):
        """
            Contructor 

            @param[in] host       The host were your MongoDB database server is running
            @param[in] port       The port the MongoDB server is using
            @param[in] db         The database name
            @param[in] collection The collection name
        """
        CacheImpl.__init__(self)
        self._mc = pymongo.Connection(host, port)
        self._db = self._mc[db]
        self._c  = self._db[collection]
    
    def read(self, key):
        """
            Read data identified by key from MongoDB

            @param[in] key   The key identifying the data
            @returns         The data associated by key
        """
        syslog(LOG_INFO, "MongoStore.read %s" % key)
        try:
            return self._c.find({'_id': ObjectId(key)}, {'_id': 0})[0]['name']
        except IndexError:
            return None

    def write(self, key, value):
        """
            Write data specified by value and identified by key to MongoDB

            @param[in] key    The key identifying the data
            @param[in] value  The data itself
        """
        syslog(LOG_INFO, "MongoStore.write %s %s" % (key, value))
        return self._c.update({'_id': ObjectId(key)}, {'_id': ObjectId(key), 'name': value}, True)


if __name__ == "__main__":
     import sys
     from   syslog import openlog

     openlog("MongoStore Unit Testing", LOG_PERROR, LOG_USER)
     
     if len(sys.argv) != 2:
         print "specify what to do!"
         sys.exit()

     ms = MongoStore()
     if sys.argv[1] == "clear":
         ms._c.remove()
     elif sys.argv[1] == "write":
         ms.write("54dc80c37b020a219e000000", "Jean Doe")     
         ms.write("54dc80c37b020a219e000001", "John Doe")
         ms.write("54dc798d77f68a6361532d05", "Hans Kramer")
     elif sys.argv[1] == "read":
         print ms.read("54dc80c37b020a219e000000")
         print ms.read("54dc80c37b020a219e000001")
         print ms.read("54dc798d77f68a6361532d05")
     else:
         print "Unknown task"
