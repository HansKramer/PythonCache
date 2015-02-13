#! /usr/bin/env python
#
# \brief     TestStore, a special class for testing purposes
# \author    Hans Kramer
# \version   0.1
# \date      Feb 2015
#
# \details
#            

import pymongo
from   bson.objectid import ObjectId
from   syslog        import syslog, LOG_INFO, LOG_PERROR, LOG_USER
from   Cache         import CacheImpl


class TestStore(CacheImpl):
   
    def __init__(self, host = '127.0.0.1', port = 27017, db = "test", collection = "test"):
        self.reset()
    
    def reset(self):
        self._data = {}
        for i in range(10):
             self._data["%03d" % i] = i

    def read(self, key):
        syslog(LOG_INFO, "TestStore.read %s" % key)
        return self._data[key]

    def write(self, key, value):
        syslog(LOG_INFO, "TestStore.write %s %s" % (key, value))
        self._data[key] = data
