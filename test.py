#! /usr/bin/env python
#
# \brief     Test Cases
# \author    Hans Kramer
# \version   0.1
# \date      Feb 2015
#


from   TestStore import TestStore
from   Cache     import CachePipe, CacheImplWriteBackMemory
import sys
from   syslog    import openlog, LOG_PERROR, LOG_USER


if __name__ == "__main__":
    openlog("Tests", LOG_PERROR, LOG_USER)
   
    if len(sys.argv) != 2:
         print "specify what to do!"
         print "\te.g. ./test.py test1"
         sys.exit()

    # construct our cache
    mdc = CachePipe()
    mdc.attach(CacheImplWriteBackMemory, cache_size=3)  # Made this node a write-back caching node (with nice small cache size for testing...)
    mdb = CachePipe()
    mdb.attach(TestStore)
    mdc.connect(mdb) # hook our cache to TestStore

    mdb._data = {"keyA": None, "keyB": None, "keyC": None, "keyD": None}

    if sys.argv[1] == "test1":
        mdb._data["keyA"] = "valueA0"
        print mdc.read("keyA")
        mdc.write("keyA", "valueA1")
        print mdc.read("keyA")
        print "on store keyA value : ", mdb._data["keyA"]
    elif sys.argv[1] == "test2":
        mdb._data["keyA"] = "valueA0"
        mdc.write("keyA", "valueA1")
        print mdc.read("keyA")
        print "on store keyA value : ", mdb._data["keyA"]
    elif sys.argv[1] == "test3":
        mdb._data["keyA"] = "valueA0"
        mdc.write("keyA", "valueA1")
        print "on store keyA value : ", mdb._data["keyA"]
        mdc.write("keyB", "valueB")
        print "on store keyA value : ", mdb._data["keyA"]
        mdc.write("keyC", "valueC")
        print "on store keyA value : ", mdb._data["keyA"]
        mdc.write("keyD", "valueD")
        print "on store keyA value : ", mdb._data["keyA"]
    elif sys.argv[1] == "test4":
        mdb._data["keyA"] = "valueA0"
        mdc.write("keyA", "valueA1")
        print "on store keyA value : ", mdb._data["keyA"]
        mdc.read("keyB")
        print "on store keyA value : ", mdb._data["keyA"]
        mdc.read("keyC")
        print "on store keyA value : ", mdb._data["keyA"]
        mdc.read("keyD")
        print "on store keyA value : ", mdb._data["keyA"]
    elif sys.argv[1] == "flush":
        mdc.write("keyA", "valueA0")
        mdc.write("keyB", "valueB0")
        mdc.flush()
    else:
        print "Unknown task"         
    
