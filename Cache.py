#! /usr/bin/env python
#
# \brief     Cache Class
# \author    Hans Kramer
# \version   0.2
# \date      Feb 2015
# \details  
#            I see a cache basically as a pipe that pushes data back and forth, while
#            it remembers a limited amount data and stores it locally. This happens
#            completely opaquely from the user.
#            
#            The following goals are set for this implementation
#           
#            - The Cache object does not any knowlegde of the back-end store
#            - The Objects that are stored do not have to be of any particular type
#            - Multiple levels of caching should be possible
#
#            To create an interface to the back-end store, the user of this class
#            simply derives from CacheImpl and overloads the read and write methods.
#            Then simply attached this implemetation by means of a Dynamic mixin to a
#            CachePipe object by means of the attach method.
#
#            To be able to store any kind of data, objects are serialized by the 
#            Python cPickle module.
#
#            The ability to chain more than one pipe provides the multi level
#            caching feature of this library 
#      
# \remark    0.2 Change the cache storage from
#                self._cache_store[key]
#                self._cache_dirty[key]
#            to
#                self._cache[key]["data"]
#                self._cache[key]["dirty"]
#


import cPickle
from   syslog import syslog, LOG_DEBUG


class CacheImpl(object):
    """
        Base class for a Cache implementation.
        This base class defines the interface.
        On its own it only passes data, and does no caching.
        Therefore, the read and write methods need to be overloaded.
    """

    def __init__(self):
        pass

    def read(self, key):
        """
            Reads a cache value 

            @param[in] key  The key identifying the cached data
            @return         the value belonging to the key
        """
        syslog(LOG_DEBUG, "CacheImpl.read : %s" % key)
        return self.pipe_read(key)

    def write(self, key, value):
        """
            Writes a cache value

            @param[in]   key     The key identifying the cached data
            @param[out]  value   The value belonging to the key
        """
        syslog(LOG_DEBUG, "CacheImpl.write : %s %s" % (key, value))
        self.pipe_write(key, value)

    def flush(self):
        """
            Calling the flush method of the backing store
        """
        CachePipe.flush(self)    # this is a tricky thing, if you don't call it, flush will not be passed
        syslog(LOG_DEBUG, "CacheImpl.flush")
        

class CacheImplWriteBackMemory(CacheImpl):
    """
         A write-back cache implementation 
         Stores data in a simple hash
         Uses cPickle to serialize data
         Uses a simple first in first out rotation mechanism
    """

    def __init__(self, cache_size = 1024):
        """
            @param[i] cache_size   Number of data items allowed in the cache
        """
        CacheImpl.__init__(self)
        self._cache_store = {}   
        self._cache_dirty = {}
        
        self._cache       = {}
        self._cache_fifo  = []
        self._cache_max   = cache_size
        
    def read(self, key):
        """
            Reads a cache value

            @param[in] key  The key identifying the cached data
            @return         the value belonging to the key
        """
        syslog(LOG_DEBUG, "CacheImplWriteBack.read : %s" % key)
        if not self._cache.has_key(key): # cache miss
            syslog(LOG_DEBUG, "cache miss for %s" % key)
            if len(self._cache_fifo) >= self._cache_max: #free a cache block
                old_key = self._cache_fifo.pop(0)
                if self._cache[old_key]["dirty"]:
                    self.pipe_write(old_key, cPickle.loads(self._cache[old_key]["data"]))
                del self._cache[old_key]
            self._cache_fifo += [key,]
            self._cache[key] = {"data": cPickle.dumps(self.pipe_read(key)), "dirty": False}
        return cPickle.loads(self._cache[key]["data"])

    def write(self, key, value):
        """
            Writes a cache value

            @param[in]   key     The key identifying the cached data
            @param[out]  value   The value belonging to the key
        """
        syslog(LOG_DEBUG, "CacheImplWriteBack.write : %s %s" % (key, value))
        if not self._cache.has_key(key):  # cache miss
            syslog(LOG_DEBUG, "cache miss for %s" % key)
            if len(self._cache_fifo) >= self._cache_max: #free cache block
                old_key = self._cache_fifo.pop(0)
                if self._cache[old_key]:
                    self.pipe_write(old_key, cPickle.loads(self._cache[old_key]["data"]))
                del self._cache[old_key]
            # cache block available
            self._cache_fifo += [key,]
        self._cache[key] = {"data": cPickle.dumps(value), "dirty": True}

    def flush(self):
        """
            Write all dirty cache enties to the store
        """
        for key in self._cache:
            if self._cache[key]["dirty"]: 
                syslog(LOG_DEBUG, "CacheImplWriteBack.flush key=%s" % key)
                self.pipe_write(key, cPickle.loads(self._cache[key]["data"]))
        CacheImpl.flush(self)


class CachePipe(object):
    """
         The Cache frame work to connect Cache pipes to each other and
         finally to the backing store
    """

    def __init__(self):
        self._end_point = None

    def connect(self, end_point):
        """
            Create a connection with another CachePipe instance.

            @param[in] end_point The CachePipe instance we should starting sending our data to
        """
        if not isinstance(end_point, CachePipe):
            raise TypeError("CachePipe.connect: invalid type passed: %s" % type(end_point))
        self._end_point = end_point

    def attach(self, klass, **args): # dynamix class mixin
        """
           The dynamic loaded mixin class will change the behavior of this
           class by overloading the read and write methods. This way we
           can implement a Caching method or we can implement an interface 
           to the back-end store. 
 
           @param[in] klass class definition
           @param[in] args  arguments to be passed to the constructor of klass
        """
        # check if it is a class derived from CacheImpl
        for k in (klass,) +  klass.__bases__:
            if k is CacheImpl:
                self.__class__ = type('CachePipe', (klass, CachePipe), {})
                klass.__init__(self, **args)
                return
        raise TypeError("CachePipe.attache: invalid type passed: %s" % klass)

    def read(self, key):
        """
            Reads a cache value. 
            Will be overloaded by the Dynamic Mixin klass as passed to the attach method

            @param[in] key  The key identifying the cached data
            @return         the value belonging to the key
        """
        syslog(LOG_DEBUG, "CachePipe.read : %s" % key)
        return self.pipe_read(key)

    def write(self, key, value):
        """
            Writes a cache value
            Will be overloaded by the Dynamic Mixin klass as passed to the attach method

            @param[in]   key     The key identifying the cached data
            @param[out]  value   The value belonging to the key
        """
        syslog(LOG_DEBUG, "CachePipe.write : %s %s" % (key, value))
        self.pipe_write(key, value)

    def pipe_write(self, key, value):
        """
            Writing data to the connected CachePipe.

            @param[in]   key     The key identifying the cached data
            @param[out]  value   The value belonging to the key
        """
        if self._end_point != None:
            self._end_point.write(key, value)

    def pipe_read(self, key):
        """
            Reading data from the connected CachePipe.

            @param[in] key  The key identifying the cached data
            @return         the value belonging to the key
        """
        if self._end_point != None:
            return self._end_point.read(key)
        return None

    def flush(self):
        """
            Calling the flush method of the backing store
        """
        syslog(LOG_DEBUG, "CachePipe.flush")
        if self._end_point != None:
            self._end_point.flush()


def md_cache(store, cache_size, **args):
    front  = CachePipe()
    front.attach(CacheImplWriteBackMemory, cache_size=cache_size)
    back   = CachePipe()
    back.attach(store, **args)
    front.connect(back)
    return front


if __name__ == "__main__":
    # UNIT TESTING CODE !!!

    import sys
    from   syslog import openlog, LOG_PERROR, LOG_USER

    openlog("MongoStore Unit Testing", LOG_PERROR, LOG_USER)
   
    if len(sys.argv) != 2:
         print "specify what to do!"
         sys.exit()

    from TestStore import TestStore

    mdc = CachePipe()
    mdc.attach(CacheImplWriteBackMemory, cache_size=3)  # Made this node a write-back caching node (with nice small cache size for testing...)
    mdb = CachePipe()
    mdb.attach(TestStore)
    mdc.connect(mdb) # hook our cache to TestStore

    if sys.argv[1] == "read":
        print "Expect cache miss"
        print mdc.read("001")
        print "Read from cache" 
        # secretly modify TestScore and verify that the cached value is returned!!!
        mdb._data["001"] = 666
        value = mdc.read("001")
        print value
        if value == 666:
            print "BUG!!!!!"
        print "Expect cache miss"
        print mdc.read("002")
        print "Expect cache miss"
        print mdc.read("003")
        print "Expect cache miss"
        print mdc.read("004")
        print mdc.read("003")
        # cache size is 3 and "001" should be out of the cache. It doesn't have the dirty bit set 
        # because it was NOT modified thru the Cache object, so the next time it shoudl return 666!
        value =  mdc.read("001")
        if value != 666:
            print "BUG!!!!!"
        print value
    elif sys.argv[1] == "write":
        print "Expect cache miss"
        mdc.write("001", 101)
        print "Expect data store not updated", mdb._data["001"]
        print "Expect cache miss"
        mdc.write("002", 102)
        print "Expect cache miss"
        mdc.write("003", 103)
        print "Expect cache miss and key 101 moved out of cache and written to store"
        mdc.write("004", 101)
        print "001:", mdb._data["001"]
        if mdb._data["001"] != 101:
            print "BUG!!!!!"
    elif sys.argv[1] == "complexobjects":
        class AKlass:
            def __init__(self):
                self._values = [1, "hello", (1,2,3)]
            def change(self):
                self._values[1] = "bye"
            def printit(self):
                print self._values

        aklass = AKlass()
        
        mdc.write("001", aklass)
        aklass.change()
        mdc.write("002", aklass)
        bklass = mdc.read("001")
        bklass = mdc.read("001")
        cklass = mdc.read("002")

        aklass.printit()
        bklass.printit()
        cklass.printit()
    else:
        print "Unknown task"         
    
