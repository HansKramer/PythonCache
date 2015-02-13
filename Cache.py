#! /usr/bin/env python
#
# \brief     Cache Class
# \author    Hans Kramer
# \version   0.1
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
        self._cache_fifo  = []
        self._cache_max   = cache_size
        
    def read(self, key):
        """
            Reads a cache value

            @param[in] key  The key identifying the cached data
            @return         the value belonging to the key
        """
        syslog(LOG_DEBUG, "CacheImplWriteBack.read : %s" % key)
        if not self._cache_store.has_key(key): # cache miss
            syslog(LOG_DEBUG, "cache miss for %s" % key)
            if len(self._cache_fifo) >= self._cache_max: #free a cache block
                old_key = self._cache_fifo.pop(0)
                if self._cache_dirty[old_key]:
                    self.pipe_write(old_key, cPickle.loads(self._cache_store[old_key]))
                del self._cache_store[old_key]
                del self._cache_dirty[old_key]
            self._cache_fifo += [key,]
            self._cache_store[key] = cPickle.dumps(self.pipe_read(key))
            self._cache_dirty[key] = False
        return cPickle.loads(self._cache_store[key])

    def write(self, key, value):
        """
            Writes a cache value

            @param[in]   key     The key identifying the cached data
            @param[out]  value   The value belonging to the key
        """
        syslog(LOG_DEBUG, "CacheImplWriteBack.write : %s %s" % (key, value))
        if not self._cache_store.has_key(key):  # cache miss
            syslog(LOG_DEBUG, "cache miss for %s" % key)
            if len(self._cache_fifo) >= self._cache_max: #free cache block
                old_key = self._cache_fifo.pop(0)
                if self._cache_dirty[old_key]:
                    self.pipe_write(old_key, cPickle.loads(self._cache_store[old_key]))
                del self._cache_store[old_key]
                del self._cache_dirty[old_key]
            # cache block available
            self._cache_fifo += [key,]
        self._cache_store[key] = cPickle.dumps(value)
        self._cache_dirty[key] = True

    def flush(self):
        """
            Write all dirty cache enties to the store
        """
        CacheImpl.flush(self)
        for key, value in self._cache_dirty.items():
            syslog(LOG_DEBUG, "CacheImplWriteBack.flush key=%s value=%s" % (key, cPickle.loads(self._cache_store[key])))
            if value:
                self.pipe_write(key, cPickle.loads(self._cache_store[key]))


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
            raise TypeError("CachePipe: invalid type passed: %s" % type(end_point))
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
        self.__class__ = type('CachePipe', (klass, CachePipe),{})
        klass.__init__(self, **args)

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


if __name__ == "__main__":
    import sys
    from   syslog import openlog, LOG_PERROR, LOG_USER

    openlog("MongoStore Unit Testing", LOG_PERROR, LOG_USER)
   
    if len(sys.argv) != 2:
         print "specify what to do!"
         sys.exit()

    class TestStore(CacheImpl):
        def __init__(self):
            self.reset()

        def reset(self):
            self._data = {}
            for i in range(10):
                self._data["%03d" % i] = i
            
        def read(self, key):
            return self._data[key]

        def write(self, key, data):
            self._data[key] = data

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
        print mdc.read("002")
        print mdc.read("003")
        print mdc.read("004")
        print mdc.read("003")
        # cache size is 3 and "001" should be out of the cache. It doesn't have the dirty bit set 
        # because it was NOT modified thru the Cache object, so the next time it shoudl return 666!
        value =  mdc.read("001")
        if value != 666:
            print "BUG!!!!!"
    else:
        print "Unknown task"         
    
