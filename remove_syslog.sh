#! /bin/bash
#
#  Remove syslog statements for source code
#

for file in Cache.py MongoStore.py ; do
    sed -i.bu '/syslog(/d' $file
done
