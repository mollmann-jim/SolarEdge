#!/bin/bash
#set -x
logDir="/home/jim/tools/SolarEdge/logs"
log=$logDir/collectDaily.$(/bin/date +%F-%T | /bin/tr : .);
/home/jim/tools/SolarEdge/collectDaily.py > $log 2>&1
cat $log
# keep only the newest
REMOVE=$(ls -t $logDir/collectDaily* | sed 1,20d)
if [ -n "$REMOVE" ]; then
    /bin/rm $REMOVE
fi
