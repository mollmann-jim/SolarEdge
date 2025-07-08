#!/bin/bash
#set -x
logDir="$HOME/tools/SolarEdge/logs"
log=$logDir/collectLayout.$(/bin/date +%F-%T | /usr/bin/tr : .);
$HOME/tools/SolarEdge/collectLayout.py > $log 2>&1
cat $log
# keep only the newest
REMOVE=$(ls -t $logDir/collectLayout* | sed 1,20d)
if [ -n "$REMOVE" ]; then
    /bin/rm $REMOVE
fi
