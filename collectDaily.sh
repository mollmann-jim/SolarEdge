#!/bin/bash
#set -x
#logDir="$HOME/tools/SolarEdge/logs"
logDir="$HOME/tools/SolarEdge.2/logs"
cd "$HOME/tools/SolarEdge.2/"
log=$logDir/collectDaily.$(/bin/date +%F-%T | /usr/bin/tr : .);
$HOME/tools/SolarEdge.2/collectDaily.py "$@" > $log 2>&1
# keep only the newest
REMOVE=$(ls -t $logDir/collectDaily* | sed 1,20d)
if [ -n "$REMOVE" ]; then
    /bin/rm $REMOVE
fi
