#!/bin/bash
#set -x
logDir="/home/jim/tools/SolarEdge/logs"
log=$logDir/report.$(/bin/date +%F-%T | /bin/tr : .);
/home/jim/tools/SolarEdge/report.py > $log 2>&1
cat $log
# keep only the newest
REMOVE=$(ls -t $logDir/report* | sed 1,20d)
if [ -n "$REMOVE" ]; then
    /bin/rm $REMOVE
fi
me='foo@bar.com'
mailTemp="/tmp/SolarEdge.mail"
(echo -e "Subject: SolarEdge Usage Report: $(date)\n"; cat $log) | sendmail $me

