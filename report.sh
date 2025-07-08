#!/bin/bash
#set -x
logDir="$HOME/tools/SolarEdge/logs"
log=$logDir/report.$(/bin/date +%F-%T | /usr/bin/tr : .);
reportDir="$HOME/SynologyDrive/Reports.Daily/"
$HOME/tools/SolarEdge/report.py > $log 2>&1
cp -p $log $reportDir/SolarEdge.txt
cp -p $log $reportDir/All/SolarEdge.$(basename -- "$log").txt
cat $log
# keep only the newest
REMOVE=$(ls -t $logDir/report* | sed 1,20d)
if [ -n "$REMOVE" ]; then
    /bin/rm $REMOVE
fi
me='jim.mollmann@gmail.com'
mailTemp="/tmp/SolarEdge.mail"
####(echo -e "Subject: SolarEdge Usage Report: $(date)\n"; cat $log) | sendmail $me

