#!/bin/bash
##H Script to anonymise data on HDFS
##H Usage: run_hdfs_an.sh <input-dir> <output-dir> <attributes> <log-area>
##H        to run this script you need to fetch CMSSpark and reside in its area
##H        Example:
##H        git clone git@github.com:dmwm/CMSSpark.git
##H        cd CMSSpark; bin/run_hdfs_an.sh ...
##H
##H Options:
##H   input-dir      input area on HDFS
##H   output-dir     output area on HDFS
##H   attributes     comma separated attributes, e.g. user_dn,Country
##H   log-area       log area (default /tmp/hdfs_an
##H

# Check if user is passing least required arguments.
if [ "$#" -lt 3  ]; then
    cat $0 | grep "^##H" | sed -e "s,##H,,g"
    exit 1
fi

hdir=$1
odir=$2
attrs=$3
ldir=$4
if [ "$ldir" == "" ]; then
    ldir=/tmp/hdfs_an
fi
mkdir -p $ldir
log=$ldir/`date '+%Y%m%d'`

# working directory, inside of CMSSpark
wdir=$PWD

# DO NOT EDIT BELOW THIS LINE
addr=cms-comp-monit-alerts@cern.ch

# for Spark 2.X
export PATH=$PATH:/usr/hdp/hadoop/bin
export HADOOP_CONF_DIR=/etc/hadoop/conf

# setup local environment
export PYTHONPATH=$wdir/src/python:$PYTHONPATH
export PATH=$wdir/bin:$PATH

amtool=""
if [ -f /data/cms/bin/amtool ]; then
    amtool=/data/cms/bin/amtool
elif [ -f /cvmfs/cms.cern.ch/cmsmon/amtool ]; then
    amtool=/cvmfs/cms.cern.ch/cmsmon/amtool
fi

# setup to run the script
cmd="$PWD/bin/run_spark hdfs_an.py --yarn --fout=$odir --attrs=$attrs --hdir=$hdir"
echo "Will execute ..."
echo $cmd
msg="Error while executing $cmd on $USER@`hostname` log at $log"

echo "amtool=$amtool"

set -e

trap func exit
# Declare the function
function func() {
    local status=$?
    if [ $status -ne 0 ]; then
        local msg="run_hdfs_an completed with non zero status"
        if [ "$amtool" != "" ]; then
            local expire=`date -d '+1 hour' --rfc-3339=ns | tr ' ' 'T'`
            local urls="http://cms-monitoring.cern.ch:30093 http://cms-monitoring-ha1.cern.ch:30093 http://cms-monitoring-ha2.cern.ch:30093"
            for url in $urls; do
                $amtool alert add run_hdfs_an \
                    alertname=hdfs_an severity=medium tag=cronjob alert=amtool \
                    --end=$expire\
                    --annotation=summary="$msg" \
                    --annotation=date="`date`" \
                    --annotation=hostname="`hostname`" \
                    --annotation=status="$status" \
                    --annotation=command="$cmd" \
                    --annotation=log="$log" \
                    --alertmanager.url=$url
            done
        else
            echo "$msg" | mail -s "alert run_hdfs_an" "$addr"
        fi
    fi
}

time $cmd >> $log 2>&1
