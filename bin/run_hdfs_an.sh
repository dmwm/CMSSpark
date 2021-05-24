#!/bin/bash
# this script is designed to be used in crontab, i.e. with full path
# adjust script to put your desired notification address if necessary

#addr=cms-popdb-alarms@cern.ch
addr=vkuznet@gmail.com

# DO NOT EDIT BELOW THIS LINE
# for Spark 2.X
export PATH=$PATH:/usr/hdp/hadoop/bin
export HADOOP_CONF_DIR=/etc/hadoop/conf

# find out which date we should use to run the script
year=`date +'%Y'`
month=`date +'%m'`
day=`date +'%d'`
hdir=hdfs:///project/monitoring/archive/cmssw_pop/raw/metric
tmpDirs=`hadoop fs -ls ${hdir}/$year/$month | grep tmp$ | awk '{print $8}' | sed -e "s,\.tmp,,g" -e "s,${hdir},,g"`
pat=`echo $tmpDirs | tr ' ' '|'`
lastSnapshot=`hadoop fs -ls ${hdir}/$year/$month | egrep -v ${pat} | tail -1 | awk '{print $8}'`
#echo "Last available snapshot"
#echo $lastSnapshot
date=`echo $lastSnapshot | sed -e "s,${hdir},,g" -e "s,/,,g"`
odir=hdfs:///cms/anonymized

# set log area and environment
me=$(dirname "$0")
wdir=`echo $me | sed -e "s,/bin,,g"`
mkdir -p $wdir/logs
log=$wdir/logs/hdfs_an-`date +%Y%m%d`.log
export PYTHONPATH=$wdir/src/python:$PYTHONPATH
export PATH=$wdir/bin:$PATH

echo "Start job for $year/$month/$day ..." >> $log 2>&1
hadoop fs -ls ${hdir}/$year/$month >> $log 2>&1
echo "Date to run: $date" >> $log 2>&1

# check if we got a date to run
if [ -z "${date}" ]; then
    echo "No date to run" >> $log 2>&1
    exit
fi
dotmp=`echo $date | grep tmp`
if [ -n "${dotmp}" ]; then
    echo "Date ends with tmp" >> $log 2>&1
    exit
fi

# setup to run the script
attrs="user_dn"
#cmd="$wdir/bin/run_spark hdfs_an.py --yarn --fout=$odir --date=$date --hdir=$hdir/$year/$month/$day"
cmd="$wdir/bin/run_spark hdfs_an.py --yarn --fout=$odir --attrs=$attrs --hdir=$hdir/$year/$month/22"
echo "Will execute ..."
echo $cmd
msg="Error while executing $cmd on $USER@`hostname` log at $log"
set -e

$cmd >> $log 2>&1
