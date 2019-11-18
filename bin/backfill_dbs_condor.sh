#!/bin/bash
# This script check if a file for each date exists and, if not, it creates it. 
# This script requieres a hadoop environment. 

#By default it will look 7 days to the past from yesterday. 
start=${1:-$(date -d "yesterday 13:00" +%Y%m%d)}
ndays=${2:-7}
hdir=hdfs:///cms/dbs_condor
DIR=$(dirname "${BASH_SOURCE[0]}")
DIR=$(realpath "${DIR}")
test -d src/python/CMSSpark || cd "$DIR/.."
dates=$(python src/python/CMSSpark/dates.py --range --format="%Y%m%d" --ndays=$ndays --start=$start)

for d in $dates; do
    date_f=$(date -d "$d" +%Y/%m/%d)
    if ! hadoop fs -test -e "$hdir/dataset/$date_f"
    then    
        cmd="PYTHONPATH=$PWD/src/python bin/run_spark dbs_condor.py --yarn --fout=$hdir --date=${d}"
        echo $cmd
        PYTHONPATH=$PWD/src/python bin/run_spark dbs_condor.py --yarn --fout=$hdir --date=$d
    fi
done
