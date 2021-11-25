#!/bin/bash

hdir=hdfs:///cms/dbs_condor
dates=$(python src/python/CMSSpark/dates.py --range --format="%Y%m%d" --ndays=24 --start=20180124)
for d in $dates; do
    cmd="PYTHONPATH=$PWD/src/python bin/run_spark dbs_condor.py --yarn --fout=$hdir --date=${d}"
    echo $cmd
    PYTHONPATH=$PWD/src/python bin/run_spark dbs_condor.py --yarn --fout=$hdir --date=$d
done
