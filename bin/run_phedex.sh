#!/bin/bash

hdir=hdfs:///cms/phedex
dates=$(python src/python/CMSSpark/dates.py --range --format="%Y%m%d" --ndays=2 --start=20171231)
dates="20180205 20180206"
for d in $dates; do
    cmd="PYTHONPATH=$PWD/src/python bin/run_spark phedex.py --yarn --fout=$hdir --date=$d"
    echo $cmd
    PYTHONPATH=$PWD/src/python bin/run_spark phedex.py --yarn --fout=$hdir --date=$d
done
