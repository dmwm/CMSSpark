#!/bin/bash
odir=data
fout=$odir/dbs_events.csv
mkdir -p $odir
hdir=hdfs:///cms/dbs_events
echo "Scan: $hdir"
hadoop fs -cat $hdir/part-00000* | head -1 >$fout
header=$(cat $fout)
hadoop fs -cat $hdir/part-* | grep -v $header | sed -e "s,\",,g" >>$fout
