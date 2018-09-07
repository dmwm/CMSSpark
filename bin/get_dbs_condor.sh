#!/bin/bash
dates="01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31"
months="01 02 03 04 05 06 07 08 09 10 11 12"
months="01 02 03 04 05 06 07 08"
year=2018
mkdir -p out
hdir=hdfs:///cms/dbs_condor
for m in $months; do
    for d in $dates; do
        idir=dataset/$year/$m/$d
        fout="out/dataset-${year}${m}${d}.csv"
        echo "Scan: $hdir/$idir"
        hadoop fs -cat $hdir/$idir/part-00000 | head -1 > $fout
#        head -1 $idir/part-00000 > $fout
        header=`cat $fout`
        hadoop fs -cat $hdir/$idir/part-* | grep -v $header | sed -e "s,\",,g" >> $fout
#        cat $idir/part-* | grep -v $header | sed -e "s,\",,g" >> $fout
    done
done
