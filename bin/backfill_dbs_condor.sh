#!/bin/bash -l
# This script check if a file for each date exists and, if not, it creates it. 
# This script requieres a hadoop environment. 

#By default it will look 7 days to the past from yesterday. 
start=${1:-$(date -d "yesterday 13:00" +%Y%m%d)}
ndays=${2:-7}
ldir="${3:-/data/cms/pop-data}"
echo $ldir
hdir=hdfs:///cms/dbs_condor
DIR=$(dirname "${BASH_SOURCE[0]}")
DIR=$(realpath "${DIR}")
test -d src/python/CMSSpark || cd "$DIR/.."
dates=$(python src/python/CMSSpark/dates.py --range --format="%Y%m%d" --ndays="$ndays" --start="$start")

for d in $dates; do
    date_f=$(date -d "$d" +%Y/%m/%d)
    if ! hadoop fs -test -e "$hdir/dataset/$date_f"
    then
        cmd="PYTHONPATH=$PWD/src/python bin/run_spark dbs_condor.py --yarn --fout=$hdir --date=${d}"
        echo "$cmd"
        PYTHONPATH=$PWD/src/python bin/run_spark dbs_condor.py --yarn --fout="$hdir" --date="$d"
    fi
    odir="$ldir/${date_f:0:4}/data"
    mkdir -p "$odir" || (>&2 echo "Its not possible to create the directory"; exit 1)
    ofile="$odir/dataset-$d.csv"
    idir="$hdir/dataset/$date_f"
    if [ ! -f "$ofile" ]
    then
	hadoop fs -cat "$idir/part-00000*" 2>/dev/null | head -1 > "$ofile"
	header=$(cat "$ofile")
	hadoop fs -cat "$idir/part-*" 2>/dev/null | grep -v "$header" | sed -e "s,\",,g" >> "$ofile"
    fi
done
