#!/bin/bash
# This script contains functions essential for testing critical cron jobs.



# This function exits 0 when file exists, is smaller than specified treshold and was modified during last N seconds (Unix time).
#arg1: file path
#arg2: time treshold in seconds
#arg3: size treshold in bytes

function check_file_status {

    if [ ! -f $1 ]; then
        # file doesn't exist
        exit 210
    fi

    CURTIME=$(date +%s)
    FILETIME=$(stat $1 -c %Y)
    TIMEDIFF=$(expr $CURTIME - $FILETIME)

    if [ $TIMEDIFF -gt $2 ]; then
        # file wasn't modified during last $2 seconds
        exit 210
    fi

    FILESIZE=$(stat -c%s "$1")
    if [ $FILESIZE -lt $3 ]; then
        # file is smaller than specified size
        exit 210
    fi
}



# This function exits 0 when amount of ES entries is bigger than given value in a specified index.
#arg1: monit path
#arg2: Grafana access token
#arg3: ES index (e.g. monit_es_condor)
#arg4: ES index time extension (e.g. RecordTime/timestamp)
#arg5: minimum number of documents to compare

function check_es_hits {
    # get monit if it doesn't exist
    DIR="$(dirname "$1")"
    if [ ! -f $1 ]; then
        curl -LJO https://github.com/dmwm/CMSMonitoring/releases/latest/download/cmsmon-tools.tar.gz
        tar -xvzf cmsmon-tools.tar.gz
        cp cmsmon-tools/monit $DIR
        rm -rf cmsmon-tools*
    fi

    TIME_EXT=$4
    BASE_QUERY=$(echo '{"query":{"range":{"data.$TIME_EXT":{"gte":"now-1h"}}}}' | sed -e "s/\$TIME_EXT/$TIME_EXT/g")
    JQ_RESULT=$(exec /tmp/monit -token $2 -query=$BASE_QUERY -dbname=$3 -esapi=_count | jq -re '.count')
    if [ $JQ_RESULT -lt $5 ]; then
        # ES contains less entries than specified
        exit 210
    fi
}


# This function exits 0 when specified file was modified during last N seconds (Unix time).
#arg1: hdfs directory and file
#arg2: time treshold in seconds
function check_hdfs_mod_date {

    CURTIME=$(date +%s)
    # convert milliseconds to seconds
    FILETIME=$(expr $(hadoop fs -stat "%Y" $1) / 1000 )
    TIMEDIFF=$(expr $CURTIME - $FILETIME)

    if [ $TIMEDIFF -gt $2 ]; then
        # file wasn't modified during last $2 seconds
        exit 210
    fi
}

"$@"
