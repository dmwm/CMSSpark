#!/bin/bash
# This script contains functions essential for testing critical cron jobs.



# This function exits 0 when file exists, is smaller than specified treshold and was modified during last N seconds (Unix time).
#arg1: file path
#arg2: time treshold in seconds
#arg3: size treshold in bytes

function check_file_status {

    # exit 2 if file doesn't exist
    if [ ! -f $1 ]; then
        exit 2
    fi

    # exit 1 if file was modified before specified time
    CURTIME=$(date +%s)
    FILETIME=$(stat $1 -c %Y)
    TIMEDIFF=$(expr $CURTIME - $FILETIME)

    if [ $TIMEDIFF -gt $2 ]; then
        exit 1
    fi

    # exit 1 if file smaller than treshold
    FILESIZE=$(stat -c%s "$1")
    if [ $FILESIZE -lt $3 ]; then
        exit 1
    fi
}



# This function exits 0 when amount of ES entries is bigger than given value in a specified index.
#arg1: Grafana access token
#arg2: ES index (e.g. monit_es_condor)
#arg3: ES index time extension (e.g. RecordTime/timestamp)
#arg4: minimum number of documents to compare

function check_cmp_es_hits {
    # get monit if it doesn't exist
    # path is subject to change
    if [ ! -f /tmp/monit ]; then
        curl -LJO https://github.com/dmwm/CMSMonitoring/releases/latest/download/cmsmon-tools.tar.gz
        tar -xvzf cmsmon-tools.tar.gz
        cp cmsmon-tools/monit /tmp/
        rm -rf cmsmon-tools*
    fi

    TIME_EXT=$3
    BASE_QUERY=$(echo '{"query":{"range":{"data.$TIME_EXT":{"gte":"now-1h"}}}}' | sed -e "s/\$TIME_EXT/$TIME_EXT/g")
    JQ_RESULT=$(exec /tmp/monit -token $1 -query=$BASE_QUERY -dbname=$2 -esapi=_count | jq -re '.count')
    if [ $JQ_RESULT -lt $4 ]; then
        exit 1
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
        # amount of time before last modification is bigger than specified treshold
        # -> file wasn't modified during last $2 seconds
        exit 1
    fi
}

"$@"
