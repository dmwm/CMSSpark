#!/bin/bash
# This script contains functions essential for checking result of critical cron jobs




# This function exits 0 when file exists, is smaller than specified treshold and was modified during last N seconds
#arg1: file path
#arg2: time treshold in seconds
#arg3: size treshold in bytes

function check_file_status() {

    # exit 2 if file doesn't exist
    if [ ! -f $1 ]; then
        exit 2
    fi

    # exit 1 if file was modified before specified time
    CURTIME=$(date +%s)
    FILETIME=$(stat $1 -c %Y)
    TIMEDIFF=$(expr $CURTIME - $FILETIME)

    if [ $TIMEDIFF -lt $2 ]; then
        exit 1
    fi

    # exit 1 if file smaller than treshold
    FILESIZE=$(stat -c%s "$1")
    if [ $FILESIZE -lt $3 ]; then
        exit 1
    fi
}



