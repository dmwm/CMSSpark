#!/bin/sh

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') $1" >> cron_log.txt
    echo "$(date +'%Y-%m-%d %H:%M:%S') $1"
}

last_non_temp_short_date() {
    # hadoop fs -ls -R $1 - get list of all files and directories in $1 (recursively)
    # grep -E ".*[0-9]{4}/[0-9]{2}/[0-9]{2}$" - get only lines that end with dddd/dd/dd (d - digit)
    # tail -n1 - get the last line (last directory)
    # tail -c11 - get last 11 characters (+1 for newline)
    # sed -e "s/\///g" - replace all / with nothing (delete /)
    result=$(hadoop fs -ls -R $1 | grep -E ".*[0-9]{4}/[0-9]{2}/[0-9]{2}$" | tail -n1 | tail -c11 | sed -e "s/\///g")

    echo $result
    return 0
}

last_non_temp_long_date() {
    # hadoop fs -ls -R $1 - get list of all files and directories in $1 (recursively)
    # sort -n - sort entries by comparing according to string numerical value
    # grep -E ".*year=[0-9]{4}/month=[0-9]{1,2}/day=[0-9]{1,2}$" - get only lines that end with year=dddd/month=dd/day=dd (d - digit)
    # tail -n1 - get the last line (last directory)
    # cut -d "=" -f 2- - get substring from first =
    # sed -E "s/[a-z]*=[0-9]{1}(\/|$)/0&/g" - replace all word=d (d - digit) with 0word=d
    # sed -E "s/[^0-9]//g" - delete all characters that are not digits
    result=$(hadoop fs -ls -R $1 | sort -n | grep -E ".*year=[0-9]{4}/month=[0-9]{1,2}/day=[0-9]{1,2}$" | tail -n1 | cut -d "=" -f 2- | sed -E "s/[a-z]*=[0-9]{1}(\/|$)/0&/g" | sed -E "s/[^0-9]//g")
    echo $result
    return 0
}

log "----------------------------------------------"
log "Starting script"

# Paths of input files in hadoop file system
aaa_dir="/project/monitoring/archive/xrootd/enr/gled"
cmssw_dir="/project/awg/cms/cmssw-popularity/avro-snappy"
eos_dir="/project/monitoring/archive/eos/logs/reports/cms"
jm_dir="/project/awg/cms/jm-data-popularity/avro-snappy"
output_dir="hdfs:///cms/users/jrumsevi/agg/Aggregated"

aaa_date=$(last_non_temp_short_date $aaa_dir)
eos_date=$(last_non_temp_short_date $eos_dir)
cmssw_date=$(last_non_temp_long_date $cmssw_dir)
jm_date=$(last_non_temp_long_date $jm_dir)

log "AAA date $aaa_date"
log "CMSSW date $cmssw_date"
log "EOS date $eos_date"
log "JM date $jm_date"

if [ $aaa_date != "" ] && [ $aaa_date == $cmssw_date ] && [ $cmssw_date == $eos_date ] && [ $eos_date == $jm_date ]; then
    log "All streams are ready for $aaa_date"

    output_dir_with_date=$output_dir"/"${aaa_date:0:4}"/"${aaa_date:4:2}"/"${aaa_date:6:2}
    log "Output directory" $output_dir_with_date
    output_dir_ls=$(hadoop fs -ls $output_dir_with_date | tail -n1)
    if [ "$output_dir_ls" != "" ]; then
        log "Output at $output_dir_with_date exist, will cancel"
    else
        log "Output at $output_dir_with_date does not exist, will run"
    fi

else
    log "Not running script because not all streams are ready"
fi

log "Finishing script"
