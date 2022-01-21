#!/bin/bash
set -e

# TODO fix checks

##H cron4rucio_datasets_last_access_ts.sh
##H    Cron job of rucio_datasets_last_access_ts.py
##H    This cron job produce html pages of datasets not read since N months using Rucio table dumps in hdfs
##H
##H Usage:
##H    cron4rucio_datasets_last_access_ts.sh <OUTPUT_DIR>
##H
##H Script arguments:
##H    OUTPUT_DIR        Directory that output html files will be written
##H
##H Prerequisites:
##H    Please create and move rses.pickle to $script_dir/../static/rucio/rses.pickle
##H        - See how to crete it: $script_dir/../static/rucio/rse_name_id_map_pickle.py
##H    This script requires partial html files
##H        - They reside in ~/CMSSpark/src/html/rucio_datasets_last_access_ts
##H

# help definition
if [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ] || [ "$1" == "help" ] || [ "$1" == "" ]; then
    grep "^##H" <"$0" | sed -e "s,##H,,g"
    exit 1
fi

# Prepare log file
script_dir="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"
mkdir -p "$script_dir"/../logs/cron4rucio
LOG_FILE=$script_dir/../logs/cron4rucio/datasets_last_access_ts-$(date +%Y%m%d).log

main() {
    # Setup envs for hadoop and spark. Tested with LCG_98python3
    source /cvmfs/sft.cern.ch/lcg/views/LCG_98python3/x86_64-centos7-gcc8-opt/setup.sh
    source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix

    # Check JAVA_HOME is set
    if [ -n "$JAVA_HOME" ]; then
        if [ -e "/usr/lib/jvm/java-1.8.0" ]; then
            export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
        elif ! (java -XX:+PrintFlagsFinal -version 2>/dev/null | grep -E -q 'UseAES\s*=\s*true'); then
            (echo >&2 "This script requires a java version with AES enabled")
            exit 1
        fi
    fi

    # Check Kerberos ticket
    if ! klist -s; then
        echo "There is not valid ticket yet"
        exit 1
    fi

    # Arg 1
    output_dir="${1:-/eos/user/c/cuzunogl/www/rucio_test}"

    static_html_dir="$script_dir"/../src/html/rucio_datasets_last_access_ts
    rses_pickle_dir="$script_dir"/../static/rucio/rses.pickle

    # PySpark job args
    py_input_args=(
        --static_html_dir "$static_html_dir"
        --output_dir "$output_dir"
        --rses_pickle "$rses_pickle_dir"
        --min_tb_limit 1
    )
    spark_submit_args=(
        --master yarn
        --conf spark.driver.extraClassPath='/eos/project/s/swan/public/hadoop-mapreduce-client-core-2.6.0-cdh5.7.6.jar'
        --conf spark.executor.memory=4g
        --conf spark.executor.instances=30
        --conf spark.executor.cores=4
        --conf spark.driver.memory=4g
        --conf spark.ui.showConsoleProgress=false
        --packages org.apache.spark:spark-avro_2.11:2.4.3
    )

    # Run
    spark-submit \
        "${spark_submit_args[@]}" \
        "$script_dir/../src/python/CMSSpark/rucio_datasets_last_access_ts.py" "${py_input_args[@]}"
}

# !!! === Check functions should return nonzero values on fail === !!!
#check_1() {
#    # Check out file exists
#    if [ ! -f "$result_file" ]; then
#        echo "Check FAILED: File does not exist: ${result_file}"
#        exit 1
#    fi
#}
#check_2() {
#    # Check file is created in last 2 hours. ot: older than
#    touch -d '-2 hour' a_file_modified_2_h_ago
#    if [ "$result_file" -ot a_file_modified_2_h_ago ]; then
#        echo "Check FAILED: File is not new."
#        rm a_file_modified_2_h_ago
#        exit 1
#    fi
#    rm a_file_modified_2_h_ago
#}
#
#check_3() {
#    # Check file size is greater than 10000Bytes
#    min_size=10000
#    file_size=$(stat -c%s "$result_file")
#    if ! (("$file_size" > "$min_size")); then
#        echo "Check FAILED:File size seems not appropriate: ${file_size} Bytes"
#        exit 1
#    fi
#}
#
#check_success() {
#    # Script produce this output html file
#    result_file="$output_dir"/rucio_datasets_last_access.html
#    # check_1 && check_2 && check_3
#}

main "$@" >>"$LOG_FILE" 2>&1
# check_success >>"$LOG_FILE" 2>&1
