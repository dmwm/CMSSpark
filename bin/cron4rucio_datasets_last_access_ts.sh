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

export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
mkdir -p "$script_dir"/../logs/cron4rucio
LOG_FILE=$script_dir/../logs/cron4rucio/datasets_last_access_ts-$(date +%Y%m%d)

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
OUTPUT_DIR="${1:-/eos/user/c/cmsmonit/www/rucio}"
STATIC_HTML_DIR="$script_dir"/../src/html/rucio_datasets_last_access_ts
RSES_PICKLE_DIR="$script_dir"/../static/rucio/rses.pickle

main() {
    local disk_or_tape=$1
    local min_tb_limit=$2

    echo "output directory: ${OUTPUT_DIR}"
    echo "rses pickle dir: ${RSES_PICKLE_DIR}"
    echo "disk or tape: ${disk_or_tape}"
    echo "min tb limit: ${min_tb_limit}"

    # PySpark job args
    py_input_args=(
        "$disk_or_tape"
        --static_html_dir "$STATIC_HTML_DIR"
        --output_dir "$OUTPUT_DIR"
        --rses_pickle "$RSES_PICKLE_DIR"
        --min_tb_limit "$min_tb_limit"
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

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Run for Disk"
main --disk 1 >>"$LOG_FILE".disk.log 2>&1
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Run for Tape"
main --tape 10 >>"$LOG_FILE".tape.log 2>&1
