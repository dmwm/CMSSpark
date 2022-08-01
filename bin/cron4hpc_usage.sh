#!/bin/bash
set -e

##H cron4hpc_usage.sh
##H    Cron job of hpc_running_cores_and_corehr.py with --iterative option
##H    This cron job produce plots in html pages of for running cores and CoreHr monthly stats of HPC
##H
##H Usage:
##H    cron4hpc_usage.sh <OUTPUT_DIR> <URL_PREFIX>
##H
##H Script arguments:
##H    OUTPUT_DIR        EOS directory that output html files will be written
##H    URL_PREFIX        CernBox EOS folder url link, i.e.: https://cmsdatapop.web.cern.ch/cmsdatapop/hpc_usage
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

# Arg 1, output dir
OUTPUT_DIR="${1}"
# Arg 2, url prefix
URL_PREFIX="${2}"

HTML_TEMPLATE="$script_dir"/../src/html/hpc/html_template.html

if [[ -z $OUTPUT_DIR || -z $URL_PREFIX ]]; then
    echo '[ERROR] One or more parameters are not set'
    echo "[ERROR] 1: ${OUTPUT_DIR} , 2: ${START_DATE} , 3: ${END_DATE} , 4: ${URL_PREFIX}"
    exit 1
else
    echo "[INFO] OUTPUT_DIR: ${OUTPUT_DIR} , URL_PREFIX: ${URL_PREFIX}"
fi

export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
mkdir -p "$script_dir"/../logs/cron4hpc_usage
LOG_FILE=$script_dir/../logs/cron4hpc_usage/$(date +%Y%m%d)

# Setup envs for hadoop and spark. Tested with LCG_98python3
source /cvmfs/sft.cern.ch/lcg/views/LCG_101/x86_64-centos7-gcc8-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix 3.2 spark3

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

echo "output directory: ${OUTPUT_DIR}"

# PySpark job args
py_input_args=(
    --output_dir "$OUTPUT_DIR"
    --url_prefix "$URL_PREFIX"
    --html_template "$HTML_TEMPLATE"
    --iterative
)
spark_submit_args=(
    --master yarn
    --conf spark.executor.memory=8g
    --conf spark.executor.instances=30
    --conf spark.executor.cores=4
    --conf spark.driver.memory=8g
    --conf spark.ui.showConsoleProgress=false
    --conf spark.sql.session.timeZone=UTC
)

# Run
spark-submit \
    "${spark_submit_args[@]}" \
    "$script_dir/../src/python/CMSSpark/hpc_running_cores_and_corehr.py" "${py_input_args[@]}" \
    >>"$LOG_FILE".log 2>&1
