#!/bin/bash
set -e

##H hpc_running_cores_and_corehr.py
##H    Cron job of hpc_running_cores_and_corehr.py
##H    This cron job produce plots in html pages of for running cores and CoreHr monthly stats of HPC
##H
##H Usage:
##H    cron4hpc_monthly_stats.sh <OUTPUT_DIR> <LAST_N_MONTHS>
##H
##H Script arguments:
##H    OUTPUT_DIR        Directory that output html files will be written
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
mkdir -p "$script_dir"/../logs/cron4hpc_monthly
LOG_FILE=$script_dir/../logs/cron4hpc_monthly/$(date +%Y%m%d)

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
OUTPUT_DIR="${1:-/eos/user/c/cmsmonit/www/hpc_monthly}"
# Arg 2, default is 19 months
LAST_N_MONTHS="${2:-19}"

echo "output directory: ${OUTPUT_DIR}"

# PySpark job args
py_input_args=(
    --output_dir "$OUTPUT_DIR"
    --last_n_months "$LAST_N_MONTHS"
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
    "$script_dir/../src/python/CMSSpark/hpc_running_cores_and_corehr.py" "${py_input_args[@]}" \
    >>"$LOG_FILE".log 2>&1
