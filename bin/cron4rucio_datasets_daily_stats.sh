#!/bin/bash
set -e

##H rucio_datasets_daily_stats.sh
##H    Cron job of rucio_datasets_daily_stats.py, sends data to MONIT via StompAMQ
##H
##H Usage:
##H    rucio_datasets_daily_stats.sh <CREDS_JSON>
##H
##H Script arguments:
##H    CREDS_JSON        AMQ and MONIT credentials
##H

# help definition
if [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ] || [ "$1" == "help" ]; then
    grep "^##H" <"$0" | sed -e "s,##H,,g"
    exit 1
fi

# Arg 1
CREDS_JSON="${1:-creds.json}"
if [ ! -f "$CREDS_JSON" ]; then
    echo "${CREDS_JSON} does not exists."
    exit 1
fi

# Prepare log file
script_dir="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"

# Create logs dir if it does not exists
mkdir -p "$script_dir"/../logs
LOG_FILE="$script_dir"/../logs/cron4rucio_datasets_daily_stats-$(date +%Y%m%d)

# Below environments and python modules that may change in future when upstream releases published

# LCG_101 release setup gives py3.6 py3.9 driver-worker compatibility error.
#   Virtual environment will be used until the issue is solved in upstream
#   Do not run `source /cvmfs/sft.cern.ch/lcg/views/LCG_101/x86_64-centos7-gcc8-opt/setup.sh`

# We need to run below venv creation every time even though venv exists, since it also setup hadoop requirements.
# shellcheck disable=SC2129
{
    /cvmfs/sft.cern.ch/lcg/releases/Python/3.6.5-f74f0/x86_64-centos7-gcc8-opt/bin/python3.6 -m venv venv_pyspark
    source venv_pyspark/bin/activate
    pip install --upgrade pip
    pip install --no-cache-dir pandas click pyspark
} >>"$LOG_FILE".log

source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix 3.2 spark3 >>"$LOG_FILE".log 2>&1

# Create zip file of stomp.py 7.0.0
{
    pip install --no-cache-dir -t stomp-v700 https://github.com/jasonrbriggs/stomp.py/archive/refs/tags/v7.0.0.zip
    cd stomp-v700
    zip -r ../stomp-v700.zip .
    cd ..
    rm -rf stomp-v700
} >>"$LOG_FILE".log 2>&1

# Create zip file of CMSMonitoring specific branch/version/tag
{
    svn export https://github.com/mrceyhun/CMSMonitoring.git/branches/f-stomp-v6+/src/python/CMSMonitoring
    zip -r CMSMonitoring.zip CMSMonitoring/*
    rm -rf CMSMonitoring
} >>"$LOG_FILE".log 2>&1

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
    echo "There is no valid ticket yet"
    exit 1
fi

export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH

# shellcheck disable=SC2054
spark_submit_args=(
    --master yarn
    --conf spark.driver.extraClassPath='/eos/project/s/swan/public/hadoop-mapreduce-client-core-2.6.0-cdh5.7.6.jar'
    --conf spark.executor.memory=4g
    --conf spark.executor.instances=30
    --conf spark.executor.cores=4
    --conf spark.driver.memory=4g
    --conf spark.ui.showConsoleProgress=false
    --packages org.apache.spark:spark-avro_2.12:3.2.1
    --py-files CMSMonitoring.zip,stomp-v700.zip
)

py_input_args=(
    --creds "$CREDS_JSON"
    --stomp_zip stomp-v700.zip
    --cmsmonit_zip CMSMonitoring.zip
    --amq_batch_size 1000
)

# Run
spark-submit \
    "${spark_submit_args[@]}" \
    "${script_dir}/../src/python/CMSSpark/rucio_datasets_daily_stats.py" \
    "${py_input_args[@]}" >>"$LOG_FILE".log 2>&1
