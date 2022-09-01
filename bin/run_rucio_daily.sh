#!/bin/bash
# shellcheck disable=SC1090

ENV_SETUP_SCRIPT="/cvmfs/sft.cern.ch/lcg/views/LCG_96/x86_64-centos7-gcc8-opt/setup.sh"
HADOOP_ENV_SETUP_SCRIPT="/cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh"
script_dir="$(cd "$(dirname "$0")" && pwd)"
# Validation:
if ! klist -s; then
    (echo >&2 -e "This application requires a valid kerberos ticket")
    exit 1
fi

if ! { [ -e "$ENV_SETUP_SCRIPT" ] &&
    [ -e "$HADOOP_ENV_SETUP_SCRIPT" ]; }; then
    (echo >&2 "the specified environment doesn't exists, check the path and try again")
    exit 1
fi

source "$ENV_SETUP_SCRIPT"
source "$HADOOP_ENV_SETUP_SCRIPT" analytix

#In lxplus, when running with acrontab, we need to set the java home
# to a jvm with avanced encryption enabled.
# see https://cern.service-now.com/service-portal/view-request.do?n=RQF1380598

if [ -e "/usr/lib/jvm/java-1.8.0" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
elif ! (java -XX:+PrintFlagsFinal -version 2>/dev/null | grep -E -q 'UseAES\s*=\s*true'); then
    (echo >&2 "This script requires a java version with AES enabled")
    exit 1
fi

# Check if CMSSpark is in the python path,
# otherwise add it assuming the script is in the default location
if ! python -c "import CMSSpark.rucio_daily" 2>/dev/null; then
    export PYTHONPATH="$script_dir/../src/python:$PYTHONPATH"
fi

# Run the script
spark-submit \
    --master yarn \
    --driver-memory 10g \
    --num-executors 30 \
    --executor-memory 6g \
    --packages org.apache.spark:spark-avro_2.11:2.4.3 \
    "$script_dir/../src/python/CMSSpark/rucio_daily.py" "$@"
