#!/bin/bash
# shellcheck disable=SC2068
set -e
##H cron4rucio_daily.sh
##H    Daily Rucio dumps
##H Usage: cron4rucio_daily.sh <ARGS>
##H Example :
##H    cron4rucio_daily.sh --keytab ./keytab --output <HDFS> --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab               : Kerberos auth file: secrets/keytab
##H   - output               : !!! *HDFS* !!! directory, i.e.: /cms/rucio_daily
##H   - p1, p2, host, wdir   : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H How to test:
##H   - Just provide test hdfs directory as output directory. No need a test run, spark job will process only data of 1 day.
##H
TZ=UTC
START_TIME=$(date +%s)
myname=$(basename "$0")
script_dir="$(cd "$(dirname "$0")" && pwd)"
. /data/utils/common_utils.sh

if [ "$1" == "" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    util_usage_help
    exit 0
fi
util_cron_send_start "$myname" "1d"

unset -v KEYTAB_SECRET OUTPUT_DIR PORT1 PORT2 K8SHOST WDIR
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} OUTPUT_DIR:${OUTPUT_DIR} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR} IS_TEST:${IS_TEST}"
util_check_vars PORT1 PORT2 K8SHOST
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"

# ----------------------------------------------------------------------------------------------------------------- RUN
util4logi "${myname} Spark Job is starting..."
util4logi "output directory: ${OUTPUT_DIR}"
current_date="$(date +%Y-%m-%d)"

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.shuffle.useOldFetchProtocol=true
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --driver-memory=8g --executor-memory=8g --packages org.apache.spark:spark-avro_2.12:3.4.0
)
py_input_args=(--verbose --output "$OUTPUT_DIR" --fdate "$current_date")

# log all to stdout
spark-submit "${spark_submit_args[@]}" "$script_dir/rucio_daily.py" "${py_input_args[@]}" 2>&1

duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" "1d" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
