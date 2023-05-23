#!/bin/bash
# shellcheck disable=SC2068
set -e
##H cron4wma_agent_count.sh
##H    Weekly cron job of wma_agent_count_to_opensearch.py
##H    This cron job calculates daily agent count per host and send to OpenSearch es-cms test tenant WEEKLY
##H
##H Example: cron4wma_agent_count.sh --keytab ./keytab --conf ./secret_opensearch.txt --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab             : Kerberos auth file: secrets/keytab
##H   - conf               : OpenSearch secret file in format of only one line "username:password"
##H   - p1, p2, host, wdir : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H How to test: Just send data to different index.
##H
TZ=UTC
START_TIME=$(date +%s)
myname=$(basename "$0")
script_dir="$(cd "$(dirname "$0")" && pwd)"
. "$script_dir"/utils/common_utils.sh

if [ "$1" == "" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    util_usage_help
    exit 0
fi
# weekly: 1w
util_cron_send_start "$myname" "1w"
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH

unset -v KEYTAB_SECRET CONF_FILE PORT1 PORT2 K8SHOST WDIR IS_TEST
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} CONF_FILE:${CONF_FILE} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR} IS_TEST:${IS_TEST}"
util_check_vars KEYTAB_SECRET CONF_FILE PORT1 PORT2 K8SHOST WDIR
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"

# ----------------------------------------------------------------------------------------------------------------- RUN
spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
    --conf spark.shuffle.useOldFetchProtocol=true
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --driver-memory=8g --executor-memory=8g --packages org.apache.spark:spark-avro_2.12:3.4.0
)
# -------------------------------------------------------------------------------------------------------------- PARAMS
# 3 days of buffer time for WMArchive HDFS data. Because we'll run WEEKLY: 10 days - 3 days = 7 days = 1 week
_start_date=$(date -d "10 days ago" +%Y-%m-%d)
_end_date=$(date -d "3 days ago" +%Y-%m-%d)
ES_INDEX="test-wmarchive-agent-count"
ES_HOST="es-cms1.cern.ch/es"

util4logi "${myname} Spark Job is starting... between input dates of ${_start_date} - ${_end_date}"
# ---------------------------------------------------------------------------------------------------------------------

py_input_args=(--start_date "$_start_date" --end_date "$_end_date" --es_host "$ES_HOST" --es_index "$ES_INDEX" --es_secret_file "$CONF_FILE")

# Run
spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/wma_agent_count_to_opensearch.py" "${py_input_args[@]}" 2>&1

duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" "1w" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
