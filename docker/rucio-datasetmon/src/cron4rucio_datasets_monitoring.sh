#!/bin/bash
# shellcheck disable=SC2068
set -e
##H Can only run in K8s, you may modify to run in local by arranging env vars
##H
##H cron4rucio_datasets_monitoring.sh
##H    Cron job of rucio_datasets_monitoring.py which runs Spark job to get Rucio datasets and writes to OpenSearch and HDFS.
##H
##H Arguments:
##H   - keytab              : Kerberos auth file to connect Spark Analytix cluster (cmsmonit)
##H   - hdfs                : (Make sure to have write permission) HDFS_PATH output path for spark job results. Mongoimport will use same path
##H   - conf                : OpenSearch secret file in format of only one line "username:password"
##H   - p1, p2, host, wdir  : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H
##H Usage Example: ./cron4rucio_datasets_monitoring.sh --keytab ./keytab --hdfs /tmp/cmsmonit --conf ./secret --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H How to test:   Give different HDFS output directory
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

unset -v KEYTAB_SECRET HDFS_PATH CONF_FILE PORT1 PORT2 K8SHOST WDIR
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

ES_INDEX="rucio-datasets-monitoring"
ES_HOST="os-cms.cern.ch/os"

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} HDFS_PATH:${HDFS_PATH} CONF_FILE:${CONF_FILE} ES_INDEX:${ES_INDEX} ES_HOST:${ES_HOST} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR}"
util_check_vars HDFS_PATH CONF_FILE PORT1 PORT2 K8SHOST
export PATH=$PATH:/usr/hdp/hadoop/bin
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
function run_spark() {
    hdfs_out_dir=$1

    util4logi "Spark Job for rucio_datasets_monitoring.py starting..."
    spark_submit_args=(
        --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
        --conf spark.shuffle.useOldFetchProtocol=true
        --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
        --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
        --driver-memory=8g --executor-memory=12g
        --packages org.apache.spark:spark-avro_2.12:3.4.0
    )
    py_input_args=(
        --hdfs_out_dir "$hdfs_out_dir" --opensearch_host "$ES_HOST"
        --opensearch_index "$ES_INDEX" --opensearch_secret_file "$CONF_FILE"
    )

    util_spark_submit_with_otel_logs "${spark_submit_args[@]}" "$script_dir/rucio_datasets_monitoring.py" \
        "${py_input_args[@]}"

    hadoop fs -chmod -R o+rx "$hdfs_out_dir"/

    util4logi "Spark results are written to ${hdfs_out_dir}"
    util4logi "Spark Job for rucio_datasets_monitoring.py finished."
}

HDFS_PATH=${HDFS_PATH%/}

hdfs_out="${HDFS_PATH}/$(date +%Y-%m-%d)"
hdfs_out_yesterday="${HDFS_PATH}/$(date -d "yesterday" '+%Y-%m-%d')"

run_spark "$hdfs_out"

hadoop fs -rm -r -f -skipTrash "$hdfs_out_yesterday"
# -------------------------------------------------------------------------------------------------------------- FINISH
duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" "1d" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
