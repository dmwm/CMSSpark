#!/bin/bash
# shellcheck disable=SC2068
set -e
##H Can only run in K8s, you may modify to run in local by arranging env vars
##H
##H cron4rucio_spark2hdfs.sh
##H    Cron job of rucio_all_datasets.py and rucio_all_detailed_datasets.py which runs Spark job to get Rucio datasets and writes to HDFS directory.
##H
##H Arguments:
##H   - keytab              : Kerberos auth file to connect Spark Analytix cluster (cmsmonit)
##H   - p1, p2, host, wdir  : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H
##H Usage Example: ./cron4rucio_spark2hdfs.sh --keytab ./keytab --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H How to test:   Use different Kerberos user(default uses the user's tmp HDFS directory) or give different HDFS output directory
##H
TZ=UTC
START_TIME=$(date +%s)
myname=$(basename "$0")
script_dir="$(cd "$(dirname "$0")" && pwd)"
. "$script_dir"/../utils/common_utils.sh

if [ "$1" == "" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    util_usage_help
    exit 0
fi
util_cron_send_start "$myname"
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
unset -v KEYTAB_SECRET PORT1 PORT2 K8SHOST WDIR IS_TEST

# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR}"
util_check_vars PORT1 PORT2 K8SHOST
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
# arg1: python file [rucio_all_datasets.py or rucio_all_detailed_datasets.py]
# arg2: hdfs output directory
# arg3: hdfs output directory of yesterday
function run_spark() {
    # Required for Spark job in K8s
    spark_py_file=$1
    hdfs_out_dir=$2
    yesterday_hdfs_out_dir=$3

    util4logi "Spark Job for ${spark_py_file} starting..."
    export PYTHONPATH=$script_dir/../../src/python:$PYTHONPATH
    spark_submit_args=(
        --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
        --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
        --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
        --driver-memory=8g --executor-memory=8g
        --packages org.apache.spark:spark-avro_2.12:3.3.1
    )
    py_input_args=(--hdfs_out_dir "$hdfs_out_dir")

    # Run
    spark-submit "${spark_submit_args[@]}" "${script_dir}/../../src/python/CMSSpark/${spark_py_file}" \
        "${py_input_args[@]}" 2>&1
    # ------------------------------------------------------------------------------------ POST OPS
    # Give read access to new dumps for all users
    hadoop fs -chmod -R o+rx "$hdfs_out_dir"/
    # Delete yesterdays dumps
    hadoop fs -rm -r -f -skipTrash "$yesterday_hdfs_out_dir"

    util4logi "Spark results are written to ${hdfs_out_dir}"
    util4logi "Spark Job for ${spark_py_file} finished."
}

###################### Run datasets
# Arrange a temporary HDFS directory that current Kerberos user can use for datasets collection
datasets_hdfs_out="/tmp/${KERBEROS_USER}/rucio_ds_for_mongo/$(date +%Y-%m-%d)"
datasets_hdfs_out_yesterday="/tmp/${KERBEROS_USER}/rucio_ds_for_mongo/$(date -d "yesterday" '+%Y-%m-%d')"
run_spark "rucio_all_datasets.py" "$datasets_hdfs_out" "$datasets_hdfs_out_yesterday" 2>&1

###################### Run detailed datasets
# Arrange a temporary HDFS directory that current Kerberos user can use for detailed_datasets collection
detailed_datasets_hdfs_out="/tmp/${KERBEROS_USER}/rucio_detailed_ds_for_mongo/$(date +%Y-%m-%d)"
detailed_datasets_hdfs_out_yesterday="/tmp/${KERBEROS_USER}/rucio_detailed_ds_for_mongo/$(date -d "yesterday" '+%Y-%m-%d')"
run_spark "rucio_all_detailed_datasets.py" "$detailed_datasets_hdfs_out" "$detailed_datasets_hdfs_out_yesterday" 2>&1

# -------------------------------------------------------------------------------------------------------------- FINISH
# Print process wall clock time
duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
