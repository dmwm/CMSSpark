#!/bin/bash
# shellcheck disable=SC2068
set -e
##H cron4hpc_at_cms.sh
##H    Spark job will process a 1 year period, ending in the first day of the current month (without including it)
##H
##H Example: cron4hpc_at_cms.sh --keytab ./keytab --output <DIR> --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab             : Kerberos auth file: secrets/keytab
##H   - output             : Output directory. If not given, $HOME/output_hpc_at_cms will be used. I.e /eos/user/c/cmsmonit/www/hpc
##H   - p1, p2, host, wdir : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test               : Flag that will process 2 months of data instead of 1 year.
##H How to test: Just provide test directory as output directory.
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
util_cron_send_start "$myname" "1M"
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH

unset -v KEYTAB_SECRET OUTPUT_DIR PORT1 PORT2 K8SHOST WDIR IS_TEST
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} OUTPUT_DIR:${OUTPUT_DIR} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR} IS_TEST:${IS_TEST}"
util_check_vars PORT1 PORT2 K8SHOST
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"
util_check_and_create_dir "$OUTPUT_DIR"

# ----------------------------------------------------------------------------------------------------------------- RUN
util4logi "${myname} Spark Job is starting..."
spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
    --driver-memory=8g --executor-memory=8g --executor-cores=4 --num-executors=30
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.3.1
)

# run spark function
function run_spark() {
    spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/hpc_at_cms.py" "$@"
}

END_DATE="$(date +%Y-%m-01)"
START_DATE="$(date -d "$END_DATE -1 year" +%Y-%m-01)"
# If test, process only 2 months
if [[ "$IS_TEST" == 1 ]]; then
    START_DATE="$(date -d "$END_DATE -2 month" +%Y-%m-01)"
fi

util4logi "Plots from $START_DATE to $END_DATE"

run_spark --output_folder "$OUTPUT_DIR" --start_date "$START_DATE" --end_date "$END_DATE" 2>&1
ln -s -f "$OUTPUT_DIR/HPC@CMS_Running_Cores_Hourly_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/HPC@CMS_Running_Cores_Hourly_latest.png"

duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" "1M" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
