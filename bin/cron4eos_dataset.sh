#!/bin/bash
# shellcheck disable=SC2068
set -e
##H cron4eos_dataset.sh <ARGS>
##H    Creates EOS datasets plots
##H
##H Example: cron4eos_dataset.sh --keytab ./keytab --output <DIR> --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab             : Kerberos auth file: secrets/keytab
##H   - output             : Output directory. If not given, $HOME/output will be used. I.e /eos/user/c/cmsmonit/www/EOS/data
##H   - p1, p2, host, wdir : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test               : Flag that will process 2 months of data instead of 1 year.
##H How to test: Just provide a test directory as output directory.
##H
TZ=UTC
START_TIME=$(date +%s)
myname=$(basename "$0")
script_dir="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"
. "$script_dir"/utils/common_utils.sh

if [ "$1" == "" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    util_usage_help
    exit 0
fi
util_cron_send_start "$myname"
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
util4logi "spark job starting.."
spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --driver-memory=8g --executor-memory=8g
    --packages org.apache.spark:spark-avro_2.12:3.2.1
)

# run spark function
function run_spark() {
    spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/dbs_hdfs_eos.py" $@
}

# Generate the previous month intermediate files
month_="$(date -d "$(date +%Y-%m-15) -1 month" +%Y/%m)"
util4logi "Processing month " "$month_"
mode_="append"

if [[ "$IS_TEST" == 1 ]]; then
    export PARQUET_LOCATION="/tmp/${KERBEROS_USER}/test_parquest_eos_dataset"
fi

# Click replaces underscore with dash
run_spark run-update --mode "$mode_" --date "$month_/*"

# Generate the totals csv for one year
end_=$(date -d "$month_/01 +1 month -1 day" +%Y%m%d)
start_=$(date -d "$end_ -1 year +1 day" +%Y%m%d)

# If test, process only 2 months
if [[ "$IS_TEST" == 1 ]]; then
    start_=$(date -d "$end_ -2 month +1 day" +%Y%m%d)
fi

util4logi "Totals for dataset/file from $start_ to $end_"
run_spark run-report-totals --outputDir "$OUTPUT_DIR" --period "$start_" "$end_"

if [[ -f "$OUTPUT_DIR/dataset_totals.csv" ]]; then
    mv "$OUTPUT_DIR/dataset_totals.csv" "$OUTPUT_DIR/dataset_totals_yearly_${start_}_${end_}.csv"
    cp -f "$OUTPUT_DIR/dataset_totals_yearly_${start_}_${end_}.csv" "$OUTPUT_DIR/dataset_totals_rolling_year.csv"
    ln -s -f "$OUTPUT_DIR/top_total_rb_${start_}-${end_}.png" "$OUTPUT_DIR/topDS_last_rolling_year.png"
    gzip "$OUTPUT_DIR/dataset_totals_yearly_${start_}_${end_}.csv"
fi

# Generate and concat the last month data.
start_=$(date -d "$month_/01" +%Y%m%d)
util4logi "Totals for the last month, $start_ to $end_"
run_spark run-report-totals --outputDir "$OUTPUT_DIR" --period "$start_" "$end_"
if [[ -f "$OUTPUT_DIR/dataset_totals.csv" ]]; then
    mv "$OUTPUT_DIR/dataset_totals.csv" "$OUTPUT_DIR/dataset_totals_${start_}_${end_}.csv"
    ln -s -f "$OUTPUT_DIR/dataset_totals_${start_}_${end_}.csv" "$OUTPUT_DIR/dataset_totals_last_month.csv"
    ln -s -f "$OUTPUT_DIR/top_total_rb_${start_}_${end_}.png" "$OUTPUT_DIR/topDS_last_rolling_month.png"
fi

duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
