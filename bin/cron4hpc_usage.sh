#!/bin/bash
# shellcheck disable=SC2068
set -e
##H cron4hpc_usage.sh
##H    Cron job of hpc_running_cores_and_corehr.py with --iterative option
##H    This cron job produce plots in html pages of for running cores and CoreHr monthly stats of HPC##H
##H
##H Example: cron4hpc_usage.sh --keytab ./keytab --output /eos/user/c/cmsmonit/www/hpc_usage \
##H        --url https://cmsdatapop.web.cern.ch/cmsdatapop/hpc_usage \
##H        --iterative --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab               : Kerberos auth file: secrets/keytab
##H   - iterative [OPTIONAL] : Flag to process HDFS data iteratively. Each iteration stores pickle files, and existing pickle files populated in next run.
##H   - output               : EOS directory
##H   - url                  : CERN web page of user account
##H   - p1, p2, host, wdir   : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test                 : Flag that will process 2 months of data instead of 1 year. Not iterative run!
##H How to test: Just provide test directory as output directory. "--iterative" mode cannot be used in first test run.
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
util_cron_send_start "$myname" "1d"
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH

unset -v KEYTAB_SECRET OUTPUT_DIR PORT1 PORT2 K8SHOST WDIR IS_TEST
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} IS_ITERATIVE:${IS_ITERATIVE} OUTPUT_DIR:${OUTPUT_DIR} URL_PREFIX:${URL_PREFIX} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR} IS_TEST:${IS_TEST}"
util_check_vars OUTPUT_DIR URL_PREFIX PORT1 PORT2 K8SHOST WDIR
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"
util_check_and_create_dir "$OUTPUT_DIR"

# ----------------------------------------------------------------------------------------------------------------- RUN
HTML_TEMPLATE="$script_dir"/../src/html/hpc/html_template.html
util4logi "output directory: ${OUTPUT_DIR}"
util4logi "${myname} Spark Job is starting..."

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --driver-memory=8g --executor-memory=8g
    --packages org.apache.spark:spark-avro_2.12:3.4.0
)
py_input_args=(--output_dir "$OUTPUT_DIR" --url_prefix "$URL_PREFIX" --html_template "$HTML_TEMPLATE")

# Add if iterative provided
if [[ "$IS_ITERATIVE" == 1 ]]; then
    py_input_args+=(--iterative --iterative_ndays_ago 10)
fi

# If it will be test run, it cannot be iterative data because no previous pickles will be exist. And start/end dates should be given.
if [[ "$IS_TEST" == 1 ]]; then
    END_DATE="$(date -d "$(date) -2 day" +%Y-%m-%d)"
    START_DATE="$(date -d "$END_DATE -5 day" +%Y-%m-%d)"
    py_input_args=(--start_date "$START_DATE" --end_date "$END_DATE" --output_dir "$OUTPUT_DIR" --url_prefix "$URL_PREFIX" --html_template "$HTML_TEMPLATE")
fi

# Run
spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/hpc_running_cores_and_corehr.py" "${py_input_args[@]}" 2>&1

duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" "1d" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
