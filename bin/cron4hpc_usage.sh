#!/bin/bash
set -e
##H cron4hpc_usage.sh
##H    Cron job of hpc_running_cores_and_corehr.py with --iterative option
##H    This cron job produce plots in html pages of for running cores and CoreHr monthly stats of HPC##H
##H Usage: cron4hpc_usage.sh <ARGS>
##H Example :
##H    cron4hpc_usage.sh \
##H        --keytab ./keytab --output /eos/user/c/cmsmonit/www/hpc_usage --url https://cmsdatapop.web.cern.ch/cmsdatapop/hpc_usage \
##H        --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H        --iterative
##H Arguments:
##H   - keytab               : Kerberos auth file: secrets/keytab
##H   - iterative [OPTIONAL] : Flag to process HDFS data iteratively. Each iteration stores pickle files, and existing pickle files populated in next run.
##H   - output               : EOS directory
##H   - url                  : CERN web page of user account
##H   - p1, p2, host, wdir   : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test                 : Flag that will process 2 months of data instead of 1 year. Not iterative run!
##H How to test:
##H   - Just provide test directory as output directory. "--iterative" mode cannot be used in first test run.
##H
TZ=UTC
START_TIME=$(date +%s)
script_dir="$(cd "$(dirname "$0")" && pwd)"
# get common util functions
. "$script_dir"/utils/common_utils.sh

trap 'onFailExit' ERR
onFailExit() {
    util4loge "finished with error!" || exit 1
}
# ------------------------------------------------------------------------------------------------------- GET USER ARGS
unset -v KEYTAB_SECRET IS_ITERATIVE OUTPUT_DIR URL_PREFIX PORT1 PORT2 K8SHOST WDIR IS_TEST help
[ "$#" -ne 0 ] || util_usage_help

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,iterative,output:,url:,p1:,p2:,host:,wdir:,test,help -- "$@")
VALID_ARGS=$?
if [ "$VALID_ARGS" != "0" ]; then
    util_usage_help
fi

util4logi "Given arguments: $PARSED_ARGS"
eval set -- "$PARSED_ARGS"
while [[ $# -gt 0 ]]; do
    case "$1" in
    --keytab)    KEYTAB_SECRET=$2     ; shift 2 ;;
    --iterative) IS_ITERATIVE=1       ; shift   ;;
    --output)    OUTPUT_DIR=$2        ; shift 2 ;;
    --url)       URL_PREFIX=$2        ; shift 2 ;;
    --p1)        PORT1=$2             ; shift 2 ;;
    --p2)        PORT2=$2             ; shift 2 ;;
    --host)      K8SHOST=$2           ; shift 2 ;;
    --wdir)      WDIR=$2              ; shift 2 ;;
    --test)      IS_TEST=1            ; shift   ;;
    -h | --help) help=1               ; shift   ;;
    *)           break                          ;;
    esac
done

if [[ "$help" == 1 ]]; then
    util_usage_help
fi
# ------------------------------------------------------------------------------------------------------------- PREPARE
# check variables set
util_check_vars OUTPUT_DIR URL_PREFIX PORT1 PORT2 K8SHOST WDIR

export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
HTML_TEMPLATE="$script_dir"/../src/html/hpc/html_template.html

LOG_DIR="$WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"
LOG_FILE=$LOG_DIR/cron4hpc_usage.log

# INITIALIZE ANALYTIX SPARK3
util_setup_spark_k8s

# Authenticate kerberos and get principle user name
KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"

# Requires kerberos ticket to reach the EOS directory
util_check_and_create_dir "$OUTPUT_DIR"

# ----------------------------------------------------------------------------------------------------------------- RUN
util4logi "output directory: ${OUTPUT_DIR}"
util4logi "spark job starting.."
# PySpark job args
spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf "spark.driver.bindAddress=0.0.0.0" --driver-memory=8g --executor-memory=8g
    --conf "spark.driver.host=${K8SHOST}" --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.2.1 --conf spark.sql.session.timeZone=UTC
)

py_input_args=(--output_dir "$OUTPUT_DIR" --url_prefix "$URL_PREFIX" --html_template "$HTML_TEMPLATE")

# Add if iterative provided
if [[ "$IS_ITERATIVE" == 1 ]]; then
    py_input_args+=(--iterative)
fi

# If it will be test run, it cannot be iterative data because no previous pickles will be exist. And start/end dates should be given.
if [[ "$IS_TEST" == 1 ]]; then
    END_DATE="$(date -d "$(date) -2 day" +%Y-%m-%d)"
    START_DATE="$(date -d "$END_DATE -2 month" +%Y-%m-%d)"
    py_input_args=(--start_date "$START_DATE" --end_date "$END_DATE" --output_dir "$OUTPUT_DIR" --url_prefix "$URL_PREFIX" --html_template "$HTML_TEMPLATE")
fi

# Run
spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/hpc_running_cores_and_corehr.py" "${py_input_args[@]}" >>"$LOG_FILE" 2>&1

duration=$(($(date +%s) - START_TIME))
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
