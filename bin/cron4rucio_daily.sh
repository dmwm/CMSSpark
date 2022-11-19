#!/bin/bash
set -e
##H cron4rucio_daily.sh
##H    Daily Rucio dumps
##H Usage: cron4rucio_daily.sh <ARGS>
##H Example :
##H    cron4rucio_daily.sh --keytab ./keytab --output <HDFS> --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab               : Kerberos auth file: secrets/keytab
##H   - output               : HDFS directory, i.e.: /cms/rucio_daily
##H   - p1, p2, host, wdir   : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H How to test:
##H   - Just provide test hdfs directory as output directory. No need a test run, spark job will process only data of 1 day.
##H
script_dir="$(cd "$(dirname "$0")" && pwd)"
# get common util functions
. "$script_dir"/utils/common_utils.sh
trap 'onFailExit' ERR
onFailExit() {
    util4loge "finished with error!" || exit 1
}
# ------------------------------------------------------------------------------------------------------- GET USER ARGS
unset -v KEYTAB_SECRET OUTPUT_DIR PORT1 PORT2 K8SHOST WDIR help
[ "$#" -ne 0 ] || util_usage_help

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,output:,p1:,p2:,host:,wdir:,help -- "$@")
VALID_ARGS=$?
if [ "$VALID_ARGS" != "0" ]; then
    util_usage_help
fi

util4logi "Given arguments: $PARSED_ARGS"
eval set -- "$PARSED_ARGS"
while [[ $# -gt 0 ]]; do
    case "$1" in
    --keytab)    KEYTAB_SECRET=$2     ; shift 2 ;;
    --output)    OUTPUT_DIR=$2        ; shift 2 ;;
    --p1)        PORT1=$2             ; shift 2 ;;
    --p2)        PORT2=$2             ; shift 2 ;;
    --host)      K8SHOST=$2           ; shift 2 ;;
    --wdir)      WDIR=$2              ; shift 2 ;;
    -h | --help) help=1               ; shift   ;;
    *)           break                          ;;
    esac
done

if [[ "$help" == 1 ]]; then
    util_usage_help
fi
# ------------------------------------------------------------------------------------------------------------- PREPARE
# check variables set
util_check_vars OUTPUT_DIR PORT1 PORT2 K8SHOST WDIR
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
current_date="$(date +%Y-%m-%d)"

# INITIALIZE ANALYTIX SPARK3
util_setup_spark_k8s

# Authenticate kerberos and get principle user name
KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"
# ----------------------------------------------------------------------------------------------------------------- RUN
util4logi "output directory: ${OUTPUT_DIR}"
util4logi "spark job starting.."
spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf "spark.driver.bindAddress=0.0.0.0" --driver-memory=8g --executor-memory=8g
    --conf "spark.driver.host=${K8SHOST}" --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.2.1
)
py_input_args=(--verbose --output "$OUTPUT_DIR" --fdate "$current_date")

# log all to stdout
spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/rucio_daily.py" "${py_input_args[@]}" 2>&1
