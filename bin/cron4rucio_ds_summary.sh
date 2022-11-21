#!/bin/bash
set -e
##H Can only run in K8s
##H
##H cron4rucio_ds_summary.sh
##H    Cron job of rucio_ds_summary.py which sends Spark agg results to MONIT via StompAMQ
##H
##H Usage:
##H    cron4rucio_ds_summary.sh \
##H        <keytab> value <amq> value <cmsmonitoring> value <stomp> value
##H
##H Example :
##H    cron4rucio_ds_summary.sh \
##H        --keytab ./keytab --amq ./amq-creds.json --cmsmonitoring ./CMSMonitoring.zip --stomp ./stomp-v700.zip \
##H        --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments with values:
##H   - keytab             : Kerberos auth file: secrets/kerberos
##H   - amq                : AMQ credentials and configurations json file for /topic/cms.rucio.dailystats:
##H                            secrets/cms-rucio-dailystats
##H   - cmsmonitoring      : dmwm/CMSMonitoring/src/python/CMSMonitoring folder as zip to be sent to Spark nodes
##H   - stomp              : stomp.py==7.0.0 module as zip to be sent to Spark nodes which has lower versions.
##H   - p1, p2, host, wdir : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test                : will run only test job which will send only 10 documents to AMQ topic. Please give test/training AMQ credentials
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
unset -v KEYTAB_SECRET AMQ_JSON_CREDS CMSMONITORING_ZIP STOMP_ZIP PORT1 PORT2 K8SHOST WDIR IS_TEST help
[ "$#" -ne 0 ] || usage

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,amq:,cmsmonitoring:,stomp:,p1:,p2:,host:,wdir:,test,help -- "$@")
VALID_ARGS=$?
if [ "$VALID_ARGS" != "0" ]; then
    usage
fi

echo "$(date --rfc-3339=seconds)" "[INFO] Given arguments: $PARSED_ARGS"
eval set -- "$PARSED_ARGS"

while [[ $# -gt 0 ]]; do
    case "$1" in
    --keytab)        KEYTAB_SECRET=$2     ; shift 2 ;;
    --amq)           AMQ_JSON_CREDS=$2    ; shift 2 ;;
    --cmsmonitoring) CMSMONITORING_ZIP=$2 ; shift 2 ;;
    --stomp)         STOMP_ZIP=$2         ; shift 2 ;;
    --p1)            PORT1=$2             ; shift 2 ;;
    --p2)            PORT2=$2             ; shift 2 ;;
    --host)          K8SHOST=$2           ; shift 2 ;;
    --wdir)          WDIR=$2              ; shift 2 ;;
    --test)          IS_TEST=1            ; shift   ;;
    -h | --help)     help=1               ; shift   ;;
    *) break ;;
    esac
done

if [[ "$help" == 1 ]]; then
    util_usage_help
fi
# ------------------------------------------------------------------------------------------------------------- PREPARE
# Define logs path for Spark imports which produce lots of info logs
LOG_DIR="$WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"

#  check files exist
util_check_files "$KEYTAB_SECRET" "$AMQ_JSON_CREDS" "$CMSMONITORING_ZIP" "$STOMP_ZIP"
# check variables set
util_check_vars PORT1 PORT2 K8SHOST WDIR

# INITIALIZE ANALYTIX SPARK3
util_setup_spark_k8s

# Authenticate kerberos and get principle user name
KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with ${KERBEROS_USER} user's keytab"

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
# Required for Spark job in K8s
util4logi "cron4rucio_ds_summary Spark Job is starting..."
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf "spark.driver.bindAddress=0.0.0.0" --driver-memory=8g --executor-memory=8g
    --conf "spark.driver.host=${K8SHOST}" --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.3.1 --py-files "${CMSMONITORING_ZIP},${STOMP_ZIP}"
)
py_input_args=(--creds "$AMQ_JSON_CREDS" --amq_batch_size 1000)
function run_spark() {
    spark-submit "${spark_submit_args[@]}" "${script_dir}/../src/python/CMSSpark/rucio_ds_summary.py" \
        "${py_input_args[@]}" >>"${LOG_DIR}/spark-rucio_ds_summary.log" 2>&1
}
function run_test_spark() {
    # Test will send 10 documents to AMQ topic
    py_input_args+=(--test)
    spark-submit "${spark_submit_args[@]}" "${script_dir}/../src/python/CMSSpark/rucio_ds_summary.py" \
        "${py_input_args[@]}" >>"${LOG_DIR}/spark-rucio_ds_summary.log" 2>&1
}

# RUN SPARK
if [[ "$IS_TEST" == 1 ]]; then
    # will send only 10 documents, only to test/training AMQ topic. Please check python script for more details.
    run_test_spark 2>&1
else
    run_spark 2>&1
fi

util4logi "last 10 lines of spark job log"
tail -10 "${LOG_DIR}/spark-rucio_ds_summary.log"

duration=$(($(date +%s) - START_TIME))
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
