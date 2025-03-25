#!/bin/bash
# shellcheck disable=SC2068
set -e
##H Can only run in K8s
##H
##H cron4rucio_datasets_stats.sh
##H    Cron job of rucio_datasets_stats.py which runs aggregates rucio and DBS results and sends it to MONIT via StompAMQ
##H
##H Usage: cron4rucio_datasets_stats.sh <ARGS>
##H
##H Example :
##H    cron4rucio_datasets_stats.sh \
##H        --keytab ./keytab --amq ./amq-creds.json \
##H        --cmsmonitoring ./CMSMonitoring.zip --stomp ./stomp-v700.zip --eos /eos/user/c/cmsmonit/www/rucio_test \
##H        --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab              : Kerberos auth file: secrets/keytab
##H   - amq                 : AMQ credentials and configurations json file for /topic/cms.rucio.dailystats:
##H                             secrets/cms-rucio-dailystats
##H   - cmsmonitoring       : dmwm/CMSMonitoring/src/python/CMSMonitoring folder as zip to be sent to Spark nodes
##H   - stomp               : stomp.py==7.0.0 module as zip to be sent to Spark nodes which has lower versions.
##H   - eos                 : Base EOS directory to write datasets statistics in html format.
##H   - p1, p2, host, wdir  : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test                : Flag that will run only test job which will send only 10 documents to AMQ topic. Please give test/training AMQ credentials
##H References:
##H   - getopt ref: https://www.shellscript.sh/tips/getopt/index.html
##H How to test:
##H   - Use personal keytab file, because sqoop dumps will be written to its '/tmp/$USER/rucio_daily_stats' directory
##H   - Use test or training AMQ topic and its credentials, test AMQ credentials json file should be provided via --amq
##H   - Delete or comment out 'run_spark 2>&1' line and uncomment '##TEST##run_test 2>&1' line
##H   - That's all
##H

TZ=UTC
START_TIME=$(date +%s)
myname=$(basename "$0")
script_dir="$(cd "$(dirname "$0")" && pwd)"
. "$script_dir"/utils/common_utils.sh

export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH

# producer metadata versioning. Please do not manually change the code, follow the git wf docker build.
CMSSPARK_GIT_TAG=$(git --git-dir "$script_dir"/../.git describe --tags | tail -1)
util4logi "CMSSPARK_GIT_TAG: $CMSSPARK_GIT_TAG"

if [ "$1" == "" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    util_usage_help
    exit 0
fi
util_cron_send_start "$myname" "1d"

unset -v KEYTAB_SECRET AMQ_JSON_CREDS CMSMONITORING_ZIP STOMP_ZIP EOS_DIR PORT1 PORT2 K8SHOST WDIR IS_TEST help
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} AMQ_JSON_CREDS:${AMQ_JSON_CREDS} CMSMONITORING_ZIP:${CMSMONITORING_ZIP} STOMP_ZIP:${STOMP_ZIP} EOS_DIR:${EOS_DIR} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR} IS_TEST:${IS_TEST}"
util_check_files "$KEYTAB_SECRET" "$AMQ_JSON_CREDS" "$CMSMONITORING_ZIP" "$STOMP_ZIP"
util_check_vars PORT1 PORT2 K8SHOST WDIR
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"

# Define logs path for Spark imports which produce lots of info logs
LOG_DIR="$WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"

# Requires kerberos ticket to reach the EOS directory
if [ ! -d "$EOS_DIR" ]; then
    util4logw "EOS directory does not exist: ${EOS_DIR}}"
    # exit 1, do not exit till we solve the problem in K8s magnum-eos pods which always cause trouble in each 40-50 days
fi

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
# Required for Spark job in K8s
util4logi "${myname} Spark Job is starting..."
current_date="$(date +%Y-%m-%d)"

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
    --conf spark.shuffle.useOldFetchProtocol=true
    --driver-memory=8g --executor-memory=16g
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.4.0 --py-files "${CMSMONITORING_ZIP},${STOMP_ZIP}"
)
py_input_args=(
    --creds "$AMQ_JSON_CREDS"
    --base_eos_dir "$EOS_DIR"
    --cmsspark_git_tag "$CMSSPARK_GIT_TAG"
    --amq_batch_size 1000
    --fdate "$current_date"
)

function run_spark() {
    spark-submit "${spark_submit_args[@]}" "${script_dir}/../src/python/CMSSpark/rucio_datasets_stats.py" \
        "${py_input_args[@]}" >>"${LOG_DIR}/spark-job.log" 2>&1
}
function run_test_spark() {
    # Test will send 10 documents to AMQ topic
    py_input_args+=(--test)
    spark-submit "${spark_submit_args[@]}" "${script_dir}/../src/python/CMSSpark/rucio_datasets_stats.py" \
        "${py_input_args[@]}" >>"${LOG_DIR}/spark-job.log" 2>&1
}

# RUN SPARK
if [[ "$IS_TEST" == 1 ]]; then
    # will send only 10 documents, only to test/training AMQ topic. Please check python script for more details.
    run_test_spark 2>&1
else
    run_spark 2>&1
fi

util4logi "last 10 lines of spark job log"
tail -10 "${LOG_DIR}/spark-job.log"

duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" "1d" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
