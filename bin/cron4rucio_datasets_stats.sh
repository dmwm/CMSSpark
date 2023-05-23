#!/bin/bash
# shellcheck disable=SC2068
set -e
##H Can only run in K8s
##H
##H cron4rucio_datasets_stats.sh
##H    Cron job of rucio_datasets_stats.py which runs Sqoop dumps and send Spark agg results to MONIT via StompAMQ
##H    We need to run Sqoop dumps and Spark job sequentially.
##H    We don't keep the historical Sqoop dumps, used in this job, by having deleting past days' HDFS data in new runs.
##H
##H Usage: cron4rucio_datasets_stats.sh <ARGS>
##H
##H Example :
##H    cron4rucio_datasets_stats.sh \
##H        --keytab ./keytab --cmsr ./cmsr_cstring --rucio ./rucio --amq ./amq-creds.json \
##H        --cmsmonitoring ./CMSMonitoring.zip --stomp ./stomp-v700.zip --eos /eos/user/c/cmsmonit/www/rucio_test \
##H        --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab              : Kerberos auth file: secrets/keytab
##H   - cmsr                : DBS oracle user&pass secret file: secrets/sqoop
##H   - rucio               : Rucio oracle user&pass secret file: secrets/rucio
##H   - amq                 : AMQ credentials and configurations json file for /topic/cms.rucio.dailystats:
##H                             secrets/cms-rucio-dailystats
##H   - cmsmonitoring       : dmwm/CMSMonitoring/src/python/CMSMonitoring folder as zip to be sent to Spark nodes
##H   - stomp               : stomp.py==7.0.0 module as zip to be sent to Spark nodes which has lower versions.
##H   - eos                 : Base EOS directory to write datasets statistics in html format.
##H   - p1, p2, host, wdir  : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test                : Flag that will run only test job which will send only 10 documents to AMQ topic. Please give test/training AMQ credentials
##H References:
##H   - some features&logics are copied from sqoop_utils.sh and cms-dbs3-full-copy.sh
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

unset -v KEYTAB_SECRET CMSR_SECRET RUCIO_SECRET AMQ_JSON_CREDS CMSMONITORING_ZIP STOMP_ZIP EOS_DIR PORT1 PORT2 K8SHOST WDIR IS_TEST help
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} CMSR_SECRET:${CMSR_SECRET} RUCIO_SECRET:${RUCIO_SECRET} AMQ_JSON_CREDS:${AMQ_JSON_CREDS} CMSMONITORING_ZIP:${CMSMONITORING_ZIP} STOMP_ZIP:${STOMP_ZIP} EOS_DIR:${EOS_DIR} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR} IS_TEST:${IS_TEST}"
util_check_files "$KEYTAB_SECRET" "$CMSR_SECRET" "$RUCIO_SECRET" "$AMQ_JSON_CREDS" "$CMSMONITORING_ZIP" "$STOMP_ZIP"
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

# -------------------------------------------------------------------------------------------------------------- TABLES
# The tables of which split-by column set as Null and -m=1 are so small tables, no need to do parallel import
RUCIO_TABLES_DICT="table split_column num_mappers
REPLICAS,rse_id,40
CONTENTS,Null,40
RSES,Null,1"
RUCIO_SCHEMA=CMS_RUCIO_PROD

DBS_TABLES_DICT="table split_column num_mappers
FILES,dataset_id,40
DATASETS,dataset_id,4
DATA_TIERS,Null,1
PHYSICS_GROUPS,Null,1
ACQUISITION_ERAS,Null,1
DATASET_ACCESS_TYPES,Null,1"
DBS_SCHEMA=CMS_DBS3_PROD_GLOBAL_OWNER

# DBS and Rucio use same jdbc url
JDBC_URL=$(sed '1q;d' "$CMSR_SECRET")

# ------------------------------------------------------------------------------------------------------------ HDFS DIR
# Use tmp hdfs directory to store table dumps
SQOOP_DUMP_DIR_PREFIX=/tmp/${KERBEROS_USER}/rucio_daily_stats
BASE_SQOOP_DUMP_DIR="$SQOOP_DUMP_DIR_PREFIX"-$(date +%Y-%m-%d)

# ------------------------------------------------------------------------------------------------- IMPORT SINGLE TABLE
function dump_single_table() {
    trap 'onFailExit' ERR
    local local_username=$1
    local local_password=$2
    local local_schema=$3
    local local_table=$4
    local local_split_column=$5
    local local_num_mappers=$6

    # If split-by column set as Null, use only num_mapper
    if [[ "$local_split_column" == "Null" ]]; then
        extra_args=(-m "$local_num_mappers")
    else
        extra_args=(-m "$local_num_mappers" --split-by "$local_split_column")
    fi

    util4logi "start: ${local_schema}.${local_table}" with "${extra_args[@]}"
    /usr/hdp/sqoop/bin/sqoop import \
        -Dmapreduce.job.user.classpath.first=true \
        -Doraoop.timestamp.string=false \
        -Dmapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
        -Ddfs.client.socket-timeout=120000 \
        --direct --compress --throw-on-error \
        --username "$local_username" --password "$local_password" \
        --connect "$JDBC_URL" \
        --as-avrodatafile \
        --target-dir "${BASE_SQOOP_DUMP_DIR}/${local_table}/" \
        --table "$local_schema"."$local_table" \
        "${extra_args[@]}" \
        >>"${LOG_DIR}/${local_table}.log" 2>&1

    util4logi "end: ${local_schema}.${local_table}"
}

# ------------------------------------------------------------------------------------------------ IMPORT RUCIO AND DBS
# One each of Rucio or DBS at a time

function import_rucio_tables() {
    trap 'onFailExit' ERR
    rucio_username=$(grep username <"$RUCIO_SECRET" | awk '{print $2}')
    rucio_password=$(grep password <"$RUCIO_SECRET" | awk '{print $2}')
    {
        # Skip first line
        read -r
        while IFS=, read -r table split_column num_mappers; do
            dump_single_table "$rucio_username" "$rucio_password" "$RUCIO_SCHEMA" "$table" "$split_column" "$num_mappers"
        done
    } <<<"$RUCIO_TABLES_DICT"
}

function import_dbs_tables() {
    trap 'onFailExit' ERR
    dbs_username=$(sed '2q;d' "$CMSR_SECRET")
    dbs_password=$(sed '3q;d' "$CMSR_SECRET")
    {
        # Skip first line since its column names
        read -r
        while IFS=, read -r table split_column num_mappers; do
            dump_single_table "$dbs_username" "$dbs_password" "$DBS_SCHEMA" "$table" "$split_column" "$num_mappers"
        done
    } <<<"$DBS_TABLES_DICT"
}

# ------------------------------------------------------------------------------------------------------------ MAIN RUN
import_rucio_tables 2>&1 &
import_dbs_tables 2>&1 &
wait

# Give read access to new dumps for all users
hadoop fs -chmod -R o+rx "$BASE_SQOOP_DUMP_DIR"/

# Delete yesterdays dumps
path_of_yesterday="$SQOOP_DUMP_DIR_PREFIX"-"$(date -d "yesterday" '+%Y-%m-%d')"
hadoop fs -rm -r -f -skipTrash "$path_of_yesterday"
util4logi "dump of yesterday is deleted ${path_of_yesterday}"
util4logi "dumps are finished. Time spent: $(util_secs_to_human "$(($(date +%s) - START_TIME))")"

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
# Required for Spark job in K8s
util4logi "${myname} Spark Job is starting..."

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
    --conf spark.shuffle.useOldFetchProtocol=true
    --driver-memory=8g --executor-memory=8g
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.4.0 --py-files "${CMSMONITORING_ZIP},${STOMP_ZIP}"
)
py_input_args=(
    --creds "$AMQ_JSON_CREDS"
    --base_hdfs_dir "$BASE_SQOOP_DUMP_DIR"
    --base_eos_dir "$EOS_DIR"
    --cmsspark_git_tag "$CMSSPARK_GIT_TAG"
    --amq_batch_size 1000
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
