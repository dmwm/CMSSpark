#!/bin/bash
set -e
##H Can only run in K8s
##H
##H cron4rucio_datasets_daily_stats.sh
##H    Cron job of rucio_datasets_daily_stats.py which runs Sqoop dumps and send Spark agg results to MONIT via StompAMQ
##H    We need to run Sqoop dumps and Spark job sequentially.
##H    We don't keep the historical Sqoop dumps, used in this job, by having deleting past days' HDFS data in new runs.
##H
##H Usage:
##H    cron4rucio_datasets_daily_stats.sh \
##H        <keytab> value <cmsr> value <rucio> value <amq> value <cmsmonitoring> value <stomp> value <eos> value
##H
##H Example :
##H    cron4rucio_datasets_daily_stats.sh \
##H        --keytab ./keytab --cmsr ./cmsr_cstring --rucio ./rucio --amq ./amq-creds.json \
##H        --cmsmonitoring ./CMSMonitoring.zip --stomp ./stomp-v700.zip --eos /eos/user/c/cmsmonit/www/rucio_test
##H
##H Arguments with values:
##H   - keytab                    : Kerberos auth file: secrets/kerberos
##H   - cmsr                      : DBS oracle user&pass secret file: secrets/sqoop
##H   - rucio                     : Rucio oracle user&pass secret file: secrets/rucio
##H   - amq                       : AMQ credentials and configurations json file for /topic/cms.rucio.dailystats:
##H                                   secrets/cms-rucio-dailystats
##H   - cmsmonitoring             : dmwm/CMSMonitoring/src/python/CMSMonitoring folder as zip to be sent to Spark nodes
##H   - stomp                     : stomp.py==7.0.0 module as zip to be sent to Spark nodes which has lower versions.
##H   - eos                       : Base EOS directory to write datasets statistics in html format.
##H
##H References:
##H   - some features&logics are copied from sqoop_utils.sh and cms-dbs3-full-copy.sh
##H   - getopt ref: https://www.shellscript.sh/tips/getopt/index.html
##H

trap 'onFailExit' ERR
onFailExit() {
    echo "$(date --rfc-3339=seconds)" "[ERROR] Finished with error!"
    exit 1
}
usage() {
    perl -ne '/^##H/ && do { s/^##H ?//; print }' <"$0"
    exit 1
}
# seconds to h, m, s format used in logging
secs_to_human() {
    # Ref https://stackoverflow.com/a/59096583/6123088
    echo "$((${1} / 3600))h $(((${1} / 60) % 60))m $((${1} % 60))s"
}
SCRIPT_DIR="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"
TZ=UTC
START_TIME=$(date +%s)

# Define logs path for Spark imports which produce lots of info logs
LOG_DIR="$WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"

# ------------------------------------------------------------------------------------------------------- GET USER ARGS
unset -v KEYTAB_SECRET CMSR_SECRET RUCIO_SECRET AMQ_JSON_CREDS CMSMONITORING_ZIP STOMP_ZIP EOS_DIR help
[ "$#" -ne 0 ] || usage

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGUMENTS=$(
    getopt \
        --unquoted \
        --options v \
        --name "$(basename -- "$0")" \
        --longoptions keytab:,cmsr:,rucio:,amq:,cmsmonitoring:,stomp:,eos:,,help \
        -- "$@"
)
VALID_ARGUMENTS=$?
if [ "$VALID_ARGUMENTS" != "0" ]; then
    usage
fi

echo "$(date --rfc-3339=seconds)" "[INFO] Given arguments: $PARSED_ARGUMENTS"
eval set -- "$PARSED_ARGUMENTS"

while [[ $# -gt 0 ]]; do
    case "$1" in
    --keytab)
        KEYTAB_SECRET=$2
        shift 2
        ;;
    --cmsr)
        CMSR_SECRET=$2
        shift 2
        ;;
    --rucio)
        RUCIO_SECRET=$2
        shift 2
        ;;
    --amq)
        AMQ_JSON_CREDS=$2
        shift 2
        ;;
    --cmsmonitoring)
        CMSMONITORING_ZIP=$2
        shift 2
        ;;
    --stomp)
        STOMP_ZIP=$2
        shift 2
        ;;
    --eos)
        EOS_DIR=$2
        shift 2
        ;;
    -h | --help)
        help=1
        shift
        ;;
    *)
        break
        ;;
    esac
done

#
if [[ "$help" == 1 ]]; then
    usage
fi

# ------------------------------------------------------------------------------------------- CHECK IF FILES/DIRS EXIST
for fname in $KEYTAB_SECRET $CMSR_SECRET $RUCIO_SECRET $AMQ_JSON_CREDS $CMSMONITORING_ZIP $STOMP_ZIP; do
    if [ ! -e "${BASE_SECRET_FILES_DIR}/${fname}" ]; then
        echo "$(date --rfc-3339=seconds)" "[ERROR] File does not exist: ${BASE_SECRET_FILES_DIR}/${fname}"
        exit 1
    fi
done

# Check JAVA_HOME is set
if [ -n "$JAVA_HOME" ]; then
    if [ -e "/usr/lib/jvm/java-1.8.0" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
    elif ! (java -XX:+PrintFlagsFinal -version 2>/dev/null | grep -E -q 'UseAES\s*=\s*true'); then
        echo "$(date --rfc-3339=seconds)" "[ERROR] This script requires a java version with AES enabled"
        exit 1
    fi
fi

# ------------------------------------------------------------------------------------------------------ CHECK K8S ENVS
[[ -z "$MY_NODE_NAME" ]] && {
    echo "ERROR: MY_NODE_NAME is not defined"
    exit 1
}
[[ -z "$CMSMON_RUCIO_DS_SERVICE_PORT_PORT_0" ]] && {
    echo "ERROR: CMSMON_RUCIO_DS_SERVICE_PORT_PORT_0 is not defined, which is used for -spark.driver.port-"
    exit 1
}
[[ -z "$CMSMON_RUCIO_DS_SERVICE_PORT_PORT_1" ]] && {
    echo "ERROR: CMSMON_RUCIO_DS_SERVICE_PORT_PORT_1 is not defined, which used for -spark.driver.blockManager.port-"
    exit 1
}
[[ -z "$WDIR" ]] && {
    echo "ERROR: WDIR is not defined"
    exit 1
}

# --------------------------------------------------------------------------------------------- Kerberos authentication
PRINCIPAL=$(klist -k "$KEYTAB_SECRET" | tail -1 | awk '{print $2}')
echo "Kerberos principle=$PRINCIPAL"
kinit "$PRINCIPAL" -k -t "$KEYTAB_SECRET"
if [ $? == 1 ]; then
    echo "$(date --rfc-3339=seconds)" "[ERROR] Unable to perform kinit"
    exit 1
fi
klist -k "$KEYTAB_SECRET"
# Get Kerberos user name to use its /tmp HDFS directory as temporary folder
KERBEROS_USER=$(echo "$PRINCIPAL" | grep -o '^[^@]*')

# Requires kerberos ticket to reach the EOS directory
if [ ! -d "$EOS_DIR" ]; then
    echo "$(date --rfc-3339=seconds)" "[ERROR] EOS directory does not exist: ${EOS_DIR}}"
    exit 1
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

# ------------------------------------------------------------------------------------------ INITIALIZE ANALYTIX SPARK3
hadoop-set-default-conf.sh analytix
source hadoop-setconf.sh analytix 3.2 spark3

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

    echo "$(date --rfc-3339=seconds)" [INFO] Start: "$local_schema"."$local_table" with "${extra_args[@]}"
    /usr/hdp/sqoop/bin/sqoop import \
        -Dmapreduce.job.user.classpath.first=true \
        -Doraoop.timestamp.string=false \
        -Dmapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom" \
        -Ddfs.client.socket-timeout=120000 \
        --username "$local_username" --password "$local_password" \
        --direct \
        --compress \
        --throw-on-error \
        --connect "$JDBC_URL" \
        --as-avrodatafile \
        --target-dir "${BASE_SQOOP_DUMP_DIR}/${local_table}/" \
        --table "$local_schema"."$local_table" \
        "${extra_args[@]}" \
        >>"${LOG_DIR}/${local_table}.log" 2>&1

    echo "$(date --rfc-3339=seconds)" [INFO] End: "$local_schema"."$local_table"
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
echo "$(date --rfc-3339=seconds)" "[INFO] Dump of yesterday is deleted ${path_of_yesterday}"
echo "$(date --rfc-3339=seconds)" "[INFO] Dumps are finished. Time spent: $(secs_to_human "$(($(date +%s) - START_TIME))")"

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
function run_spark() {
    # Required for Spark job in K8s
    export SPARK_LOCAL_IP=127.0.0.1
    echo "$(date --rfc-3339=seconds)" "[INFO] Spark job starts"
    export PYTHONPATH=$SCRIPT_DIR/../src/python:$PYTHONPATH
    spark_submit_args=(
        --master yarn
        --conf spark.executor.memory=8g
        --conf spark.executor.instances=30
        --conf spark.executor.cores=4
        --conf spark.driver.memory=8g
        --conf spark.ui.showConsoleProgress=false
        --packages org.apache.spark:spark-avro_2.12:3.2.1
        --py-files "${CMSMONITORING_ZIP},${STOMP_ZIP}"
    )
    # Define Spark network settings in K8s cluster
    spark_submit_args+=(
        --conf "spark.driver.bindAddress=0.0.0.0"
        --conf "spark.driver.host=${MY_NODE_NAME}"
        --conf "spark.driver.port=${CMSMON_RUCIO_DS_SERVICE_PORT_PORT_0}"
        --conf "spark.driver.blockManager.port=${CMSMON_RUCIO_DS_SERVICE_PORT_PORT_1}"
    )
    py_input_args=(
        --creds "$AMQ_JSON_CREDS"
        --base_hdfs_dir "$BASE_SQOOP_DUMP_DIR"
        --base_eos_dir "$EOS_DIR"
        --amq_batch_size 1000
    )
    # Run
    spark-submit \
        "${spark_submit_args[@]}" \
        "${SCRIPT_DIR}/../src/python/CMSSpark/rucio_datasets_daily_stats.py" \
        "${py_input_args[@]}" \
        >>"${LOG_DIR}/spark-job.log" 2>&1
}

run_spark 2>&1

echo "$(date --rfc-3339=seconds)" "[INFO] Last 10 lines of Spark job log"
tail -10 "${LOG_DIR}/spark-job.log"
# Print process wall clock time
echo "$(date --rfc-3339=seconds)" "[INFO] All finished. Time spent: $(secs_to_human "$(($(date +%s) - START_TIME))")"
