#!/bin/bash
set -e
##H Can only run in K8s, you may modify to run in local by arranging env vars
##H
##H cron4rucio_ds_mongo.sh
##H    Cron job of rucio_ds_mongo.py which runs Spark job to get Rucio datasets and writes to HDFS directory.
##H    After writing data to HDFS directory, it copies HDFS files to LOCAL directory as a single file.
##H
##H Usage:
##H    cron4rucio_ds_mongo.sh --keytab <value> --mongouser <value> --mongopass <value>
##H
##H Example :
##H    cron4rucio_ds_mongo.sh \
##H        --keytab ./keytab \
##H        --mongouser $MONGO_ROOT_USERNAME \
##H        --mongopass $MONGO_ROOT_PASSWORD
##H
##H Arguments with values:
##H   - keytab       : Kerberos auth file to connect Spark Analytix cluster (cmsmonit)
##H   - mongouser    : MongoDB user which has write access to required MongoDB database/collection
##H   - mongopass    : MongoDB user password
##H
##H Requirements:
##H   - Kubernetes service name should be "cmsmon-rucio-ds-mongo". If not please modify Spark ports envs accordingly.
##H   - "mongoimport" CLI tool executable path should be set in $PATH.
##H
##H Environment Variable Requirements
##H   - MONGO_HOST
##H   - MONGO_PORT
##H   - MONGO_WRITE_DB                            : stores the spark results
##H   - MY_NODE_NAME                              : spark.driver.host
##H   - CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_0 : comes from K8s service NodePort
##H   - CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_1 : comes from K8s service NodePort
##H   - WDIR                                      : working directory
##H
##H References:
##H   - CMSSpark/bin/cron4rucio_datasets_daily_stats.sh
##H

trap 'onFailExit' ERR
onFailExit() {
    echo "$(date --rfc-3339=seconds)" "[ERROR] Finished with error!"
    exit 1
}
usage() {
    grep "^##H" <"$0" | sed -e "s,##H,,g"
    exit 1
}
# seconds to h, m, s format used in logging
secs_to_human() {
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
unset -v KEYTAB_SECRET MONGODB_USER MONGODB_PASSWORD help
[ "$#" -ne 0 ] || usage

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGUMENTS=$(
    getopt \
        --unquoted \
        --options v \
        --name "$(basename -- "$0")" \
        --longoptions keytab:,mongouser:,mongopass:,,help \
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
    --mongouser)
        MONGODB_USER=$2
        shift 2
        ;;
    --mongopass)
        MONGODB_PASSWORD=$2
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

# ------------------------------------------------------------------------------------------- CHECKS
# Script arguments checks
if [ ! -e "$KEYTAB_SECRET" ]; then
    echo "$(date --rfc-3339=seconds)" "[ERROR] Keytab file does not exist: $KEYTAB_SECRET, please check --keytab"
    exit 1
fi
[[ -z "$MONGODB_USER" ]] && {
    echo "$(date --rfc-3339=seconds)" "[ERROR]: please set --mongouser"
    exit 1
}
[[ -z "$MONGODB_PASSWORD" ]] && {
    echo "$(date --rfc-3339=seconds)" "[ERROR]: please set --mongopass"
    exit 1
}

# K8s env vars checks. These variables automatically created by kubernetes. Service name should be "cmsmon-rucio-ds-mongo"
[[ -z "$MONGO_HOST" ]] && {
    echo "ERROR: MONGO_HOST is not defined"
    exit 1
}
[[ -z "$MONGO_PORT" ]] && {
    echo "ERROR: MONGO_PORT is not defined"
    exit 1
}
[[ -z "$MONGO_WRITE_DB" ]] && {
    echo "ERROR: MONGO_WRITE_DB is not defined"
    exit 1
}
[[ -z "$MY_NODE_NAME" ]] && {
    echo "ERROR: MY_NODE_NAME is not defined"
    exit 1
}
[[ -z "$CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_0" ]] && {
    echo "ERROR: CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_0 is not defined, which is used for -spark.driver.port-"
    exit 1
}
[[ -z "$CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_1" ]] && {
    echo "ERROR: CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_1 is not defined, which used for -spark.driver.blockManager.port-"
    exit 1
}
[[ -z "$WDIR" ]] && {
    echo "ERROR: WDIR is not defined"
    exit 1
}

# Check JAVA_HOME is set
if [ -n "$JAVA_HOME" ]; then
    if [ -e "/usr/lib/jvm/java-1.8.0" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
    elif ! (java -XX:+PrintFlagsFinal -version 2>/dev/null | grep -E -q 'UseAES\s*=\s*true'); then
        echo "$(date --rfc-3339=seconds)" "[ERROR] This script requires a java version with AES enabled"
        exit 1
    fi
fi

# Check mongoimport exist
if which mongoimport >/dev/null; then
    echo "$(date --rfc-3339=seconds)" "[INFO] mongoimport exists, $(mongoimport --version | grep mongoimport)"
else
    echo "$(date --rfc-3339=seconds)" "[ERROR] Please make sure you correctly set mongoimport executable path in PATH env var."
    exit 1
fi

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

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB

# Arrange a temporary HDFS directory that current Kerberos user can use
HDFS_OUTPUT_PATH="/tmp/${KERBEROS_USER}/rucio_ds_mongo/$(date +%Y-%m-%d)"

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
        --conf spark.sql.session.timeZone=UTC
        --packages org.apache.spark:spark-avro_2.12:3.2.1
    )
    # Define Spark network settings in K8s cluster
    spark_submit_args+=(
        --conf "spark.driver.bindAddress=0.0.0.0"
        --conf "spark.driver.host=${MY_NODE_NAME}"
        --conf "spark.driver.port=${CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_0}"
        --conf "spark.driver.blockManager.port=${CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_1}"
    )
    py_input_args=(
        --hdfs_out_dir "$HDFS_OUTPUT_PATH"
    )
    # Run
    spark-submit \
        "${spark_submit_args[@]}" \
        "${SCRIPT_DIR}/../src/python/CMSSpark/rucio_ds_mongo.py" \
        "${py_input_args[@]}" \
        >>"${LOG_DIR}/spark-job.log" 2>&1
}

run_spark 2>&1

echo "$(date --rfc-3339=seconds)" "[INFO] Last 10 lines of Spark job log"
tail -10 "${LOG_DIR}/spark-job.log"

# --------------------------------------------------------------------------------- COPY HDFS OUTPUT TO LOCAL DIRECTORY
# Give read access to new dumps for all users
hadoop fs -chmod -R o+rx "$HDFS_OUTPUT_PATH"/

# Local directory in K8s pod to store Spark results which will be copied from HDFS
LOCAL_SPARK_RESULTS_DIR=$WDIR/results

# Create dir silently
mkdir -p "$LOCAL_SPARK_RESULTS_DIR"

LOCAL_DATASETS_FILE=$LOCAL_SPARK_RESULTS_DIR/datasets.json

# Delete if old one exists
rm -rf "$LOCAL_DATASETS_FILE"

# Copy files from HDFS to LOCAL directory as a single file
hadoop fs -getmerge "$HDFS_OUTPUT_PATH"/part-*.json "$LOCAL_DATASETS_FILE"

# -------------------------------------------------------------------------------------------------------- MONGO IMPORT
mongoimport --host "$MONGO_HOST" \
    --drop \
    --port "$MONGO_PORT" \
    --username "$MONGODB_USER" \
    --password "$MONGODB_PASSWORD" \
    --authenticationDatabase admin \
    --db "$MONGO_WRITE_DB" \
    --collection datasets \
    --file "$LOCAL_DATASETS_FILE" \
    --type=json

# ------------------------------------------------------------------------------------------------------ POST DELETIONS
# Delete yesterdays dumps
path_of_yesterday="/tmp/${KERBEROS_USER}/rucio_ds_mongo/$(date -d "yesterday" '+%Y-%m-%d')"
hadoop fs -rm -r -f -skipTrash "$path_of_yesterday"
echo "$(date --rfc-3339=seconds)" "[INFO] Spark results of yesterday is deleted ${path_of_yesterday}"

# Print process wall clock time
echo "$(date --rfc-3339=seconds)" "[INFO] All finished. Time spent: $(secs_to_human "$(($(date +%s) - START_TIME))")"
