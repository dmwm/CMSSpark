#!/bin/bash
set -e
##H Can only run in K8s, you may modify to run in local by arranging env vars
##H
##H cron4rucio_datasets_to_mongo.sh
##H    Cron job of rucio_ds_mongo.py which runs Spark job to get Rucio datasets and writes to HDFS directory.
##H    After writing data to HDFS directory, it copies HDFS files to LOCAL directory as a single file.
##H
##H Arguments with values:
##H   - keytab       : Kerberos auth file to connect Spark Analytix cluster (cmsmonit)
##H   - mongohost    : MongoDB host
##H   - mongoport    : MongoDB port
##H   - mongouser    : MongoDB user which has write access to required MongoDB database/collection
##H   - mongopass    : MongoDB user password
##H   - mongowritedb : MongoDB database name that results will be written
##H   - mongoauthdb  : MongoDB database for authentication. Required for mongoimport `--authenticationDatabase` argument
##H   - nodename     : spark-submit "spark.driver.host". In K8s, it should be "MY_NODE_NAME"(community uses this variable)
##H   - sparkport0   : spark-submit "spark.driver.port"
##H   - sparkport1   : spark-submit "spark.driver.blockManager.port"
##H   - sparkbindadr : spark-submit "spark.driver.bindAddress". In K8s, it should be "0.0.0.0"
##H   - sparklocalip : Spark "$SPARK_LOCAL_IP" environment variable. In K8s, it should be "127.0.0.1"
##H   - wdir         : Working directory to arrange logging directory
##H
##H Usage Example:
##H    ./cron4rucio_datasets_to_mongo.sh --keytab ./keytab --mongohost $MONGO_HOST --mongoport $MONGO_PORT --mongouser $MONGO_ROOT_USERNAME --mongopass $MONGO_ROOT_PASSWORD \
##H                                      --mongowritedb rucio --mongoauthdb admin --nodename $MY_NODE_NAME --sparkport0 $CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_0 \
##H                                      --sparkport1 $CMSMON_RUCIO_DS_MONGO_SERVICE_PORT_PORT_1 --sparkbindadr 0.0.0.0 --sparklocalip 127.0.0.1 --wdir $WDIR
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

# ------------------------------------------------------------------------------------------------------- GET USER ARGS
unset -v ARG_KEYTAB ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB ARG_NODENAME ARG_SPARKPORT0 ARG_SPARKPORT1 ARG_SPARKBINDADDRESS ARG_SPARKLOCALIP ARG_WDIR help
[ "$#" -ne 0 ] || usage

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGUMENTS=$(
    getopt \
        --unquoted \
        --options v \
        --name "$(basename -- "$0")" \
        --longoptions keytab:,mongohost:,mongoport:,mongouser:,mongopass:,mongowritedb:,mongoauthdb:,nodename:,sparkport0:,sparkport1:,sparklocalip:,sparkbindadr:,wdir:,,help \
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
    --keytab)       ARG_KEYTAB=$2           ; shift 2 ;;
    --mongohost)    ARG_MONGOHOST=$2        ; shift 2 ;;
    --mongoport)    ARG_MONGOPORT=$2        ; shift 2 ;;
    --mongouser)    ARG_MONGOUSER=$2        ; shift 2 ;;
    --mongopass)    ARG_MONGOPASS=$2        ; shift 2 ;;
    --mongowritedb) ARG_MONGOWRITEDB=$2     ; shift 2 ;;
    --mongoauthdb)  ARG_MONGOAUTHDB=$2      ; shift 2 ;;
    --nodename)     ARG_NODENAME=$2         ; shift 2 ;;
    --sparkport0)   ARG_SPARKPORT0=$2       ; shift 2 ;;
    --sparkport1)   ARG_SPARKPORT1=$2       ; shift 2 ;;
    --sparkbindadr) ARG_SPARKBINDADDRESS=$2 ; shift 2 ;;
    --sparklocalip) ARG_SPARKLOCALIP=$2     ; shift 2 ;;
    --wdir)         ARG_WDIR=$2             ; shift 2 ;;
    -h | --help)    help=1                  ; shift   ;;
    *) break;;
    esac
done

#
if [[ "$help" == 1 ]]; then
    usage
fi

# -------------------------------------------------------------------------------------------------------------- CHECKS
# Check variables are defined
function check_vars() {
    var_names=("$@")
    for var_name in "${var_names[@]}"; do
        [ -z "${!var_name}" ] && echo "$var_name is unset." && var_unset=true
    done
    [ -n "$var_unset" ] && exit 1
    return 0
}
# Run check function
check_vars ARG_KEYTAB ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB ARG_NODENAME ARG_SPARKPORT0 ARG_SPARKPORT1 ARG_SPARKBINDADDRESS ARG_SPARKLOCALIP ARG_WDIR

# Check file
if [ ! -e "$ARG_KEYTAB" ]; then
    echo "$(date --rfc-3339=seconds)" "[ERROR] Keytab file does not exist: $KEYTAB_SECRET, please check --keytab"
    exit 1
fi

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

# --------------------------------------------------------------------------------------------- KERBEROS AUTHENTICATION
PRINCIPAL=$(klist -k "$ARG_KEYTAB" | tail -1 | awk '{print $2}')
echo "Kerberos principle=$PRINCIPAL"
kinit "$PRINCIPAL" -k -t "$ARG_KEYTAB"
if [ $? == 1 ]; then
    echo "$(date --rfc-3339=seconds)" "[ERROR] Unable to perform kinit"
    exit 1
fi
klist -k "$ARG_KEYTAB"
# Get Kerberos user name to use its /tmp HDFS directory as temporary folder
KERBEROS_USER=$(echo "$PRINCIPAL" | grep -o '^[^@]*')

# ------------------------------------------------------------------------------------------ INITIALIZE ANALYTIX SPARK3
hadoop-set-default-conf.sh analytix
source hadoop-setconf.sh analytix 3.2 spark3

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
# Define logs path for Spark imports which produce lots of info logs
LOG_DIR="$ARG_WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"

# arg1: python file [rucio_all_datasets.py or rucio_all_detailed_datasets.py]
# arg2: hdfs output directory
# arg3: log file
# arg4: mongodb collection name [datasets or detailed_datasets]
# arg5: hdfs output directory of yesterday
function run_spark_and_mongo_import() {
    # Required for Spark job in K8s
    spark_py_file=$1
    hdfs_out_dir=$2
    log_file=$3
    collection=$4
    yesterday_hdfs_out_dir=$5

    export SPARK_LOCAL_IP="$ARG_SPARKLOCALIP"
    echo "$(date --rfc-3339=seconds)" "[INFO] Spark job for ${spark_py_file} starting"
    export PYTHONPATH=$SCRIPT_DIR/../src/python:$PYTHONPATH
    spark_submit_args=(
        --master yarn
        --conf spark.executor.memory=8g
        #--conf spark.executor.instances=30
        #--conf spark.executor.cores=4
        --conf spark.driver.memory=8g
        --conf spark.ui.showConsoleProgress=false
        --conf spark.sql.session.timeZone=UTC
        --packages org.apache.spark:spark-avro_2.12:3.2.1
    )
    # Define Spark network settings in K8s cluster
    spark_submit_args+=(
        --conf "spark.driver.bindAddress=${ARG_SPARKBINDADDRESS}"
        --conf "spark.driver.host=${ARG_NODENAME}"
        --conf "spark.driver.port=${ARG_SPARKPORT0}"
        --conf "spark.driver.blockManager.port=${ARG_SPARKPORT1}"
    )
    py_input_args=(
        --hdfs_out_dir "$hdfs_out_dir"
    )
    # Run
    spark-submit \
        "${spark_submit_args[@]}" \
        "${SCRIPT_DIR}/../src/python/CMSSpark/${spark_py_file}" \
        "${py_input_args[@]}" \
        >>"${LOG_DIR}/${log_file}" 2>&1

    echo "$(date --rfc-3339=seconds)" "[INFO] Spark job for ${spark_py_file} finished"
    echo "$(date --rfc-3339=seconds)" "[INFO] Last 10 lines of Spark job log"
    tail -10 "${LOG_DIR}/${log_file}"

    # Give read access to new dumps for all users
    hadoop fs -chmod -R o+rx "$hdfs_out_dir"/

    # Local directory in K8s pod to store Spark results which will be copied from HDFS
    local_json_merge_dir=$ARG_WDIR/results

    # Create dir silently
    mkdir -p "$local_json_merge_dir"

    local_json_merge_file=$local_json_merge_dir/"${collection}.json"

    # Delete if old one exists
    rm -rf "$local_json_merge_file"

    # Copy files from HDFS to LOCAL directory as a single file
    hadoop fs -getmerge "$hdfs_out_dir"/part-*.json "$local_json_merge_file"

    mongoimport --host "$ARG_MONGOHOST" \
        --drop \
        --port "$ARG_MONGOPORT" \
        --username "$ARG_MONGOUSER" \
        --password "$ARG_MONGOPASS" \
        --authenticationDatabase "$ARG_MONGOAUTHDB" \
        --db "$ARG_MONGOWRITEDB" \
        --collection "$collection" \
        --file "$local_json_merge_file" \
        --type=json

    echo "$(date --rfc-3339=seconds)" "[INFO] Mongo import finished."
    # ------------------------------------------------------------------------------------------------   POST DELETIONS
    # Delete yesterdays dumps
    hadoop fs -rm -r -f -skipTrash "$yesterday_hdfs_out_dir"
    echo "$(date --rfc-3339=seconds)" "[INFO] HDFS results of previous day is deleted: ${yesterday_hdfs_out_dir}"
}

###################### Run datasets
# Arrange a temporary HDFS directory that current Kerberos user can use for datasets collection
datasets_hdfs_out="/tmp/${KERBEROS_USER}/rucio_ds_for_mongo/$(date +%Y-%m-%d)"
datasets_hdfs_out_yesterday="/tmp/${KERBEROS_USER}/rucio_ds_for_mongo/$(date -d "yesterday" '+%Y-%m-%d')"
run_spark_and_mongo_import "rucio_all_datasets.py" "$datasets_hdfs_out" "spark-job-datasets.log" "datasets" "$datasets_hdfs_out_yesterday" 2>&1

###################### Run detailed datasets
# Arrange a temporary HDFS directory that current Kerberos user can use for detailed_datasets collection
detailed_datasets_hdfs_out="/tmp/${KERBEROS_USER}/rucio_detailed_ds_for_mongo/$(date +%Y-%m-%d)"
detailed_datasets_hdfs_out_yesterday="/tmp/${KERBEROS_USER}/rucio_detailed_ds_for_mongo/$(date -d "yesterday" '+%Y-%m-%d')"
run_spark_and_mongo_import "rucio_all_detailed_datasets.py" "$detailed_datasets_hdfs_out" "spark-job-detailed-datasets.log" "detailed_datasets" "$detailed_datasets_hdfs_out_yesterday" 2>&1

# ---------------------------------------------------------------------------------------- SOURCE TIMESTAMP MONGOIMPORT
# Write current date to json file
echo "{\"createdAt\": \"$(date +%Y-%m-%d)\"}" >source_timestamp.json
mongoimport --host "$ARG_MONGOHOST" \
    --drop \
    --port "$ARG_MONGOPORT" \
    --username "$ARG_MONGOUSER" \
    --password "$ARG_MONGOPASS" \
    --authenticationDatabase "$ARG_MONGOAUTHDB" \
    --db "$ARG_MONGOWRITEDB" \
    --collection "source_timestamp" \
    --file source_timestamp.json \
    --type=json

# -------------------------------------------------------------------------------------------------------------- FINISH
# Print process wall clock time
echo "$(date --rfc-3339=seconds)" "[INFO] All finished. Time spent: $(secs_to_human "$(($(date +%s) - START_TIME))")"
