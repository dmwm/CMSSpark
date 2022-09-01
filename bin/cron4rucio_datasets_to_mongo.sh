#!/bin/bash
set -e
##H Can only run in K8s, you may modify to run in local by arranging env vars
##H
##H cron4rucio_datasets_to_mongo.sh
##H    Cron job of rucio_ds_mongo.py which runs Spark job to get Rucio datasets and writes to HDFS directory.
##H    After writing data to HDFS directory, it copies HDFS files to LOCAL directory as a single file.
##H
##H Arguments:
##H   - keytab              : Kerberos auth file to connect Spark Analytix cluster (cmsmonit)
##H   - mongohost           : MongoDB host
##H   - mongoport           : MongoDB port
##H   - mongouser           : MongoDB user which has write access to required MongoDB database/collection
##H   - mongopass           : MongoDB user password
##H   - mongowritedb        : MongoDB database name that results will be written
##H   - mongoauthdb         : MongoDB database for authentication. Required for mongoimport `--authenticationDatabase` argument
##H   - p1, p2, host, wdir  : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H
##H Usage Example:
##H    ./cron4rucio_datasets_to_mongo.sh --keytab ./keytab --mongohost $MONGO_HOST --mongoport $MONGO_PORT \
##H                                      --mongouser $MONGO_ROOT_USERNAME --mongopass $MONGO_ROOT_PASSWORD --mongowritedb rucio --mongoauthdb admin \
##H                                       --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H
##H References:
##H   - CMSSpark/bin/cron4rucio_datasets_daily_stats.sh
##H How to test:
##H   - You can test just giving different '--mongowritedb'
##H   - OR, you can test just giving different collection names for !ALL! collection names that will be used. For example, put 'test_' prefix in K8s ConfigMap
##H   - OR, you can use totally different MongoDB instance
##H   - All of them will test safely. Of course use same test configs(DB name, collection names) in Go web service
##H
TZ=UTC
START_TIME=$(date +%s)
script_dir="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"
# get common util functions
. "$script_dir"/utils/common_utils.sh

trap 'onFailExit' ERR
onFailExit() {
    util4loge "finished with error!" || exit 1
}
# ------------------------------------------------------------------------------------------------------- GET USER ARGS
unset -v KEYTAB_SECRET ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB PORT1 PORT2 K8SHOST WDIR help
[ "$#" -ne 0 ] || util_usage_help

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,mongohost:,mongoport:,mongouser:,mongopass:,mongowritedb:,mongoauthdb:,p1:,p2:,host:,wdir:,,help -- "$@")

VALID_ARGS=$?
if [ "$VALID_ARGS" != "0" ]; then
    util_usage_help
fi

util4logi "given arguments: $PARSED_ARGS"
eval set -- "$PARSED_ARGS"

while [[ $# -gt 0 ]]; do
    case "$1" in
    --keytab)       KEYTAB_SECRET=$2     ; shift 2 ;;
    --mongohost)    ARG_MONGOHOST=$2     ; shift 2 ;;
    --mongoport)    ARG_MONGOPORT=$2     ; shift 2 ;;
    --mongouser)    ARG_MONGOUSER=$2     ; shift 2 ;;
    --mongopass)    ARG_MONGOPASS=$2     ; shift 2 ;;
    --mongowritedb) ARG_MONGOWRITEDB=$2  ; shift 2 ;;
    --mongoauthdb)  ARG_MONGOAUTHDB=$2   ; shift 2 ;;
    --p1)           PORT1=$2             ; shift 2 ;;
    --p2)           PORT2=$2             ; shift 2 ;;
    --host)         K8SHOST=$2           ; shift 2 ;;
    --wdir)         WDIR=$2              ; shift 2 ;;
    -h | --help)    help=1               ; shift   ;;
    *)              break                          ;;
    esac
done

#
if [[ "$help" == 1 ]]; then
    util_usage_help
fi

# ------------------------------------------------------------------------------------------------------------- PREPARE
# Define logs path for Spark imports which produce lots of info logs
LOG_DIR="$WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"

# Check variables are set
util_check_vars ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB PORT1 PORT2 K8SHOST WDIR

# Check files exist
util_check_files "$KEYTAB_SECRET"

# Check commands/CLIs exist
util_check_cmd mongoimport
util_check_cmd mongosh

# INITIALIZE ANALYTIX SPARK3
util_setup_spark_k8s

# Authenticate kerberos and get principle user name
KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
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

    util4logi "spark job for ${spark_py_file} starting"
    export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
    spark_submit_args=(
        --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC --conf "spark.driver.bindAddress=0.0.0.0"
        --driver-memory=8g --executor-memory=8g --packages org.apache.spark:spark-avro_2.12:3.2.1
        --conf "spark.driver.host=${K8SHOST}" --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    )
    py_input_args=(--hdfs_out_dir "$hdfs_out_dir")

    # Run
    spark-submit "${spark_submit_args[@]}" "${script_dir}/../src/python/CMSSpark/${spark_py_file}" \
        "${py_input_args[@]}" >>"${LOG_DIR}/${log_file}" 2>&1

    util4logi "spark job for ${spark_py_file} finished"
    util4logi "last 10 lines of Spark job log"
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

    mongoimport --drop --type=json --host "$ARG_MONGOHOST" --port "$ARG_MONGOPORT" \
        --username "$ARG_MONGOUSER" --password "$ARG_MONGOPASS" --authenticationDatabase "$ARG_MONGOAUTHDB" --db "$ARG_MONGOWRITEDB" \
        --collection "$collection" --file "$local_json_merge_file"

    util4logi "mongoimport finished."
    # ------------------------------------------------------------------------------------------------   POST DELETIONS
    # Delete yesterdays dumps
    hadoop fs -rm -r -f -skipTrash "$yesterday_hdfs_out_dir"
    util4logi "HDFS results of previous day is deleted: ${yesterday_hdfs_out_dir}"
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

mongoimport --drop --type=json --host "$ARG_MONGOHOST" --port "$ARG_MONGOPORT" --username "$ARG_MONGOUSER" --password "$ARG_MONGOPASS" \
    --authenticationDatabase "$ARG_MONGOAUTHDB" --db "$ARG_MONGOWRITEDB" --collection "source_timestamp" --file source_timestamp.json

# -------------------------------------------------------------------------------------------------------------- FINISH
# Print process wall clock time
duration=$(($(date +%s) - START_TIME))
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
