#!/bin/bash
set -e
##H Can only run in K8s, you may modify to run in local by arranging env vars
##H
##H cron4rucio_hdfs_to_mongo.sh
##H    Gets "datasets" and "detailed_datasets" HDFS results to LOCAL directory and send to MongoDB.
##H
##H Arguments:
##H   - keytab        : Kerberos auth file to connect Spark Analytix cluster (cmsmonit)
##H   - mongohost     : MongoDB host
##H   - mongoport     : MongoDB port
##H   - mongouser     : MongoDB user which has write access to required MongoDB database/collection
##H   - mongopass     : MongoDB user password
##H   - mongowritedb  : MongoDB database name that results will be written
##H   - mongoauthdb   : MongoDB database for authentication. Required for mongoimport `--authenticationDatabase` argument
##H   - wdir          : working directory
##H
##H Usage Example:
##H    ./cron4rucio_hdfs_to_mongo.sh --keytab ./keytab --mongohost $MONGO_HOST --mongoport $MONGO_PORT \
##H        --mongouser $MONGO_ROOT_USERNAME --mongopass $MONGO_ROOT_PASSWORD --mongowritedb rucio --mongoauthdb admin --wdir $WDIR
##H
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
. "$script_dir"/../utils/common_utils.sh

trap 'onFailExit' ERR
onFailExit() {
    util4loge "finished with error!" || exit 1
}
# ------------------------------------------------------------------------------------------------------- GET USER ARGS
unset -v KEYTAB_SECRET ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB WDIR help
[ "$#" -ne 0 ] || util_usage_help

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,mongohost:,mongoport:,mongouser:,mongopass:,mongowritedb:,mongoauthdb:,wdir:,,help -- "$@")

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
util_check_vars ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB WDIR

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
# arg1: hdfs output directory
# arg2: mongodb collection name [datasets or detailed_datasets]
function run_mongo_import() {
    hdfs_out_dir=$1
    collection=$2
    # Local directory in K8s pod to store Spark results which will be copied from HDFS. Create directory if not exist, delete file if exists.
    local_json_merge_dir=$WDIR/results
    mkdir -p "$local_json_merge_dir"
    local_json_merge_file=$local_json_merge_dir/"${collection}.json"
    rm -rf "$local_json_merge_file"

    # Copy files from HDFS to LOCAL directory as a single file
    hadoop fs -getmerge "$hdfs_out_dir"/part-*.json "$local_json_merge_file"

    mongoimport --drop --type=json --host "$ARG_MONGOHOST" --port "$ARG_MONGOPORT" --username "$ARG_MONGOUSER" \
        --password "$ARG_MONGOPASS" --authenticationDatabase "$ARG_MONGOAUTHDB" --db "$ARG_MONGOWRITEDB" \
        --collection "$collection" --file "$local_json_merge_file"
    util4logi "mongoimport finished for  ${collection}"
}

###################### Run datasets
# Arrange a temporary HDFS directory that current Kerberos user can use for datasets collection
datasets_hdfs_out="/tmp/${KERBEROS_USER}/rucio_ds_for_mongo/$(date +%Y-%m-%d)"
run_mongo_import "$datasets_hdfs_out" "datasets" 2>&1

###################### Run detailed datasets
detailed_datasets_hdfs_out="/tmp/${KERBEROS_USER}/rucio_detailed_ds_for_mongo/$(date +%Y-%m-%d)"
run_mongo_import "$detailed_datasets_hdfs_out" "detailed_datasets" 2>&1

# ---------------------------------------------------------------------------------------- SOURCE TIMESTAMP MONGOIMPORT
# Write current date to json file and import to MongoDB "source_timestamp" collection for Go Web Page.
echo "{\"createdAt\": \"$(date +%Y-%m-%d)\"}" >source_timestamp.json
mongoimport --drop --type=json --host "$ARG_MONGOHOST" --port "$ARG_MONGOPORT" --username "$ARG_MONGOUSER" --password "$ARG_MONGOPASS" \
    --authenticationDatabase "$ARG_MONGOAUTHDB" --db "$ARG_MONGOWRITEDB" --collection "source_timestamp" --file source_timestamp.json

util4logi "source_timestamp collection is updated with current date"

# ---------------------------------------------------------------------------------------------- MongoDB INDEX CREATION
mongosh --host "$ARG_MONGOHOST" --port "$ARG_MONGOPORT" --username "$ARG_MONGOUSER" --password "$ARG_MONGOPASS" \
    --authenticationDatabase "$ARG_MONGOAUTHDB" <"$script_dir"/createindexes.js
util4logi "MongoDB indexes are created for datasets and detailed_datasets collections"

# -------------------------------------------------------------------------------------------------------------- FINISH
# Print process wall clock time
duration=$(($(date +%s) - START_TIME))
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
