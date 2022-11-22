#!/bin/bash
# shellcheck disable=SC2068
set -e
##H Can only run in K8s, you may modify to run in local by arranging env vars
##H
##H cron4rucio_hdfs2mongo.sh
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
##H    ./cron4rucio_hdfs2mongo.sh --keytab ./keytab --mongohost $MONGO_HOST --mongoport $MONGO_PORT \
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
myname=$(basename "$0")
script_dir="$(cd "$(dirname "$0")" && pwd)"
. "$script_dir"/../utils/common_utils.sh

if [ "$1" == "" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    util_usage_help
    exit 0
fi
util_cron_send_start "$myname"
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
unset -v KEYTAB_SECRET ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB WDIR help
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} ARG_MONGOHOST:${ARG_MONGOHOST} ARG_MONGOPORT:${ARG_MONGOPORT} ARG_MONGOUSER:${ARG_MONGOUSER} ARG_MONGOWRITEDB:${ARG_MONGOWRITEDB} ARG_MONGOAUTHDB:${ARG_MONGOAUTHDB} WDIR:${WDIR}"
util_check_vars ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB WDIR
util_setup_spark_k8s

# Check commands/CLIs exist
util_check_cmd mongoimport
util_check_cmd mongosh

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"

# ---------------------------------------------------------------------------------------------------- RUN MONGO IMPORT
# arg1: hdfs output directory
# arg2: mongodb collection name [datasets or detailed_datasets]
function run_mongo_import() {
    hdfs_out_dir=$1
    collection=$2
    # Local directory in K8s pod to store Spark results which will be copied from HDFS.
    # Create directory if not exist, delete file if exists.
    local_json_merge_dir=$WDIR/results
    mkdir -p "$local_json_merge_dir"
    local_json_merge_file=$local_json_merge_dir/"${collection}.json"
    rm -rf "$local_json_merge_file"

    # Copy files from HDFS to LOCAL directory as a single file
    hadoop fs -getmerge "$hdfs_out_dir"/part-*.json "$local_json_merge_file"

    mongoimport --drop --type=json \
        --host "$ARG_MONGOHOST" --port "$ARG_MONGOPORT" --username "$ARG_MONGOUSER" --password "$ARG_MONGOPASS" \
        --authenticationDatabase "$ARG_MONGOAUTHDB" --db "$ARG_MONGOWRITEDB" \
        --collection "$collection" --file "$local_json_merge_file"
    util4logi "Mongoimport finished. ${hdfs_out_dir} imported to collection: ${collection}"
}

###################### Import datasets
# Arrange a temporary HDFS directory that current Kerberos user can use for datasets collection
datasets_hdfs_path="/tmp/${KERBEROS_USER}/rucio_ds_for_mongo/$(date +%Y-%m-%d)"
run_mongo_import "$datasets_hdfs_path" "datasets" 2>&1

###################### Import detailed datasets
detailed_datasets_hdfs_path="/tmp/${KERBEROS_USER}/rucio_detailed_ds_for_mongo/$(date +%Y-%m-%d)"
run_mongo_import "$detailed_datasets_hdfs_path" "detailed_datasets" 2>&1

# ---------------------------------------------------------------------------------------- SOURCE TIMESTAMP MONGOIMPORT
# Write current date to json file and import it to MongoDB "source_timestamp" collection for Go Web Page.
echo "{\"createdAt\": \"$(date +%Y-%m-%d)\"}" >source_timestamp.json
mongoimport --drop --type=json \
    --host "$ARG_MONGOHOST" --port "$ARG_MONGOPORT" --username "$ARG_MONGOUSER" --password "$ARG_MONGOPASS" \
    --authenticationDatabase "$ARG_MONGOAUTHDB" --db "$ARG_MONGOWRITEDB" \
    --collection "source_timestamp" --file source_timestamp.json

util4logi "source_timestamp collection is updated with current date"

# ---------------------------------------------------------------------------------------------- MongoDB INDEX CREATION
mongosh --host "$ARG_MONGOHOST" --port "$ARG_MONGOPORT" --username "$ARG_MONGOUSER" --password "$ARG_MONGOPASS" \
    --authenticationDatabase "$ARG_MONGOAUTHDB" <"$script_dir"/createindexes.js
util4logi "MongoDB indexes are created for datasets and detailed_datasets collections"

# -------------------------------------------------------------------------------------------------------------- FINISH
# Print process wall clock time
duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
