#!/bin/bash
set -e
##H Can only run in K8s
##H k8s_condor_cpu_efficiency.sh
##H   Creates a html page which provides cpu efficiency for workflows
##H   Each CMS job type(prod,analysis,folding@home, test) will have separate url
##H
##H Usage Example :
##H    k8s_condor_cpu_efficiency.sh --keytab ./keytab --out /eos/foo --lastndays 30 --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab              : Kerberos auth file: secrets/kerberos
##H   - out                 : Base EOS directory to write datasets statistics in html format.
##H   - lastndays           : Last N days that HDFS condor data will be processed
##H   - p1, p2, host, wdir  : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H How to test:
##H   - You can test by just giving different output directory to '--out'
##H
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
unset -v KEYTAB_SECRET MAIN_OUTPUT_DIR LAST_N_DAYS PORT1 PORT2 K8SHOST WDIR help
[ "$#" -ne 0 ] || util_usage_help

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,out:,lastndays:,p1:,p2:,host:,wdir:,,help -- "$@")
VALID_ARGS=$?
if [ "$VALID_ARGS" != "0" ]; then
    util_usage_help
fi

util4logi "Given arguments: $PARSED_ARGS"
eval set -- "$PARSED_ARGS"

while [[ $# -gt 0 ]]; do
    case "$1" in
    --keytab)        KEYTAB_SECRET=$2    ; shift 2 ;;
    --out)           MAIN_OUTPUT_DIR=$2  ; shift 2 ;;
    --lastndays)     LAST_N_DAYS=$2      ; shift 2 ;;
    --p1)            PORT1=$2            ; shift 2 ;;
    --p2)            PORT2=$2            ; shift 2 ;;
    --host)          K8SHOST=$2          ; shift 2 ;;
    --wdir)          WDIR=$2             ; shift 2 ;;
    -h | --help)     help=1              ; shift   ;;
    *)               break                         ;;
    esac
done

if [[ "$help" == 1 ]]; then
    util_usage_help
fi

# ------------------------------------------------------------------------------------------------------------- PREPARE
TZ=UTC
START_TIME=$(date +%s)

export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH

# Define logs path for Spark imports which produce lots of info logs
LOG_DIR="$WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"

# Authenticate kerberos and get principle user name
KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with ${KERBEROS_USER} user's keytab"

# check files exist, should run after authentication since out dir can be EOS directory
util_check_files "$MAIN_OUTPUT_DIR"
# check variables set
util_check_vars PORT1 PORT2 K8SHOST WDIR

# INITIALIZE ANALYTIX SPARK3
util_setup_spark_k8s

# ----------------------------------------------------------------------------------------------------------------- RUN
currentDir=$(
    cd "$(dirname "$0")" && pwd
)
spark_confs=(
    --master yarn --conf "spark.driver.bindAddress=0.0.0.0" --conf spark.ui.showConsoleProgress=false
    --conf "spark.executor.memory=8g" --conf "spark.driver.memory=4g"
    --conf "spark.driver.host=${K8SHOST}" --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
)

# cpu_eff
OUTPUT_DIR="${MAIN_OUTPUT_DIR}/cpu_eff"

# cpu_eff_outlier
OUTPUT_DIR_OUTLIER="${OUTPUT_DIR}_outlier"
CMS_TYPES=("analysis" "production" "folding@home" "test")

# ---------------------------------------------------------------------------------------------------------------------
util4logi "Condor cpu efficiency starts"
for type in "${CMS_TYPES[@]}"; do
    SUB_FOLDER=$(echo "$type" | sed -e 's/[^[:alnum:]]/-/g' | tr -s '-' | tr '[:upper:]' '[:lower:]')
    util4logi "Starting spark jobs for cpu_eff_outlier=0, folder: ${OUTPUT_DIR}, CMS_TYPE: ${SUB_FOLDER}"
    spark-submit "${spark_confs[@]}" "$currentDir/../src/python/CMSSpark/condor_cpu_efficiency.py" \
        --cms_type "$type" --output_folder "$OUTPUT_DIR/$SUB_FOLDER" --last_n_days "$LAST_N_DAYS" --cpu_eff_outlier=0
done

for type in "${CMS_TYPES[@]}"; do
    SUB_FOLDER=$(echo "$type" | sed -e 's/[^[:alnum:]]/-/g' | tr -s '-' | tr '[:upper:]' '[:lower:]')
    util4logi "Starting spark jobs for cpu_eff_outlier=1, folder: ${OUTPUT_DIR}, CMS_TYPE: ${SUB_FOLDER}"
    spark-submit "${spark_confs[@]}" "$currentDir/../src/python/CMSSpark/condor_cpu_efficiency.py" \
        --cms_type "$type" --output_folder "$OUTPUT_DIR_OUTLIER/$SUB_FOLDER" --last_n_days "$LAST_N_DAYS" --cpu_eff_outlier=1
done

# We should clean old files which are not used in the web site anymore.
util4logi "deleting html and png files older than 60 days in dir: ${OUTPUT_DIR}"
find "$MAIN_OUTPUT_DIR" -type f \( -name '*.html' -o -name '*.png' \) -mtime +60 -delete
util4logi "old file deletion is finished"

util4logi "Condor cpu efficiency finished. Time spent: $(util_secs_to_human "$(($(date +%s) - START_TIME))")"
