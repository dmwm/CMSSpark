#!/bin/bash
# shellcheck disable=SC2068
set -e
##H Can only run in K8s
##H k8s_condor_cpu_efficiency.sh
##H   Creates a html page which provides cpu efficiency for workflows
##H   Each CMS job type(prod,analysis,folding@home, test) will have separate url
##H
##H Usage Example: k8s_condor_cpu_efficiency.sh --keytab ./keytab --output /eos/foo --lastndays 30 --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab              : Kerberos auth file: secrets/kerberos
##H   - output                 : Base EOS directory to write datasets statistics in html format.
##H   - lastndays           : Last N days that HDFS condor data will be processed
##H   - p1, p2, host, wdir  : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H How to test: You can test by just giving different output directory to '--output'
##H
TZ=UTC
START_TIME=$(date +%s)
myname=$(basename "$0")
script_dir="$(cd "$(dirname "$0")" && pwd)"
. "$script_dir"/utils/common_utils.sh

if [ "$1" == "" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    util_usage_help
    exit 0
fi
util_cron_send_start "$myname" "1d"
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH

unset -v KEYTAB_SECRET OUTPUT_DIR LAST_N_DAYS PORT1 PORT2 K8SHOST WDIR IS_TEST
# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} OUTPUT_DIR:${OUTPUT_DIR} LAST_N_DAYS:${LAST_N_DAYS} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR} IS_TEST:${IS_TEST}"
util_check_vars PORT1 PORT2 K8SHOST
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"
util_check_and_create_dir "$OUTPUT_DIR"

# Define logs path for Spark imports which produce lots of info logs
LOG_DIR="$WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"

# ----------------------------------------------------------------------------------------------------------------- RUN
spark_confs=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
    --driver-memory=4g --executor-memory=8g --executor-cores=4 --num-executors=30
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
)

# cpu_eff
CPU_EFF_DIR="${OUTPUT_DIR}/cpu_eff"

# cpu_eff_outlier
CPU_EFF_OUTLIER_DIR="${CPU_EFF_DIR}_outlier"
CMS_TYPES=("analysis" "production" "folding@home" "test")

# ---------------------------------------------------------------------------------------------------------------------
util4logi "${myname} Spark Job is starting..."
for type in "${CMS_TYPES[@]}"; do
    SUB_FOLDER=$(echo "$type" | sed -e 's/[^[:alnum:]]/-/g' | tr -s '-' | tr '[:upper:]' '[:lower:]')
    _type_output_dir="${CPU_EFF_DIR}/${SUB_FOLDER}"
    # We should clean old files and empty directories which are not used in the web site anymore.
    util4logi "deleting html and png files older than 60 days in dir: ${_type_output_dir}"
    find "${_type_output_dir}" -type f \( -name '*.html' -o -name '*.png' \) -mtime +60 -delete
    find "${_type_output_dir}" -empty -type d -delete
    util4logi "old file deletion is finished"

    util4logi "Starting spark jobs for cpu_eff_outlier=0, folder: ${CPU_EFF_DIR}, CMS_TYPE: ${SUB_FOLDER}"
    spark-submit "${spark_confs[@]}" "$script_dir/../src/python/CMSSpark/condor_cpu_efficiency.py" \
        --cms_type "$type" --output_folder "${_type_output_dir}" --last_n_days "$LAST_N_DAYS" --cpu_eff_outlier=0
done

for type in "${CMS_TYPES[@]}"; do
    SUB_FOLDER=$(echo "$type" | sed -e 's/[^[:alnum:]]/-/g' | tr -s '-' | tr '[:upper:]' '[:lower:]')
    _type_output_dir="${CPU_EFF_OUTLIER_DIR}/${SUB_FOLDER}"
    # We should clean old files and empty directories which are not used in the web site anymore.
    util4logi "deleting html and png files older than 60 days in dir: ${_type_output_dir}"
    find "${_type_output_dir}" -type f \( -name '*.html' -o -name '*.png' \) -mtime +60 -delete
    find "${_type_output_dir}" -empty -type d -delete
    util4logi "old file deletion is finished"

    util4logi "Starting spark jobs for cpu_eff_outlier=1, folder: ${CPU_EFF_OUTLIER_DIR}, CMS_TYPE: ${SUB_FOLDER}"
    spark-submit "${spark_confs[@]}" "$script_dir/../src/python/CMSSpark/condor_cpu_efficiency.py" \
        --cms_type "$type" --output_folder "${_type_output_dir}" --last_n_days "$LAST_N_DAYS" --cpu_eff_outlier=1
done

duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" "1d" 0
util4logi "Condor cpu efficiency finished., time spent: $(util_secs_to_human $duration)"
