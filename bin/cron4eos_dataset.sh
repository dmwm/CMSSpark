#!/bin/bash
set -e
##H cron4eos_dataset.sh
##H    Creates EOS datasets plots
##H
##H Usage: cron4eos_dataset.sh <ARGS>
##H Example :
##H    cron4eos_dataset.sh --keytab ./keytab --output <DIR> --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab             : Kerberos auth file: secrets/keytab
##H   - output             : Output directory. If not given, $HOME/output will be used. I.e /eos/user/c/cmsmonit/www/EOS/data
##H   - p1, p2, host, wdir : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test               : Flag that will process 2 months of data instead of 1 year.
##H How to test:
##H   - Just provide test directory as output directory.
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
unset -v KEYTAB_SECRET OUTPUT_DIR PORT1 PORT2 K8SHOST WDIR IS_TEST help
[ "$#" -ne 0 ] || util_usage_help

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,output:,p1:,p2:,host:,wdir:,test,help -- "$@")
VALID_ARGS=$?
if [ "$VALID_ARGS" != "0" ]; then
    util_usage_help
fi

util4logi "Given arguments: $PARSED_ARGS"
eval set -- "$PARSED_ARGS"
while [[ $# -gt 0 ]]; do
    case "$1" in
    --keytab)    KEYTAB_SECRET=$2     ; shift 2 ;;
    --output)    OUTPUT_DIR=$2        ; shift 2 ;;
    --p1)        PORT1=$2             ; shift 2 ;;
    --p2)        PORT2=$2             ; shift 2 ;;
    --host)      K8SHOST=$2           ; shift 2 ;;
    --wdir)      WDIR=$2              ; shift 2 ;;
    --test)      IS_TEST=1            ; shift   ;;
    -h | --help) help=1               ; shift   ;;
    *)           break                          ;;
    esac
done


if [[ "$help" == 1 ]]; then
    util_usage_help
fi
# ------------------------------------------------------------------------------------------------------------- PREPARE
# check variables set
util_check_vars PORT1 PORT2 K8SHOST WDIR
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH

# INITIALIZE ANALYTIX SPARK3
util_setup_spark_k8s

# Authenticate kerberos and get principle user name. Required for EOS
KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"

# Check and set OUTPUT_DIR
if [[ -n "$OUTPUT_DIR" ]]; then
    if [ ! -d "$OUTPUT_DIR" ]; then
        util4logw "output directory does not exist, creating..: ${OUTPUT_DIR}}"
        if [ "$(mkdir -p "$OUTPUT_DIR" >/dev/null)" -ne 0 ]; then
            util4loge "cannot create output directory: ${OUTPUT_DIR}"
            exit 1
        fi
    fi
else
    OUTPUT_DIR=$HOME/output_crab_uu
fi
# ----------------------------------------------------------------------------------------------------------------- RUN
util4logi "output directory: ${OUTPUT_DIR}"
util4logi "spark job starting.."
spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf "spark.driver.bindAddress=0.0.0.0" --driver-memory=8g --executor-memory=8g
    --conf "spark.driver.host=${K8SHOST}" --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.2.1
)
# run spark function
function run_spark() {
    spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/dbs_hdfs_eos.py" "$@"
    # Do not print progress bar: showConsoleProgress=false
}

# Generate the previous month intermediate files
month_="$(date -d "$(date +%Y-%m-15) -1 month" +%Y/%m)"
#month_=2020/11
util4logi "Processing month " "$month_"
mode_="append"
run_spark run_update --mode "$mode_" "$month_/*"

# Generate the totals csv for one year
end_=$(date -d "$month_/01 +1 month -1 day" +%Y%m%d)
start_=$(date -d "$end_ -1 year +1 day" +%Y%m%d)
# If test, process only 2 months
if [[ "$IS_TEST" == 1 ]]; then
    start_=$(date -d "$end_ -2 month +1 day" +%Y%m%d)
fi

util4logi "Totals for dataset/file from $start_ to $end_"
run_spark run_report_totals --outputDir "$OUTPUT_DIR" "$start_" "$end_"

if [[ -f "$OUTPUT_DIR/dataset_totals.csv" ]]; then
    mv "$OUTPUT_DIR/dataset_totals.csv" "$OUTPUT_DIR/dataset_totals_yearly_${start_}_${end_}.csv"
    cp -f "$OUTPUT_DIR/dataset_totals_yearly_${start_}_${end_}.csv" "$OUTPUT_DIR/dataset_totals_rolling_year.csv"
    ln -s -f "$OUTPUT_DIR/top_total_rb_${start_}-${end_}.png" "$OUTPUT_DIR/topDS_last_rolling_year.png"
    gzip "$OUTPUT_DIR/dataset_totals_yearly_${start_}_${end_}.csv"
fi

# Generate and concat the last month data.
start_=$(date -d "$month_/01" +%Y%m%d)
util4logi "Totals for the last month, $start_ to $end_"
run_spark run_report_totals --outputDir "$OUTPUT_DIR" "$start_" "$end_"
if [[ -f "$OUTPUT_DIR/dataset_totals.csv" ]]; then
    mv "$OUTPUT_DIR/dataset_totals.csv" "$OUTPUT_DIR/dataset_totals_${start_}_${end_}.csv"
    ln -s -f "$OUTPUT_DIR/dataset_totals_${start_}_${end_}.csv" "$OUTPUT_DIR/dataset_totals_last_month.csv"
    ln -s -f "$OUTPUT_DIR/top_total_rb_${start_}_${end_}.png" "$OUTPUT_DIR/topDS_last_rolling_month.png"
fi

duration=$(($(date +%s) - START_TIME))
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
