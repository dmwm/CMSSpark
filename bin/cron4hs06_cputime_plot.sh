#!/bin/bash
set -e
##H cron4hs06_cputime_plot.sh
##H    Creates HS06 CPU time plots
##H    This script will generate two plots and two datasets, one with monthly values and other with weekly values.
##H    Spark job will cover a 1 year period, ending in the first day of the current month (without including it)
##H
##H Usage: cron4hs06_cputime_plot.sh <ARGS>
##H Example :
##H    cron4hs06_cputime_plot.sh --keytab ./keytab --output <DIR> --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab             : Kerberos auth file: secrets/keytab
##H   - output             : Output directory. If not given, $HOME/output will be used. I.e /eos/user/c/cmsmonit/www/hs06cputime
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
    OUTPUT_DIR=$HOME/output_hs06_cpu
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
    spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/condor_hs06coreHrPlot.py" "$@"
}

END_DATE="$(date +%Y-%m-01)"
START_DATE="$(date -d "$END_DATE -1 year" +%Y-%m-01)"
# If test, process only 2 months
if [[ "$IS_TEST" == 1 ]]; then
    START_DATE="$(date -d "$END_DATE -2 month" +%Y-%m-01)"
fi

util4logi "Plots from $START_DATE to $END_DATE"

# T2 sites excluding cern.
run_spark --generate_plots --by "month" --output_folder "$OUTPUT_DIR/T2" --start_date "$START_DATE" --end_date "$END_DATE"
/bin/bash "$script_dir/run_hs06cputime_plot.sh" --generate_plots --by "weekofyear" --output_folder "$OUTPUT_DIR/T2" --start_date "$START_DATE" --end_date "$END_DATE"

ln -s -f "$OUTPUT_DIR/T2/HS06CpuTimeHr_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/T2/HS06CpuTimeHr_month_latest.csv"
ln -s -f "$OUTPUT_DIR/T2/HS06CpuTimeHr_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/T2/HS06CpuTimeHr_month_latest.png"
ln -s -f "$OUTPUT_DIR/T2/HS06CpuTimeHr_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/T2/HS06CpuTimeHr_weekofyear_latest.csv"
ln -s -f "$OUTPUT_DIR/T2/HS06CpuTimeHr_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/T2/HS06CpuTimeHr_weekofyear_latest.png"

# T1 sites
run_spark --generate_plots --by "month" --include_re "T1_.*" --exclude_re "^$" --output_folder "$OUTPUT_DIR/T1" --start_date "$START_DATE" --end_date "$END_DATE"
run_spark --generate_plots --by "weekofyear" --include_re "T1_.*" --exclude_re "^$" --output_folder "$OUTPUT_DIR/T1" --start_date "$START_DATE" --end_date "$END_DATE"

ln -s -f "$OUTPUT_DIR/T1/HS06CpuTimeHr_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/T1/HS06CpuTimeHr_month_latest.csv"
ln -s -f "$OUTPUT_DIR/T1/HS06CpuTimeHr_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/T1/HS06CpuTimeHr_month_latest.png"
ln -s -f "$OUTPUT_DIR/T1/HS06CpuTimeHr_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/T1/HS06CpuTimeHr_weekofyear_latest.csv"
ln -s -f "$OUTPUT_DIR/T1/HS06CpuTimeHr_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/T1/HS06CpuTimeHr_weekofyear_latest.png"

duration=$(($(date +%s) - START_TIME))
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
