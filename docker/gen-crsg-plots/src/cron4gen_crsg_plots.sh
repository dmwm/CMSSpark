#!/bin/bash
# shellcheck disable=SC2068
set -e
##H cron4gen_crsg_plots.sh <ARGS>
##H    Generate the plots for the CRSG report
##H    In the -PY PARAMETERS- section, comment the parameter if you want to use the default value. using an empty array will set the parameter without value.
##H
##H Example: cron4gen_crsg_plots.sh --keytab ./keytab --output <DIR> --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab             : Kerberos auth file: secrets/keytab
##H   - output             : Output directory. If not given, $HOME/output will be used. I.e /eos/user/c/cmsmonit/www/EventCountPlots
##H   - p1, p2, host, wdir : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test               : Flag that will process 3 months of data instead of 1 year.
##H How to test: Just provide a test directory as output directory.
##H
TZ=UTC
START_TIME=$(date +%s)
myname=$(basename "$0")
script_dir="$(cd "$(dirname "$0")" && pwd)"
. /data/utils/common_utils.sh

if [ "$1" == "" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    util_usage_help
    exit 0
fi
util_cron_send_start "$myname" "1M"
export PYTHONPATH=$script_dir/../src/python:$PYTHONPATH
unset -v KEYTAB_SECRET OUTPUT_DIR PORT1 PORT2 K8SHOST WDIR IS_TEST

# ------------------------------------------------------------------------------------------------------------- PREPARE
util_input_args_parser $@

util4logi "Parameters: KEYTAB_SECRET:${KEYTAB_SECRET} OUTPUT_DIR:${OUTPUT_DIR} PORT1:${PORT1} PORT2:${PORT2} K8SHOST:${K8SHOST} WDIR:${WDIR} IS_TEST:${IS_TEST}"
util_check_vars PORT1 PORT2 K8SHOST
util_setup_spark_k8s

KERBEROS_USER=$(util_kerberos_auth_with_keytab "$KEYTAB_SECRET")
util4logi "authenticated with Kerberos user: ${KERBEROS_USER}"
util_check_and_create_dir "$OUTPUT_DIR"

# ----------------------------------------------------------------------------------------------------------- FUNCTIONS
util4logi "${myname} Spark Job is starting..."

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
    --conf spark.shuffle.useOldFetchProtocol=true
    --driver-memory=4g --executor-memory=8g --executor-cores=4 --num-executors=30
    --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${K8SHOST}"
    --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.4.0
)

# run spark function
function run_spark() {
    spark-submit "${spark_submit_args[@]}" "$script_dir/dbs_event_count_plot.py" "$@"
}

function gen_plot() {
    # Second argument is optional, it is start month in yyyy/MM format
    PREFIX=${1:-"F9"}
    SK_VAR="${PREFIX}_SKIMS"
    SK_EXPAND="${SK_VAR}[@]"
    SKIMS_PAR=$([[ -v "$SK_VAR" ]] && echo "--skims ${!SK_EXPAND}" || echo "")
    TIERS_VAR="${PREFIX}_TIERS"
    TIERS_EXPAND="${TIERS_VAR}[@]"
    TIERS_PAR=$([[ -v "$TIERS_VAR" ]] && echo "--tiers ${!TIERS_EXPAND}" || echo "")
    REMOVE_VAR="${PREFIX}_REMOVE"
    REMOVE_EXPAND="${REMOVE_VAR}[@]"
    REMOVE_PAR=$([[ -v "$REMOVE_VAR" ]] && echo "--remove ${!REMOVE_EXPAND}" || echo "")
    if [ $# -eq 2 ]; then
        util4logi "start month: $2"
        run_spark --attributes "$ATTRIBUTES_FILE" --start_month "$2" --output_folder "$OUTPUT_DIR/$PREFIX" --output_format "$OUTPUT_FORMAT" $TIERS_PAR $SKIMS_PAR $REMOVE_PAR $ADDITIONAL_PARAMS
        IMAGE=$(cat "${OUTPUT_DIR}/${PREFIX}/${OUTPUT_FORMAT}_output_path_for_ln.txt")
    else
        run_spark --attributes "$ATTRIBUTES_FILE" --output_folder "$OUTPUT_DIR/$PREFIX" --output_format "$OUTPUT_FORMAT" $TIERS_PAR $SKIMS_PAR $REMOVE_PAR $ADDITIONAL_PARAMS
        IMAGE=$(cat "${OUTPUT_DIR}/${PREFIX}/${OUTPUT_FORMAT}_output_path_for_ln.txt")
    fi
    util4logi "Image: $IMAGE"
    ln -sf "$IMAGE" "$OUTPUT_DIR/$PREFIX/event_count_latest.$OUTPUT_FORMAT"
}
# ------------------------------------------------------------------------------------------------------- PY PARAMETERS
# DO NOT MODIFY ANY COMMENTED OUT OR UNUSED PARAMETERS, they are accessed by gen_plot() function
# PARAMETERS FIGURE 9
F9_TIERS="GEN,GEN-SIM,GEN-RAW,GEN-SIM-RECO,AODSIM,MINIAODSIM,RAWAODSIM,NANOAODSIM,GEN-SIM-DIGI-RAW,GEN-SIM-RAW,GEN-SIM-DIGI-RECO"
#F9_SKIMS=()
#F9_REMOVE=()
# PARAMETERS FIGURE 10
F10_TIERS="RAW,RECO,AOD,RAW-RECO,USER,MINIAOD,NANOAOD"
F10_SKIMS="PromptReco,Parking"
F10_REMOVE="test,backfill,StoreResults,monitor,Error/,Scouting,MiniDaq,/Alca,L1Accept,L1EG,L1Jet,L1Mu,PhysicsDST,VdM,/Hcal,express,Interfill,Bunnies,REPLAY,LogError"

OUTPUT_FORMAT="png"
# ADDITIONAL_PARAMS="${@:3}"
ADDITIONAL_PARAMS=""
ATTRIBUTES_FILE="$script_dir/event_count_attributes.json"

# If test, process only 1 month
if [[ "$IS_TEST" == 1 ]]; then
    util4logi "Running test"
    start_month=$(date -d "$(date) -1 month" +%Y/%m)
    gen_plot "F10" "$start_month" 2>&1
    gen_plot "F9" "$start_month" 2>&1
else
    gen_plot "F10" 2>&1
    gen_plot "F9" 2>&1
fi

duration=$(($(date +%s) - START_TIME))
util_cron_send_end "$myname" "1M" 0
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
