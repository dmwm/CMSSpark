#!/bin/bash
set -e
##H cron4gen_crsg_plots.sh
##H    Generate the plots for the CRSG report
##H    In the -PY PARAMETERS- section, comment the parameter if you want to use the default value.
##H      using an empty array will set the parameter without value.
##H
##H Usage: cron4gen_crsg_plots.sh <ARGS>
##H Example :
##H    cron4gen_crsg_plots.sh --keytab ./keytab --output <DIR> --p1 32000 --p2 32001 --host $MY_NODE_NAME --wdir $WDIR
##H Arguments:
##H   - keytab             : Kerberos auth file: secrets/keytab
##H   - output             : Output directory. If not given, $HOME/output will be used. I.e /eos/user/c/cmsmonit/www/EventCountPlots
##H   - p1, p2, host, wdir : [ALL FOR K8S] p1 and p2 spark required ports(driver and blockManager), host is k8s node dns alias, wdir is working directory
##H   - test               : Flag that will process 3 months of data instead of 1 year.
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
    util_check_and_create_dir "$OUTPUT_DIR"
else
    OUTPUT_DIR=$HOME/output
fi
# ----------------------------------------------------------------------------------------------------------- FUNCTIONS
util4logi "output directory: ${OUTPUT_DIR}"
util4logi "spark job starting.."

spark_submit_args=(
    --master yarn --conf spark.ui.showConsoleProgress=false --conf "spark.driver.bindAddress=0.0.0.0" --driver-memory=8g --executor-memory=8g
    --conf "spark.driver.host=${K8SHOST}" --conf "spark.driver.port=${PORT1}" --conf "spark.driver.blockManager.port=${PORT2}"
    --packages org.apache.spark:spark-avro_2.12:3.2.1
)

# run spark function
function run_spark() {
    spark-submit "${spark_submit_args[@]}" "$script_dir/../src/python/CMSSpark/dbs_event_count_plot.py" "$@"
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
        IMAGE=$(run_spark --attributes $ATTRIBUTES_FILE --start_month "$2" --output_folder "$BASE_OUTPUT_FOLDER/$PREFIX" --output_format "$OUTPUT_FORMAT" $TIERS_PAR $SKIMS_PAR $REMOVE_PAR $ADDITIONAL_PARAMS)
    else
        IMAGE=$(run_spark --attributes $ATTRIBUTES_FILE --output_folder "$BASE_OUTPUT_FOLDER/$PREFIX" --output_format "$OUTPUT_FORMAT" $TIERS_PAR $SKIMS_PAR $REMOVE_PAR $ADDITIONAL_PARAMS)
    fi
    util4logi "Image: $IMAGE"
    ln -sf "$IMAGE" "$OUTPUT_DIR/$PREFIX/event_count_latest.$OUTPUT_FORMAT"
}
# ------------------------------------------------------------------------------------------------------- PY PARAMETERS
# DO NOT MODIFY ANY COMMENTED OUT OR UNUSED PARAMETERS, they are accessed by gen_plot() function
# PARAMETERS FIGURE 9
F9_TIERS=("GEN" "GEN-SIM" "GEN-RAW" "GEN-SIM-RECO" "AODSIM" "MINIAODSIM" "RAWAODSIM" "NANOAODSIM" "GEN-SIM-DIGI-RAW" "GEN-SIM-RAW" "GEN-SIM-DIGI-RECO")
#F9_SKIMS=()
#F9_REMOVE=()
# PARAMETERS FIGURE 10
F10_TIERS=("RAW" "RECO" "AOD" "RAW-RECO" "USER" "MINIAOD" "NANOAOD")
F10_SKIMS=("PromptReco" "Parking")
F10_REMOVE=("test" "backfill" "StoreResults" "monitor" "Error/" "Scouting" "MiniDaq" "/Alca" "L1Accept" "L1EG" "L1Jet" "L1Mu" "PhysicsDST" "VdM" "/Hcal" "express" "Interfill" "Bunnies" "REPLAY" "LogError")

OUTPUT_FORMAT="png"
# ADDITIONAL_PARAMS="${@:3}"
ADDITIONAL_PARAMS=""
ATTRIBUTES_FILE="$script_dir/../etc/event_count_attributes.json"

# If test, process only 3 months
if [[ "$IS_TEST" == 1 ]]; then
    start_month=$(date -d "$(date) -3 month" +%Y/%m)
    gen_plot "F10" "$start_month"
    gen_plot "F9" "$start_month"
else
    gen_plot "F10"
    gen_plot "F9"
fi

duration=$(($(date +%s) - START_TIME))
util4logi "all finished, time spent: $(util_secs_to_human $duration)"
