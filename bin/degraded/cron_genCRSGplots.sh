#!/bin/bash
# Generate the plots for the CRSG report
# In the following parameters comment the parameter if you want to use the default value.
# using an empty array will set the parameter without value.
#
# PARAMETERS FIGURE 9
F9_TIERS=("GEN" "GEN-SIM" "GEN-RAW" "GEN-SIM-RECO" "AODSIM" "MINIAODSIM" "RAWAODSIM" "NANOAODSIM" "GEN-SIM-DIGI-RAW" "GEN-SIM-RAW" "GEN-SIM-DIGI-RECO")
#F9_SKIMS=()
#F9_REMOVE=()
# PARAMETERS FIGURE 10
F10_TIERS=("RAW" "RECO" "AOD" "RAW-RECO" "USER" "MINIAOD" "NANOAOD")
F10_SKIMS=("PromptReco" "Parking")
F10_REMOVE=("test" "backfill" "StoreResults" "monitor" "Error/" "Scouting" "MiniDaq" "/Alca" "L1Accept" "L1EG" "L1Jet" "L1Mu" "PhysicsDST" "VdM" "/Hcal" "express" "Interfill" "Bunnies" "REPLAY" "LogError")


# DO NOT MODIFY AFTER THIS COMMENT
BASE_OUTPUT_FOLDER="${1:-./output}"
echo "OUTPUT TO $BASE_OUTPUT_FOLDER"
OUTPUT_FORMAT="${2:-png}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
ADDITIONAL_PARAMS="${@:3}"
ATTRIBUTES_FILE="$script_dir/../etc/event_count_attributes.json"

function gen_plot() {
    PREFIX=${1:-"F9"}
    SK_VAR="${PREFIX}_SKIMS"
    SK_EXPAND="${SK_VAR}[@]"
    SKIMS_PAR=$([[ -v  "$SK_VAR" ]]&&echo "--skims ${!SK_EXPAND}"||echo "")
    TIERS_VAR="${PREFIX}_TIERS"
    TIERS_EXPAND="${TIERS_VAR}[@]"
    TIERS_PAR=$([[ -v  "$TIERS_VAR" ]]&&echo "--tiers ${!TIERS_EXPAND}"||echo "")
    REMOVE_VAR="${PREFIX}_REMOVE"
    REMOVE_EXPAND="${REMOVE_VAR}[@]"
    REMOVE_PAR=$([[ -v "$REMOVE_VAR" ]]&&echo "--remove ${!REMOVE_EXPAND}"||echo "")
    IMAGE=$(/bin/bash "$script_dir/generate_event_count_plot.sh" --attributes $ATTRIBUTES_FILE --output_folder "$BASE_OUTPUT_FOLDER/$PREFIX" --output_format "$OUTPUT_FORMAT" $TIERS_PAR $SKIMS_PAR $REMOVE_PAR $ADDITIONAL_PARAMS)
    echo "$IMAGE"
    ln -sf "$IMAGE" "$BASE_OUTPUT_FOLDER/$PREFIX/event_count_latest.$OUTPUT_FORMAT"
}

gen_plot "F10"
gen_plot "F9"

# ----- CRON SUCCESS CHECK -----
# This cron job updates yearly plots every month, so treshold should be 32 days
TIME_TRESHOLD=2764800
# 00 14 05 * * /bin/bash $HOME/CMSSpark/bin/cron_genCRSGplots.sh /eos/user/c/cmsmonit/www/EventCountPlots
SIZE_TRESHOLD=10000 # (10KB)
FILE_TO_CHECK="event_count_$(date -d "1 year ago" +"%Y%m")-$(date -d "1 month ago" +"%Y%m").png" # generate name of the files we are testing

trap 'EC'=210 ERR

/bin/bash "$SCRIPT_DIR"/utils/check_utils.sh check_file_status "$BASE_OUTPUT_FOLDER/F9/$FILE_TO_CHECK" $TIME_TRESHOLD $SIZE_TRESHOLD || EC=$?
/bin/bash "$SCRIPT_DIR"/utils/check_utils.sh check_file_status "$BASE_OUTPUT_FOLDER/F10/$FILE_TO_CHECK" $TIME_TRESHOLD $SIZE_TRESHOLD || EC=$?
exit $EC
# RUNNING COMMANDS AFTER THIS POINT WILL CHANGE EXIT CODE
