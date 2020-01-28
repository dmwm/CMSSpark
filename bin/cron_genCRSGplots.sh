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
OUTPUT_FORMAT="${2:-png}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ADDITIONAL_PARAMS="${@:3}"


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
    IMAGE=$(/bin/bash "$SCRIPT_DIR/generate_event_count_plot.sh" --output_folder "$BASE_OUTPUT_FOLDER/$PREFIX" --output_format "$OUTPUT_FORMAT" $TIERS_PAR $SKIMS_PAR $REMOVE_PAR $ADDITIONAL_PARAMS)
    echo "$IMAGE"
    ln -sf "$IMAGE" "$BASE_OUTPUT_FOLDER/$PREFIX/event_count_latest.$OUTPUT_FORMAT" 
}

gen_plot "F10"
gen_plot "F9"
