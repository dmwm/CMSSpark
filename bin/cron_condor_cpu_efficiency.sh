#!/bin/bash
# Create a site for the default period for each of the cms types
# This script is intended to be used as cron job. 
# For this reason it will use sensible defaults 
# for the required parameters.
OUTPUT_DIR="${1:-$HOME/www/cpu_eff}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CMS_TYPES=("analysis" "production" "folding@home" "test")
for type in "${CMS_TYPES[@]}"
do
SUBFOLDER=$(echo "$type"|sed -e 's/[^[:alnum:]]/-/g' | tr -s '-' | tr '[:upper:]' '[:lower:]')
/bin/bash "$SCRIPT_DIR/run_condor_cpu_efficiency.sh" --cms_type "$type" \
 --output_folder "$OUTPUT_DIR/$SUBFOLDER"
done
