#!/bin/bash
# Create a site for the default period for each of the cms types
# This script is intended to be used as cron job. 
# For this reason it will use sensible defaults 
# for the required parameters.
OUTPUT_DIR="${1:-$HOME/www/cpu_eff}"
OUTPUT_DIR_OUTLIER="${1:-$HOME/www/cpu_eff}_outlier"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CMS_TYPES=("analysis" "production" "folding@home" "test")

echo "Starting spark jobs for cpu_eff_outlier=0, folder: " $OUTPUT_DIR
for type in "${CMS_TYPES[@]}"
do
SUBFOLDER=$(echo "$type"|sed -e 's/[^[:alnum:]]/-/g' | tr -s '-' | tr '[:upper:]' '[:lower:]')
/bin/bash "$SCRIPT_DIR/run_condor_cpu_efficiency.sh" --cms_type "$type" \
 --output_folder "$OUTPUT_DIR/$SUBFOLDER" \
 --cpu_eff_outlier=0
done

echo "Starting spark jobs for cpu_eff_outlier=1, folder: " $OUTPUT_DIR_OUTLIER
for type in "${CMS_TYPES[@]}"
do
SUBFOLDER=$(echo "$type"|sed -e 's/[^[:alnum:]]/-/g' | tr -s '-' | tr '[:upper:]' '[:lower:]')
/bin/bash "$SCRIPT_DIR/run_condor_cpu_efficiency.sh" --cms_type "$type" \
 --output_folder "$OUTPUT_DIR_OUTLIER/$SUBFOLDER" \
 --cpu_eff_outlier=1
done
