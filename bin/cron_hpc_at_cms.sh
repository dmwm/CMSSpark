#!/bin/bash
# This script is intended to be used as cron job.
# For this reason it will use sensible defaults
# for the required parameters.
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    cat <<EOF
 Usage: cron_hpc_at_cms.sh <OUTPUT_DIR> <START_DATE> <END_DATE>
   - if OUTPUT_DIR is not specified, $HOME/output_hpc_at_cms will be used.
   - Valid START_DATE and END_DATE formats are: "%Y/%m/%d", "%Y-%m-%d" and "%Y%m%d"
   - if not END_DATE is specified, it will use the first day of the current month.
   - if there is not START_DATE and END_DATE it will cover a 1 year period,
     ending in the first day of the current month (without including it)
EOF
    exit 0
fi

OUTPUT_DIR="${1:-$HOME/output_hpc_at_cms}"
script_dir="$(cd "$(dirname "$0")" && pwd)"

# Validation of dates are performed by python script
# Valid date formats: "%Y/%m/%d", "%Y-%m-%d" and "%Y%m%d"
END_DATE="${3:-$(date +%Y-%m-01)}"
START_DATE="${2:-$(date -d "$END_DATE -1 year" +%Y-%m-01)}"

(echo >&2 "Plots from $START_DATE to $END_DATE")

/bin/bash "$script_dir/run_hpc_at_cms.sh" --output_folder "$OUTPUT_DIR" --start_date "$START_DATE" --end_date "$END_DATE"

ln -s -f "$OUTPUT_DIR/HPC@CMS_Running_Cores_Hourly_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/HPC@CMS_Running_Cores_Hourly_latest.png"
