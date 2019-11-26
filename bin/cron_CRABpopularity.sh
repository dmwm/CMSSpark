#!/bin/bash
OUTPUT_DIR="${1:-./output}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

END_DATE="${3:-$(date +%Y-%m-01)}"
START_DATE="${2:-$(date -d "$END_DATE -1 year" +%Y-%m-01)}"

(>&2 echo "Totals for dataset/datablock from $START_DATE to $END_DATE")

/bin/bash "$SCRIPT_DIR/run_hdfs_crab.sh" --generate_plots --output_folder "$OUTPUT_DIR" "$START_DATE" "$END_DATE"

ln -s -f "$OUTPUT_DIR/CRAB_popularity_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/CRAB_popularity_latest.csv"