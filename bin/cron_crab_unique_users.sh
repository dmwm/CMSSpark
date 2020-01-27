#!/bin/bash
OUTPUT_DIR="${1:-./output}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

END_DATE="${3:-$(date +%Y-%m-01)}"
START_DATE="${2:-$(date -d "$END_DATE -1 year" +%Y-%m-01)}"

(>&2 echo "Totals for dataset/datablock from $START_DATE to $END_DATE")

/bin/bash "$SCRIPT_DIR/run_crab_unique_users.sh" --generate_plots --by "month" --output_folder "$OUTPUT_DIR" "$START_DATE" "$END_DATE"
/bin/bash "$SCRIPT_DIR/run_crab_unique_users.sh" --generate_plots --by "weekofyear" --output_folder "$OUTPUT_DIR" "$START_DATE" "$END_DATE"

ln -s -f "$OUTPUT_DIR/UniqueUsersBy_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/UniqueUsersBy_month_latest.csv"
ln -s -f "$OUTPUT_DIR/UniqueUsersBy_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/UniqueUsersBy_month_latest.png"


ln -s -f "$OUTPUT_DIR/UniqueUsersBy_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/UniqueUsersBy_weekofyear_latest.csv"
ln -s -f "$OUTPUT_DIR/UniqueUsersBy_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/UniqueUsersBy_weekofyear_latest.png"
