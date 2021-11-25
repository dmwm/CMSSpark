#!/bin/bash
# This script is intended to be used as cron job.
# For this reason it will use sensible defaults
# for the required parameters.
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    cat <<EOF
 Usage: cron_hs06cputme_plot.sh <OUTPUT_DIR> <START_DATE> <END_DATE>
   - if OUTPUT_DIR is not specified, $HOME/output_hs06_cpu will be used.
   - if not END_DATE is specified, it will use the first day of the current month.
   - if there is not START_DATE and END_DATE it will cover a 1 year period,
     ending in the first day of the current month (without including it)
 This script will generate two plots and two datasets, one with monthly values
 and other with weekly values.
EOF
    exit 0
fi
OUTPUT_DIR="${1:-$HOME/output_hs06_cpu}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

END_DATE="${3:-$(date +%Y-%m-01)}"
START_DATE="${2:-$(date -d "$END_DATE -1 year" +%Y-%m-01)}"

(echo >&2 "Plots from $START_DATE to $END_DATE")

# T2 sites excluding cern.
/bin/bash "$SCRIPT_DIR/run_hs06cputime_plot.sh" --generate_plots --by "month" --output_folder "$OUTPUT_DIR/T2" "$START_DATE" "$END_DATE"
/bin/bash "$SCRIPT_DIR/run_hs06cputime_plot.sh" --generate_plots --by "weekofyear" --output_folder "$OUTPUT_DIR/T2" "$START_DATE" "$END_DATE"

ln -s -f "$OUTPUT_DIR/T2/HS06CpuTimeHr_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/T2/HS06CpuTimeHr_month_latest.csv"
ln -s -f "$OUTPUT_DIR/T2/HS06CpuTimeHr_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/T2/HS06CpuTimeHr_month_latest.png"
ln -s -f "$OUTPUT_DIR/T2/HS06CpuTimeHr_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/T2/HS06CpuTimeHr_weekofyear_latest.csv"
ln -s -f "$OUTPUT_DIR/T2/HS06CpuTimeHr_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/T2/HS06CpuTimeHr_weekofyear_latest.png"

# T1 sites
/bin/bash "$SCRIPT_DIR/run_hs06cputime_plot.sh" --generate_plots --by "month" --include_re "T1_.*" --exclude_re "^$" --output_folder "$OUTPUT_DIR/T1" "$START_DATE" "$END_DATE"
/bin/bash "$SCRIPT_DIR/run_hs06cputime_plot.sh" --generate_plots --by "weekofyear" --include_re "T1_.*" --exclude_re "^$" --output_folder "$OUTPUT_DIR/T1" "$START_DATE" "$END_DATE"

ln -s -f "$OUTPUT_DIR/T1/HS06CpuTimeHr_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/T1/HS06CpuTimeHr_month_latest.csv"
ln -s -f "$OUTPUT_DIR/T1/HS06CpuTimeHr_month_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/T1/HS06CpuTimeHr_month_latest.png"
ln -s -f "$OUTPUT_DIR/T1/HS06CpuTimeHr_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).csv" "$OUTPUT_DIR/T1/HS06CpuTimeHr_weekofyear_latest.csv"
ln -s -f "$OUTPUT_DIR/T1/HS06CpuTimeHr_weekofyear_$(date -d "$START_DATE" +%Y%m%d)-$(date -d "$END_DATE" +%Y%m%d).png" "$OUTPUT_DIR/T1/HS06CpuTimeHr_weekofyear_latest.png"
