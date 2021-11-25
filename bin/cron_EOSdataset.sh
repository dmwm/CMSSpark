#!/bin/bash
OUTPUT_DIR="${1:-./output}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

#Generate the previous month intermediate files
MONTH="${2:-$(date -d "$(date +%Y-%m-15) -1 month" +%Y/%m)}"
#MONTH=2020/11
echo "Processing month " "$MONTH"
MODE="${3:-append}"
/bin/bash "$SCRIPT_DIR/run_hdfs_eos.sh" run_update --mode "$MODE" "$MONTH/*"

#Generate the totals csv for one year

END=$(date -d "$MONTH/01 +1 month -1 day" +%Y%m%d)
START=$(date -d "$END -1 year +1 day" +%Y%m%d)

(echo >&2 "Totals for dataset/file from $START to $END")
/bin/bash "$SCRIPT_DIR/run_hdfs_eos.sh" run_report_totals --outputDir "$OUTPUT_DIR" "$START" "$END"

if [[ -f "$OUTPUT_DIR/dataset_totals.csv" ]]; then
    mv "$OUTPUT_DIR/dataset_totals.csv" "$OUTPUT_DIR/dataset_totals_yearly_${START}_${END}.csv"
    cp -f "$OUTPUT_DIR/dataset_totals_yearly_${START}_${END}.csv" "$OUTPUT_DIR/dataset_totals_rolling_year.csv"
    ln -s -f "$OUTPUT_DIR/top_total_rb_${START}-${END}.png" "$OUTPUT_DIR/topDS_last_rolling_year.png"
    gzip "$OUTPUT_DIR/dataset_totals_yearly_${START}_${END}.csv"
fi

#Generate and concat the last month data.
START=$(date -d "$MONTH/01" +%Y%m%d)
(echo >&2 "Totals for the last month, $START to $END")
/bin/bash "$SCRIPT_DIR/run_hdfs_eos.sh" run_report_totals --outputDir "$OUTPUT_DIR" "$START" "$END"
if [[ -f "$OUTPUT_DIR/dataset_totals.csv" ]]; then
    mv "$OUTPUT_DIR/dataset_totals.csv" "$OUTPUT_DIR/dataset_totals_${START}_${END}.csv"
    ln -s -f "$OUTPUT_DIR/dataset_totals_${START}_${END}.csv" "$OUTPUT_DIR/dataset_totals_last_month.csv"
fi
