#!/bin/bash
OUTPUT_DIR="${1:-./output}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

#Generate the previous month intermediate files
MONTH="${2:-$(date -d "$(date +%Y-%m-15) -1 month" +%Y/%m)}"
/bin/bash "$SCRIPT_DIR/run_hdfs_eos.sh" run_update --mode overwrite "$MONTH/*"
#Generate the totals csv for one year

END=$(date -d "$MONTH/01 +1 month -1 day" +%Y%m%d)
START=$(date -d "$END -1 year +1 day" +%Y%m%d)
/bin/bash "$SCRIPT_DIR/run_hdfs_eos.sh" run_report_totals --outputDir "$OUTPUT_DIR" "$START" "$END"

#Generate and concat the last month data. 
START=$(date -d "$END -1 month +1 day" +%Y%m%d)
/bin/bash "$SCRIPT_DIR/run_hdfs_eos.sh" get_filenames_per_day --outputDir "$OUTPUT_DIR" "$START" "$END"