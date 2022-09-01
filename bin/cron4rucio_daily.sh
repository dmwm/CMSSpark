#!/bin/bash
# Daily Rucio dumps
HDFS_OUTPUT_DIR="${1}"
CURRENT_DATE="$(date +%Y-%m-%d)"
script_dir="$(cd "$(dirname "$0")" && pwd)"

/bin/bash "$script_dir/run_rucio_daily.sh" --verbose --output_folder "$HDFS_OUTPUT_DIR" --fdate "$CURRENT_DATE"
