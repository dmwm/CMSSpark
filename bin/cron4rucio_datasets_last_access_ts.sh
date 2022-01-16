#!/bin/bash
# Cron job of run_rucio_datasets_last_access_ts.py
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    cat <<EOF
 Usage: .sh --html_directory <HTML_DIRECTORY> --output_dir <OUTPUT_DIR> --rses_pickle <RSES_PICKLE>
    - This cron job produce rucio_dataset_last_access.html using Rucio table dumps in hdfs
 Args:
    - HTML_DIRECTORY: directory of partial html files used in html creation
    - OUTPUT_DIR: directory of output html files
    - RSES_PICKLE: Rucio rse name and id map pickle file, see ~/CMSSpark/static/rucio/rse_name_id_map_pickle.py

EOF
    exit 0
fi

# Setup envs for hadoop and spark. Tested with LCG_98python3
source /cvmfs/sft.cern.ch/lcg/views/LCG_98python3/x86_64-centos7-gcc8-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix

# Check JAVA_HOME is set
if [ -n "$JAVA_HOME" ]; then
    if [ -e "/usr/lib/jvm/java-1.8.0" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
    elif ! (java -XX:+PrintFlagsFinal -version 2>/dev/null | grep -E -q 'UseAES\s*=\s*true'); then
        (echo >&2 "This script requires a java version with AES enabled")
        exit 1
    fi
fi

# Check Kerberos ticket
if ! klist -s; then
    echo "There is not valid ticket yet"
    kinit
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HTML_DIRECTORY="${1:-$SCRIPT_DIR/../src/html/rucio_datasets_last_access_ts}"
OUTPUT_DIR="${1:-/eos/user/c/cmsmonit/www/rucio_datasets_not_read}"
RSES_PICKLE="${1:-$SCRIPT_DIR/../static/rucio/rses.pickle}"

PY_INPUT_ARGS=(
    --html_directory "$HTML_DIRECTORY"
    --output_dir "$OUTPUT_DIR"
    --rses_pickle "$RSES_PICKLE"
)

(echo >&2 "Output html file: ${OUTPUT_DIR}")

# Run
spark-submit \
    --master yarn \
    --conf spark.driver.extraClassPath='/eos/project/s/swan/public/hadoop-mapreduce-client-core-2.6.0-cdh5.7.6.jar' \
    --conf spark.executor.memory=8g \
    --conf spark.executor.instances=30 \
    --conf spark.executor.cores=4 \
    --conf spark.driver.memory=8g \
    --conf spark.ui.showConsoleProgress=false \
    --packages org.apache.spark:spark-avro_2.11:2.4.3 \
    "$SCRIPT_DIR/../src/python/CMSSpark/rucio_datasets_last_access_ts.py" "${PY_INPUT_ARGS[@]}"
