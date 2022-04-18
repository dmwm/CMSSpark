#!/bin/bash
# Creates StepChain cpu efficiency static web site
#
# This script is intended to be used as cron job in kubernetes.
export SPARK_LOCAL_IP=127.0.0.1
export PYTHONPATH=$SCRIPT_DIR/../src/python:$PYTHONPATH

# seconds to h, m, s format used in logging
secs_to_human() {
    # Ref https://stackoverflow.com/a/59096583/6123088
    echo "$((${1} / 3600))h $(((${1} / 60) % 60))m $((${1} % 60))s"
}
TZ=UTC
START_TIME=$(date +%s)

# Check output path is given
[[ -z "$1" || -z $2 ]] && {
    echo "ERROR: Output path or last_n_days is not defined"
    exit 1
}

# Check env vars are set
[[ -z "$MY_NODE_NAME" ]] && {
    echo "ERROR: MY_NODE_NAME is not defined"
    exit 1
}
[[ -z "$CONDOR_CPU_EFF_SERVICE_PORT_PORT_0" ]] && {
    echo "ERROR: CONDOR_CPU_EFF_SERVICE_PORT_PORT_0 is not defined"
    exit 1
}
[[ -z "$CONDOR_CPU_EFF_SERVICE_PORT_PORT_1" ]] && {
    echo "ERROR: CONDOR_CPU_EFF_SERVICE_PORT_PORT_1 is not defined"
    exit 1
}

# Kerberos
keytab=/etc/condor-cpu-eff/keytab
principal=$(klist -k "$keytab" | tail -1 | awk '{print $2}')
echo "principal=$principal"
kinit "$principal" -k -t "$keytab"
if [ $? == 1 ]; then
    echo "Unable to perform kinit"
    exit 1
fi
klist -k "$keytab"

currentDir=$(
    cd "$(dirname "$0")" && pwd
)

spark_confs=(
    --conf "spark.driver.bindAddress=0.0.0.0"
    --conf "spark.driver.host=${MY_NODE_NAME}"
    --conf "spark.driver.port=${CONDOR_CPU_EFF_SERVICE_PORT_PORT_0}"
    --conf "spark.driver.blockManager.port=${CONDOR_CPU_EFF_SERVICE_PORT_PORT_1}"
    --conf "spark.executor.memory=8g"
    --conf "spark.executor.instances=30"
    --conf "spark.executor.cores=4"
    --conf "spark.driver.memory=4g"
)

OUTPUT_DIR="${1}/stepchain"
LAST_N_DAYS="${2}"

echo "$(date --rfc-3339=seconds)" "[INFO] Starting spark job for step chain cpu efficiencies, folder: ${OUTPUT_DIR}"
spark-submit \
    --master yarn \
    "${spark_confs[@]}" \
    "$currentDir/../src/python/CMSSpark/stepchain_cpu_eff.py" \
    --output_folder "$OUTPUT_DIR" \
    --last_n_days "$LAST_N_DAYS"

# We should clean old files which are not used in the web site anymore.
echo "Deleting html and png files older than 60 days in dir: ${OUTPUT_DIR}"
find "$OUTPUT_DIR" -type f \( -name '*.html' -o -name '*.png' \) -mtime +60 -delete
echo -ne "\nDeletion is finished\n"

echo "$(date --rfc-3339=seconds)" "[INFO] StepChain finished. Time spent: $(secs_to_human "$(($(date +%s) - START_TIME))")"
