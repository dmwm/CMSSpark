#!/bin/bash
# Creates StepChain cpu efficiency static web site
#
# This script is intended to be used as cron job in kubernetes.

echo '==================================================================================================='
echo '_.~"~._.~"~._.~"~._.~"~._  StepChain CPU Efficiency cron job is starting  _.~"~._.~"~._.~"~._.~"~._'
echo '==================================================================================================='

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

# Start crond if it is not runing
if [ -z "$(pgrep crond)" ]; then
  crond -n &
fi

currentDir=$(
  cd "$(dirname "$0")" && pwd
)

spark_submit=/usr/hdp/spark-2.4/bin/spark-submit
spark_confs=(
  --conf "spark.driver.bindAddress=0.0.0.0"
  --conf "spark.driver.host=${MY_NODE_NAME}"
  --conf "spark.driver.port=${CONDOR_CPU_EFF_SERVICE_PORT_PORT_0}"
  --conf "spark.driver.blockManager.port=${CONDOR_CPU_EFF_SERVICE_PORT_PORT_1}"
  --conf "spark.driver.extraClassPath=${WDIR}/hadoop-mapreduce-client-core-2.6.0-cdh5.7.6.jar"
  --conf "spark.executor.memory=8g"
  --conf "spark.executor.instances=30"
  --conf "spark.executor.cores=4"
  --conf "spark.driver.memory=4g"
)

OUTPUT_DIR="${1}/stepchain"
LAST_N_DAYS="${2}"

echo "Starting spark job for stepchain cpu efficiencies, folder: ${OUTPUT_DIR}"
$spark_submit --master yarn "${spark_confs[@]}" \
  "$currentDir/../src/python/CMSSpark/stepchain_cpu_eff.py" --output_folder "$OUTPUT_DIR" --last_n_days "$LAST_N_DAYS"

# We should clean old files which are not used in the web site anymore.
echo "Deleting html and png files older than 60 days in dir: ${OUTPUT_DIR}"
find "$OUTPUT_DIR" -type f \( -name '*.html' -o -name '*.png' \) -mtime +60 -delete
echo -ne "\nDeletion is finished\n"

echo '==================================================================================================='
echo '_.~"~._.~"~._.~"~._.~"~._  StepChain CPU Efficiency cron job is finished  _.~"~._.~"~._.~"~._.~"~._'
echo '==================================================================================================='
echo -en "\n\n\n"
