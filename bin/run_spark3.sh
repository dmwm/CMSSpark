#!/bin/bash
set -e
##H  Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
##H  A wrapper script to submit spark job with CMSSpark python script.
##H
##H  Usage in Lxplus7: run_spark3 <cmsspark_script> <options>
##H  Usage in K8s:
##H    run_spark3 defines the run environment via environment variables: IS_K8S K8SHOST K8S_PORT1 K8S_PORT2
##H    before running it, these env vars should be set like 'export IS_K8S=true'
##H  Example usage in K8s:
##H    export IS_K8S=true
##H    export K8SHOST=$MY_NODE_NAME
##H    export K8S_PORT1=32000
##H    export K8S_PORT2=32001
##H    run_spark <cmsspark_script> <options>
##H  Info:
##H    - Supports only Spark3, Analytix cluster, LxPlus environment, yarn mode
##H    - Supports '--py-files', for multiple files provide them as comma separated, no whitespace string
##H    - Supports  '--yes-log4j' allows to get whole spark debug logs
##H    - 1st argument: should be full/relative path of python script or help
##H    - $PYTHONPATH should be set before running script
##H
script_dir="$(cd "$(dirname "$0")" && pwd)"
# get common util functions
. "$script_dir"/utils/common_utils.sh

trap 'onFailExit' ERR
onFailExit() {
    util4loge "finished with error!" || exit 1
}
# ------------------------------------------------------------------------------------------------------------- PREPARE
unset args confs cmsspark conf_pyfiles

export PATH="${PATH}:${script_dir}"

# parse first argument
if [ $# -eq 0 ] || [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ]; then
    # exit with 0 code
    util_usage_help
elif [ -f "$1" ]; then
    cmsspark=$1
    # Since first arg is read, shift arguments to next one
    shift
else
    cmsspark=${script_dir}/../src/python/CMSSpark/$1
fi

# Setup Analytix cluster settings for Lxplus7 or Kubernetes
if [[ -n "$IS_K8S" ]] ; then
    util4logi "Attempted to run in K8s, checking required environment variables"
    util_check_vars K8SHOST K8S_PORT1 K8S_PORT1
    util_setup_spark_k8s
    # Define K8s required configurations for Spark
    conf=(
        --conf "spark.driver.bindAddress=0.0.0.0"
        --conf "spark.driver.host=${K8SHOST}"
        --conf "spark.driver.port=${K8S_PORT1}"
        --conf "spark.driver.blockManager.port=${K8S_PORT1}"
    )
    util4logi "running in K8s"
else
    util4logi "Running on Lxplus7"
    util_setup_spark_lxplus7
    conf=()
fi

# Python script help
# Since we shifted arguments, $1 represents 2nd user argument.
if [ "$1" == "-h" ] || [ "$1" == "-help" ] || [ "$1" == "--help" ]; then
    # Since we already setup hadoop spark environment, python will look to appropriate executor python
    python "$cmsspark" --help
    exit 0
fi

# Get all arguments as string
args="$@"

# Check if debug logs are required, default is false
if [[ "$args" == *"--yes-log4j"* ]]; then
    conf+=(--conf spark.ui.showConsoleProgress=true)
else
    conf+=(--conf spark.ui.showConsoleProgress=false)
fi

# extract --py-files with values (space after args variable is important to catch if values given as last args)
conf_pyfiles=$(echo "$args " | sed -e 's/^.*\(--py-files[=| ][^ ]*\) .*$/\1/')
util4logi "py-files extracted: $conf_pyfiles"

# If there is --py-files argument, add it to confs and delete it from python script arguments
if [[ -n $conf_pyfiles ]]; then
    conf+=("$conf_pyfiles")
    # Remove conf_pyfiles from the args
    args="${args//$conf_pyfiles/}"
fi

conf+=(
    --master yarn
    --conf spark.executor.memory=5g --conf spark.driver.memory=4g
    --packages org.apache.spark:spark-avro_2.12:3.4.0
)

util4logi "PYTHONPATH: $PYTHONPATH"
util4logi "cmsspark=$cmsspark, confs=" "${conf[@]}" "arguments=" "${args[@]}"

spark-submit "${conf[@]}" "$cmsspark" "${args[@]}"
