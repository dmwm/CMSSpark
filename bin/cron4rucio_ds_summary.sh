#!/bin/bash
set -e
##H Can only run in K8s
##H
##H cron4rucio_ds_summary.sh
##H    Cron job of rucio_ds_summary.py which sends Spark agg results to MONIT via StompAMQ
##H
##H Usage:
##H    cron4rucio_ds_summary.sh \
##H        <keytab> value <amq> value <cmsmonitoring> value <stomp> value
##H
##H Example :
##H    cron4rucio_datasets_daily_stats.sh \
##H        --keytab ./keytab --amq ./amq-creds.json --cmsmonitoring ./CMSMonitoring.zip --stomp ./stomp-v700.zip \
##H        --nodename $MY_NODE_NAME \
##H        --sparkport0 $K8S_SERVICE_PORT_PORT_0 \
##H        --sparkport1 $K8S_SERVICE_PORT_PORT_1 \
##H        --sparkbindadr 0.0.0.0 \
##H        --sparklocalip 127.0.0.1 \
##H        --wdir $WDIR
##H
##H Arguments with values:
##H   - keytab           : Kerberos auth file: secrets/kerberos
##H   - amq              : AMQ credentials and configurations json file for /topic/cms.rucio.dailystats:
##H                          secrets/cms-rucio-dailystats
##H   - cmsmonitoring    : dmwm/CMSMonitoring/src/python/CMSMonitoring folder as zip to be sent to Spark nodes
##H   - stomp            : stomp.py==7.0.0 module as zip to be sent to Spark nodes which has lower versions.
##H   - nodename         : spark-submit "spark.driver.host". In K8s, it should be "MY_NODE_NAME"(community uses this variable)
##H   - sparkport0       : spark-submit "spark.driver.port"
##H   - sparkport1       : spark-submit "spark.driver.blockManager.port"
##H   - sparkbindadr     : spark-submit "spark.driver.bindAddress". In K8s, it should be "0.0.0.0"
##H   - sparklocalip     : Spark "$SPARK_LOCAL_IP" environment variable. In K8s, it should be "127.0.0.1"
##H   - wdir             : Working directory to arrange logging directory
##H
##H References:
##H   - some features&logics are copied from sqoop_utils.sh and cron4rucio_datasets_daily_stats.sh
##H   - getopt ref: https://www.shellscript.sh/tips/getopt/index.html
##H

trap 'onFailExit' ERR
onFailExit() {
    echo "$(date --rfc-3339=seconds)" "[ERROR] Finished with error!"
    exit 1
}
usage() {
    grep "^##H" <"$0" | sed -e "s,##H,,g"
    exit 1
}
# seconds to h, m, s format used in logging
secs_to_human() {
    # Ref https://stackoverflow.com/a/59096583/6123088
    echo "$((${1} / 3600))h $(((${1} / 60) % 60))m $((${1} % 60))s"
}
SCRIPT_DIR="$(
    cd -- "$(dirname "$0")" >/dev/null 2>&1
    pwd -P
)"
TZ=UTC
START_TIME=$(date +%s)

# ------------------------------------------------------------------------------------------------------- GET USER ARGS
unset -v KEYTAB_SECRET AMQ_JSON_CREDS CMSMONITORING_ZIP STOMP_ZIP ARG_NODENAME ARG_SPARKPORT0 ARG_SPARKPORT1 ARG_SPARKBINDADDRESS ARG_SPARKLOCALIP ARG_WDIR help
[ "$#" -ne 0 ] || usage

# --options (short options) is mandatory, and v is a dummy param.
PARSED_ARGUMENTS=$(
    getopt \
        --unquoted \
        --options v \
        --name "$(basename -- "$0")" \
        --longoptions keytab:,amq:,cmsmonitoring:,stomp:,nodename:,sparkport0:,sparkport1:,sparklocalip:,sparkbindadr:,wdir:,,help \
        -- "$@"
)
VALID_ARGUMENTS=$?
if [ "$VALID_ARGUMENTS" != "0" ]; then
    usage
fi

echo "$(date --rfc-3339=seconds)" "[INFO] Given arguments: $PARSED_ARGUMENTS"
eval set -- "$PARSED_ARGUMENTS"

while [[ $# -gt 0 ]]; do
    case "$1" in
    --keytab)
        KEYTAB_SECRET=$2
        shift 2
        ;;
    --amq)
        AMQ_JSON_CREDS=$2
        shift 2
        ;;
    --cmsmonitoring)
        CMSMONITORING_ZIP=$2
        shift 2
        ;;
    --stomp)
        STOMP_ZIP=$2
        shift 2
        ;;
    --nodename)
        ARG_NODENAME=$2
        shift 2
        ;;
    --sparkport0)
        ARG_SPARKPORT0=$2
        shift 2
        ;;
    --sparkport1)
        ARG_SPARKPORT1=$2
        shift 2
        ;;
    --sparkbindadr)
        ARG_SPARKBINDADDRESS=$2
        shift 2
        ;;
    --sparklocalip)
        ARG_SPARKLOCALIP=$2
        shift 2
        ;;
    --wdir)
        ARG_WDIR=$2
        shift 2
        ;;
    -h | --help)
        help=1
        shift
        ;;
    *)
        break
        ;;
    esac
done

#
if [[ "$help" == 1 ]]; then
    usage
fi

# Define logs path for Spark imports which produce lots of info logs
LOG_DIR="$ARG_WDIR"/logs/$(date +%Y%m%d)
mkdir -p "$LOG_DIR"

# ------------------------------------------------------------------------------------------- CHECK IF FILES/DIRS EXIST
# Check variables are defined
function check_vars() {
    var_names=("$@")
    for var_name in "${var_names[@]}"; do
        [ -z "${!var_name}" ] && echo "$var_name is unset." && var_unset=true
    done
    [ -n "$var_unset" ] && exit 1
    return 0
}

# Check variables
check_vars KEYTAB_SECRET AMQ_JSON_CREDS CMSMONITORING_ZIP STOMP_ZIP ARG_NODENAME ARG_SPARKPORT0 ARG_SPARKPORT1 ARG_SPARKBINDADDRESS ARG_SPARKLOCALIP ARG_WDIR

# Check files
for fname in $KEYTAB_SECRET $AMQ_JSON_CREDS $CMSMONITORING_ZIP $STOMP_ZIP; do
    if [ ! -e "${BASE_SECRET_FILES_DIR}/${fname}" ]; then
        echo "$(date --rfc-3339=seconds)" "[ERROR] File does not exist: ${BASE_SECRET_FILES_DIR}/${fname}"
        exit 1
    fi
done

# Check JAVA_HOME is set
if [ -n "$JAVA_HOME" ]; then
    if [ -e "/usr/lib/jvm/java-1.8.0" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
    elif ! (java -XX:+PrintFlagsFinal -version 2>/dev/null | grep -E -q 'UseAES\s*=\s*true'); then
        echo "$(date --rfc-3339=seconds)" "[ERROR] This script requires a java version with AES enabled"
        exit 1
    fi
fi

# --------------------------------------------------------------------------------------------- Kerberos authentication
PRINCIPAL=$(klist -k "$KEYTAB_SECRET" | tail -1 | awk '{print $2}')
echo "Kerberos principle=$PRINCIPAL"
kinit "$PRINCIPAL" -k -t "$KEYTAB_SECRET"
if [ $? == 1 ]; then
    echo "$(date --rfc-3339=seconds)" "[ERROR] Unable to perform kinit"
    exit 1
fi
klist -k "$KEYTAB_SECRET"

# Requires kerberos ticket to reach the EOS directory
if [ ! -d "$EOS_DIR" ]; then
    echo "$(date --rfc-3339=seconds)" "[ERROR] EOS directory does not exist: ${EOS_DIR}}"
    # exit 1, do not exit till we solve the problem in K8s magnum-eos pods which always cause trouble in each 40-50 days
fi
# ------------------------------------------------------------------------------------------ INITIALIZE ANALYTIX SPARK3
hadoop-set-default-conf.sh analytix
source hadoop-setconf.sh analytix 3.2 spark3

# ------------------------------------------------------------------------------------------------------- RUN SPARK JOB
function run_spark() {
    # Required for Spark job in K8s
    echo "$(date --rfc-3339=seconds)" "[INFO] Spark job starts"

    export SPARK_LOCAL_IP="$ARG_SPARKLOCALIP"
    export PYTHONPATH=$SCRIPT_DIR/../src/python:$PYTHONPATH
    spark_submit_args=(
        --master yarn
        --conf spark.executor.memory=8g
        --conf spark.executor.instances=30
        --conf spark.executor.cores=4
        --conf spark.driver.memory=8g
        --conf spark.ui.showConsoleProgress=false
        --packages org.apache.spark:spark-avro_2.12:3.2.1
        --py-files "${CMSMONITORING_ZIP},${STOMP_ZIP}"
    )
    # Define Spark network settings in K8s cluster
    spark_submit_args+=(
        --conf "spark.driver.bindAddress=${ARG_SPARKBINDADDRESS}"
        --conf "spark.driver.host=${ARG_NODENAME}"
        --conf "spark.driver.port=${ARG_SPARKPORT0}"
        --conf "spark.driver.blockManager.port=${ARG_SPARKPORT1}"
    )
    py_input_args=(
        --creds "$AMQ_JSON_CREDS"
        --amq_batch_size 1000
    )
    # Run
    spark-submit \
        "${spark_submit_args[@]}" \
        "${SCRIPT_DIR}/../src/python/CMSSpark/rucio_ds_summary.py" \
        "${py_input_args[@]}" \
        >>"${LOG_DIR}/spark-rucio_ds_summary.log" 2>&1
}

run_spark 2>&1

echo "$(date --rfc-3339=seconds)" "[INFO] Last 10 lines of Spark job log"
tail -10 "${LOG_DIR}/spark-rucio_ds_summary.log"
# Print process wall clock time
echo "$(date --rfc-3339=seconds)" "[INFO] All finished. Time spent: $(secs_to_human "$(($(date +%s) - START_TIME))")"
