#!/bin/bash
# Utils for cron scripts
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>

# -------------------------------------- LOGGING UTILS --------------------------------------------
#######################################
# info log function
#######################################
function util4logi() {
    echo "$(date --rfc-3339=seconds) [INFO]" "$@"
}

#######################################
# warn log function
#######################################
function util4logw() {
    echo "$(date --rfc-3339=seconds) [WARN]" "$@"
}

#######################################
# error log function
#######################################
function util4loge() {
    echo "$(date --rfc-3339=seconds) [ERROR]" "$@"
}

#######################################
# util to print help message
#######################################
function util_usage_help() {
    grep "^##H" <"$0" | sed -e "s,##H,,g"
}

#######################################
# util to exit function on fail
#######################################
function util_on_fail_exit() {
    util4loge "finished with error!"
    exit 1
}
# -------------------------------------------------------------------------------------------------

# -------------------------------------- CHECK UTILS ----------------------------------------------
#######################################
# Util to check variables are defined
#  Arguments:
#    $1: variable name(s), meaning without $ sign. Supports multiple values
#  Usage:
#   util_check_vars ARG1 ARG2 ARG3
#  Returns:
#    success: 0
#    fail   : exits with exit-code 1
#######################################
function util_check_vars() {
    local var_check_flag var_names
    unset var_check_flag
    var_names=("$@")
    for var_name in "${var_names[@]}"; do
        [ -z "${!var_name}" ] && util4loge "$var_name is unset." && var_check_flag=true
    done
    [ -n "$var_check_flag" ] && exit 1
    return 0
}

#######################################
# Util to check file exists
#  Arguments:
#    $1: file(s), full paths or names in current directory. Supports multiple files
#  Usage:
#    util_check_files $ARG_KEYTAB /etc/secrets/foo /data/config.json $FOO
#  Returns:
#    success: 0
#    fail   : exits with exit-code 1
#######################################
function util_check_files() {
    local var_check_flag file_names
    unset var_check_flag
    file_names=("$@")
    for file_name in "${file_names[@]}"; do
        [ -e "${!file_name}" ] && util4loge "$file_name file does not exist, please check given args." && var_check_flag=true
    done
    [ -n "$var_check_flag" ] && exit 1
    return 0
}

#######################################
# Util to check shell command or executable exists
#  Arguments:
#    $1: command/executable name or full path
#  Usage:
#    util_check_cmd foo
#  Returns:
#    success: 0 and info log
#    fail   : exits with exit-code 1
#######################################
function util_check_cmd() {
    if which "$1" >/dev/null; then
        util4logi "$1 exists"
    else
        util4loge "Please make sure you correctly set $1 executable path in PATH." && exit 1
    fi
}

#######################################
# Util to check if directory exist and create if not
#  Arguments:
#    $1: directory path
#  Usage:
#    util_check_cmd /dir/foo
#  Returns:
#    success: 0 and info log
#    fail   : exits with exit-code 1
#######################################
function util_check_and_create_dir() {
    local dir=$1
    if [ -z "$dir" ]; then
        util4loge "Please provide output directory. Exiting.."
        util_usage_help
        exit 1
    fi
    if [ ! -d "$dir" ]; then
        util4logw "output directory does not exist, creating..: ${dir}}"
        if [[ "$(mkdir -p "$dir" >/dev/null)" -ne 0 ]]; then
            util4loge "cannot create output directory: ${dir}"
            exit 1
        fi
    fi
}

#######################################
# Util to check if PYTHONPATH is successfully defined as including CMSSpark
#  Assumption:
#    Either python or python3 alias should be exist in the system
#  Returns:
#    success: 0 and info log
#    fail   : exits with exit-code 1
#######################################
function util_check_pythonpath_for_cmsspark() {
    # OR operation, if both python and python3 fails, it will fail
    if python3 -c "import CMSSpark" >/dev/null 2>&1 || python -c "import CMSSpark" >/dev/null 2>&1; then
        util4logi "CMSSpark is in PYTHONPATH"
    else
        util4log3 "CMSSpark is not in PYTHONPATH, exiting"
        exit 1
    fi
}
# -------------------------------------------------------------------------------------------------

# ------------------------------------- PRE SETUP UTILS -------------------------------------------
#######################################
# check and set JAVA_HOME
#######################################
function util_set_java_home() {
    if [ -n "$JAVA_HOME" ]; then
        if [ -e "/usr/lib/jvm/java-1.8.0" ]; then
            export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
        elif ! (java -XX:+PrintFlagsFinal -version 2>/dev/null | grep -E -q 'UseAES\s*=\s*true'); then
            util4loge "this script requires a java version with AES enabled"
            exit 1
        fi
    fi
}

#######################################
# Util to authenticate with keytab and to return Kerberos principle name
#  Arguments:
#    $1: keytab file
#  Usage:
#    principle=$(util_kerberos_auth_with_keytab /foo/keytab)
#  Returns:
#    success: principle name before '@' part. If principle is 'johndoe@cern.ch, will return 'johndoe'
#    fail   : exits with exit-code 1
#######################################
function util_kerberos_auth_with_keytab() {
    local principle
    principle=$(klist -k "$1" | tail -1 | awk '{print $2}')
    # run kinit and check if it fails or not
    if ! kinit "$principle" -k -t "$1" >/dev/null; then
        util4loge "Exiting. Kerberos authentication failed with keytab:$1"
        exit 1
    fi
    # remove "@" part from the principle name
    echo "$principle" | grep -o '^[^@]*'
}

#######################################
# setup hadoop and spark in k8s
#######################################
function util_setup_spark_k8s() {
    # check hava home
    util_set_java_home

    hadoop-set-default-conf.sh analytix
    source hadoop-setconf.sh analytix 3.2 spark3
    export SPARK_LOCAL_IP=127.0.0.1
    export PYSPARK_PYTHON=/cvmfs/sft.cern.ch/lcg/releases/Python/3.9.6-b0f98/x86_64-centos7-gcc8-opt/bin/python3
}

#######################################
# setup hadoop and spark in lxplus
#######################################
function util_setup_spark_lxplus7() {
    # check hava home
    util_set_java_home

    source /cvmfs/sft.cern.ch/lcg/views/LCG_101/x86_64-centos7-gcc8-opt/setup.sh
    source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix 3.2 spark3
    export PATH="${PATH}:/usr/hdp/hadoop/bin/hadoop:/usr/hdp/spark3/bin:/usr/hdp/sqoop/bin"
}
# -------------------------------------------------------------------------------------------------

# ----------------------------------- PUSHGATEWAY UTILS -------------------------------------------
#######################################
# Returns left part of the dot containing string
# Arguments:
#   arg1: string
#######################################
function util_dotless_name() {
    echo "$1" | cut -f1 -d"."
}

#######################################
# Util to send cronjob start time to pushgateway
#  Arguments:
#    $1: cronjob name
#    $2: environment, prod/dev/test
#  Usage:
#    util_cron_send_start foo test
#######################################
function util_cron_send_start() {
    local script_name env
    if [ -z "$PUSHGATEWAY_URL" ]; then
        util4loge "PUSHGATEWAY_URL variable is not defined. Exiting.."
        exit 1
    fi
    script_name=$1
    env=$K8S_ENV
    script_name_wo_extension=$(util_dotless_name "$script_name")
    cat <<EOF | curl --data-binary @- "$PUSHGATEWAY_URL"/metrics/job/cmsmon-cron/instance/"$(hostname)"
# TYPE cmsmon_cron_start_${env}_${script_name_wo_extension} gauge
# HELP cmsmon_cron_start_${env}_${script_name_wo_extension} cronjob START Unix time
cmsmon_cron_start_${env}_${script_name_wo_extension}{} $(date +%s)
EOF
}

#######################################
# Util to send cronjob end time to pushgateway
#  Arguments:
#    $1: cronjob name
#    $2: environment, prod/dev/test
#    $3: cronjob exit status
#  Usage:
#    util_cron_send_end foo test 210
#######################################
function util_cron_send_end() {
    local script_name env exit_code
    if [ -z "$PUSHGATEWAY_URL" ]; then
        util4loge "PUSHGATEWAY_URL variable is not defined. Exiting.."
        exit 1
    fi
    script_name=$1
    env=$K8S_ENV
    exit_code=$3
    script_name_wo_extension=$(util_dotless_name "$script_name")
    cat <<EOF | curl --data-binary @- "$PUSHGATEWAY_URL"/metrics/job/cmsmon-cron/instance/"$(hostname)"
# TYPE cmsmon_cron_end_${env}_${script_name_wo_extension} gauge
# HELP cmsmon_cron_end_${env}_${script_name_wo_extension} cronjob END Unix time
cmsmon_cron_end_${env}_${script_name_wo_extension}{status="${exit_code}"} $(date +%s)
EOF
}
# -------------------------------------------------------------------------------------------------

# --------------------------------- INPUT ARGS PARSER UTILS ---------------------------------------
#######################################
# util to parse all input parameters of scripts and EVAL them to local variables
#  Explanation:
#    Please put all variables into getopt parameters depending on they accept value (ends with :) or flag (ends with nothing)
#    The name that used in "arr" dict will EVAL to variable with that name.
#  Arguments:
#    $@: all
#######################################
function util_input_args_parser() {
    unset -v KEYTAB_SECRET CMSR_SECRET RUCIO_SECRET AMQ_JSON_CREDS CMSMONITORING_ZIP STOMP_ZIP EOS_DIR PORT1 PORT2 K8SHOST WDIR OUTPUT_DIR CONF_FILE URL_PREFIX LAST_N_DAYS IS_ITERATIVE IS_K8S IS_TEST help
    # Dictionary to keep variables
    declare -A arr

    PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,cmsr:,rucio:,amq:,cmsmonitoring:,stomp:,eos:,p1:,p2:,host:,wdir:,output:,conf:,url:,lastndays:,iterative,k8s,test,help -- "$@")
    VALID_ARGS=$?
    if [ "$VALID_ARGS" != "0" ]; then
        util4loge "Given args not valid: $*"
        exit 1
    fi
    eval set -- "$PARSED_ARGS"
    while [[ $# -gt 0 ]]; do
        case "$1" in
        --keytab)        arr["KEYTAB_SECRET"]=$2     ; shift 2 ;;
        --cmsr)          arr["CMSR_SECRET"]=$2       ; shift 2 ;;
        --rucio)         arr["RUCIO_SECRET"]=$2      ; shift 2 ;;
        --amq)           arr["AMQ_JSON_CREDS"]=$2    ; shift 2 ;;
        --cmsmonitoring) arr["CMSMONITORING_ZIP"]=$2 ; shift 2 ;;
        --stomp)         arr["STOMP_ZIP"]=$2         ; shift 2 ;;
        --eos)           arr["EOS_DIR"]=$2           ; shift 2 ;;
        --p1)            arr["PORT1"]=$2             ; shift 2 ;;
        --p2)            arr["PORT2"]=$2             ; shift 2 ;;
        --host)          arr["K8SHOST"]=$2           ; shift 2 ;;
        --wdir)          arr["WDIR"]=$2              ; shift 2 ;;
        --output)        arr["OUTPUT_DIR"]=$2        ; shift 2 ;;
        --conf)          arr["CONF_FILE"]=$2         ; shift 2 ;;
        --url)           arr["URL_PREFIX"]=$2        ; shift 2 ;;
        --lastndays)     arr["LAST_N_DAYS"]=$2       ; shift 2 ;;
        --iterative)     arr["IS_ITERATIVE"]=1       ; shift   ;;
        --k8s)           arr["IS_K8S"]=1             ; shift   ;;
        --test)          arr["IS_TEST"]=1            ; shift   ;;
        -h | --help)     arr["help"]=1               ; shift   ;;
        *)               break                                 ;;
        esac
    done

    for key in "${!arr[@]}"; do
        eval $key=${arr[$key]}
    done
}
# -------------------------------------------------------------------------------------------------

# ------------------------------------------- OTHER UTILS -----------------------------------------
#######################################
# util to convert seconds to h, m, s format used in logging
#  Arguments:
#    $1: seconds in integer
#  Usage:
#    util_secs_to_human SECONDS
#    util_secs_to_human 1000 # returns: 0h 16m 40s
#  Returns:
#    '[\d+]h [\d+]m [\d+]s' , assuming [\d+] integer values
#######################################
function util_secs_to_human() {
    echo "$((${1} / 3600))h $(((${1} / 60) % 60))m $((${1} % 60))s"
}
# -------------------------------------------------------------------------------------------------
