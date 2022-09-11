#!/bin/bash
# Utils for cron scripts
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>

#######################################
# info log function
#######################################
util4logi() {
    echo "$(date --rfc-3339=seconds) [INFO]" "$@"
}

#######################################
# warn log function
#######################################
util4logw() {
    echo "$(date --rfc-3339=seconds) [WARN]" "$@"
}

#######################################
# error log function
#######################################
util4loge() {
    echo "$(date --rfc-3339=seconds) [ERROR]" "$@"
}

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
util_secs_to_human() {
    echo "$((${1} / 3600))h $(((${1} / 60) % 60))m $((${1} % 60))s"
}

#######################################
# util to print help message
#######################################
util_usage_help() {
    grep "^##H" <"$0" | sed -e "s,##H,,g"
    exit 0
}

#######################################
# util to exit function on fail
#######################################
util_on_fail_exit() {
    util4loge "finished with error!"
    exit 1
}

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
util_check_vars() {
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
util_check_files() {
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
util_check_cmd() {
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
util_check_and_create_dir() {
    local dir=$1
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
util_check_pythonpath_for_cmsspark() {
    # OR operation, if both python and python3 fails, it will fail
    if python3 -c "import CMSSpark" >/dev/null 2>&1 || python -c "import CMSSpark" >/dev/null 2>&1; then
        util4logi "CMSSpark is in PYTHONPATH"
    else
        util4log3 "CMSSpark is not in PYTHONPATH, exiting"
        exit 1
    fi
}

#######################################
# check and set JAVA_HOME
#######################################
util_set_java_home() {
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
util_kerberos_auth_with_keytab() {
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
util_setup_spark_k8s() {
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
util_setup_spark_lxplus7() {
    # check hava home
    util_set_java_home

    source /cvmfs/sft.cern.ch/lcg/views/LCG_101/x86_64-centos7-gcc8-opt/setup.sh
    source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix 3.2 spark3
    export PATH="${PATH}:/usr/hdp/hadoop/bin/hadoop:/usr/hdp/spark3/bin:/usr/hdp/sqoop/bin"
}
