#!/bin/bash

# info log function
util_logi() {
    echo "$(date --rfc-3339=seconds) [INFO] $1"
}

# warn log function
util_logw() {
    echo "$(date --rfc-3339=seconds) [WARN] $1"
}

# error log function
util_loge() {
    echo "$(date --rfc-3339=seconds) [ERROR] $1"
}

# seconds to h, m, s format used in logging
util_secs_to_human() {
    echo "$((${1} / 3600))h $(((${1} / 60) % 60))m $((${1} % 60))s"
}

# util to print help message
util_usage_help() {
    grep "^##H" <"$0" | sed -e "s,##H,,g" && exit 1
}

# util to exit function on fail
util_on_fail_exit() {
    util_loge "finished with error!" && exit 1
}

# Util to check variables are defined
#   Accepts variable names(without $ sign), not values
#   Usage: util_check_vars ARG1 ARG2 ARG3
util_check_vars() {
    local var_check_flag, var_names
    unset var_check_flag
    var_names=("$@")
    for var_name in "${var_names[@]}"; do
        [ -z "${!var_name}" ] && util_loge "$var_name is unset." && var_check_flag=true
    done
    [ -n "$var_check_flag" ] && exit 1
    return 0
}

# Util to check file exists
#   Accepts file names
#   Usage: util_check_files $ARG_KEYTAB /etc/secrets/foo /data/config.json $FOO
util_check_files() {
    local var_check_flag, file_names
    unset var_check_flag
    file_names=("$@")
    for file_name in "${file_names[@]}"; do
        [ -e "${!file_name}" ] && util_loge "$file_name file does not exist, please check given args." && var_check_flag=true
    done
    [ -n "$var_check_flag" ] && exit 1
    return 0
}

# Util to check shell command or executable exists
util_check_cmd() {
    if which $1 >/dev/null; then
        util_logi "$1 exists"
    else
        util_loge "Please make sure you correctly set $1 executable path in PATH." && exit 1
    fi
}

# check and set JAVA_HOME
util_set_java_home() {
    if [ -n "$JAVA_HOME" ]; then
        if [ -e "/usr/lib/jvm/java-1.8.0" ]; then
            export JAVA_HOME="/usr/lib/jvm/java-1.8.0"
        elif ! (java -XX:+PrintFlagsFinal -version 2>/dev/null | grep -E -q 'UseAES\s*=\s*true'); then
            util_loge "this script requires a java version with AES enabled"
            exit 1
        fi
    fi
}

# Util to authenticate with keytab and to return Kerberos principle name
#  Accepts file,
#  Usage Important: check file exit code "$?=0" because it returns principle name
#  Usage:
#      principle=$(util_kerberos_auth_with_keytab /foo/keytab)
#      if [ $? -ne 0 ]; then
#          util_loge "unable to perform kinit"
#          exit 1
#      fi
util_kerberos_auth_with_keytab() {
    local principle
    principle=$(klist -k "$1" | tail -1 | awk '{print $2}')
    kinit "$principle" -k -t "$1" >/dev/null
    if [ $? -ne 0 ]; then
        exit 1
    fi
    # remove "@" part from the principle name
    echo "$principle" | grep -o '^[^@]*'
}
