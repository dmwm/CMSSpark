#!/bin/bash
# Utils for datasetmon scripts
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>

function util4datasetmon_input_args_parser() {
    unset -v KEYTAB_SECRET HDFS_PATH ARG_MONGOHOST ARG_MONGOPORT ARG_MONGOUSER ARG_MONGOPASS ARG_MONGOWRITEDB ARG_MONGOAUTHDB PORT1 PORT2 K8SHOST WDIR help
    # Dictionary to keep variables
    declare -A arr

    PARSED_ARGS=$(getopt --unquoted --options v,h --name "$(basename -- "$0")" --longoptions keytab:,hdfs:,mongohost:,mongoport:,mongouser:,mongopass:,mongowritedb:,mongoauthdb:,p1:,p2:,host:,wdir:,help -- "$@")
    VALID_ARGS=$?
    if [ "$VALID_ARGS" != "0" ]; then
        util4loge "Given args not valid: $*"
        exit 1
    fi
    eval set -- "$PARSED_ARGS"
    while [[ $# -gt 0 ]]; do
        case "$1" in
        --keytab)       arr["KEYTAB_SECRET"]=$2     ; shift 2 ;;
        --hdfs)         arr["HDFS_PATH"]=$2         ; shift 2 ;;
        --mongohost)    arr["ARG_MONGOHOST"]=$2     ; shift 2 ;;
        --mongoport)    arr["ARG_MONGOPORT"]=$2     ; shift 2 ;;
        --mongouser)    arr["ARG_MONGOUSER"]=$2     ; shift 2 ;;
        --mongopass)    arr["ARG_MONGOPASS"]=$2     ; shift 2 ;;
        --mongowritedb) arr["ARG_MONGOWRITEDB"]=$2  ; shift 2 ;;
        --mongoauthdb)  arr["ARG_MONGOAUTHDB"]=$2   ; shift 2 ;;
        --p1)           arr["PORT1"]=$2             ; shift 2 ;;
        --p2)           arr["PORT2"]=$2             ; shift 2 ;;
        --host)         arr["K8SHOST"]=$2           ; shift 2 ;;
        --wdir)         arr["WDIR"]=$2              ; shift 2 ;;
        -h | --help)    arr["help"]=1               ; shift   ;;
        *)              break                                 ;;
        esac
    done

    for key in "${!arr[@]}"; do
        eval $key=${arr[$key]}
    done
}
