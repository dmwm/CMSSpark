#!/bin/sh

# Script is used for downloading and concatenating files from hadoop
# Usage: download_and_concat /cms/users/jrumsevi/agg 20170901 20170915

hadoop_directory=$1
date_from=$2
date_to=$3
first_file_date=""
last_file_date=""

if [ "$date_from" == "" ] || [ "$date_to" == "" ]; then
    date_from="00000000"
    date_to="99999999"
fi

# hadoop fs -ls -R $1 - get list of all files and directories in $1 (recursively)
# grep -E ".*[0-9]{4}/[0-9]{2}/[0-9]{2}$" - get only lines that end with dddd/dd/dd (d - digit)
result=$(hadoop fs -ls -R $1 | grep -E ".*[0-9]{4}/[0-9]{2}/[0-9]{2}$")

IFS=$'\n'
for directory in $result
do
    date=$(echo $directory | tail -c11)
    current_dir=$hadoop_directory"/"$date
    # echo $current_dir
    clean_date=$(echo $date | sed -e "s/\///g")
    # echo "Clean date " $clean_date

    if [[ ! "$date_from" > "$clean_date" ]] && [[ ! "$clean_date" > "$date_to" ]]; then
        echo "Will do for "$date
        mkdir $clean_date
        hadoop fs -get "$current_dir/*" "./$clean_date"
        cd $clean_date
        head -1 part-00000 > "../$clean_date.csv"; tail -n +2 -q part-* >> "../$clean_date.csv"
        cd ..
        rm -r $clean_date

        if [ "$first_file_date" == "" ]; then
            first_file_date=$clean_date
        fi
        last_file_date=$clean_date
    fi
done
unset IFS

head -1 "$first_file_date.csv" > all.TEMP; tail -n +2 -q *.csv >> all.TEMP
# rm *.csv
mv all.TEMP "$first_file_date-$last_file_date.csv"
