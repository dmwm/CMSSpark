#!/bin/bash
output_dir="${1:-./output}"
script_dir="$(cd "$(dirname "$0")" && pwd)"

#Generate the previous month intermediate files
month_="${2:-$(date -d "$(date +%Y-%m-15) -1 month" +%Y/%m)}"
#month_=2020/11
echo "Processing month " "$month_"
mode_="${3:-append}"
/bin/bash "$script_dir/run_hdfs_eos.sh" run_update --mode "$mode_" "$month_/*"

#Generate the totals csv for one year

end_=$(date -d "$month_/01 +1 month -1 day" +%Y%m%d)
start_=$(date -d "$end_ -1 year +1 day" +%Y%m%d)

(echo >&2 "Totals for dataset/file from $start_ to $end_")
/bin/bash "$script_dir/run_hdfs_eos.sh" run_report_totals --outputDir "$output_dir" "$start_" "$end_"

if [[ -f "$output_dir/dataset_totals.csv" ]]; then
    mv "$output_dir/dataset_totals.csv" "$output_dir/dataset_totals_yearly_${start_}_${end_}.csv"
    cp -f "$output_dir/dataset_totals_yearly_${start_}_${end_}.csv" "$output_dir/dataset_totals_rolling_year.csv"
    ln -s -f "$output_dir/top_total_rb_${start_}-${end_}.png" "$output_dir/topDS_last_rolling_year.png"
    gzip "$output_dir/dataset_totals_yearly_${start_}_${end_}.csv"
fi

#Generate and concat the last month data.
start_=$(date -d "$month_/01" +%Y%m%d)
(echo >&2 "Totals for the last month, $start_ to $end_")
/bin/bash "$script_dir/run_hdfs_eos.sh" run_report_totals --outputDir "$output_dir" "$start_" "$end_"
if [[ -f "$output_dir/dataset_totals.csv" ]]; then
    mv "$output_dir/dataset_totals.csv" "$output_dir/dataset_totals_${start_}_${end_}.csv"
    ln -s -f "$output_dir/dataset_totals_${start_}_${end_}.csv" "$output_dir/dataset_totals_last_month.csv"
    ln -s -f "$output_dir/top_total_rb_${start_}_${end_}.png" "$output_dir/topDS_last_rolling_month.png"
fi
