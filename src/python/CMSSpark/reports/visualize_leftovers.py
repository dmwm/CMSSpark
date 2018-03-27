import pandas as pd
from report_builder import ReportBuilder
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.abspath(__file__)))))
from utils import bytes_to_pb_string, bytes_to_pib_string, bytes_to_readable
import argparse

TIME_DATA_FILE = 'spark_exec_time_leftovers.txt'

report_builder = ReportBuilder()

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_report_dir():
    return '%s/../../../bash/CERNTasks.wiki' % get_script_dir()

def get_destination_dir():
    return '%s/../../../bash/report_leftovers' % get_script_dir()

def append_report(lines):
    report_builder.append(lines)
    report_builder.append('\n')

def write_dataset_to_report(df, head=0):
    append_report('| Dataset | Sites | PhEDEx Size (PB - PiB) | Campaign |')
    append_report('| ------- | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['dataset'] + 
                      ' | ' + row['sites'][13:-1] + 
                      ' | ' + bytes_to_pb_string(row['phedex_size']) + ' - ' + bytes_to_pib_string(row['phedex_size']) + 
                      ' | ' + row['campaign'] + 
                      ' |')

def append_report_header():
    append_report('# PhEDEx and DBS dataset leftovers for 2017-02-28')
    append_report('Following two tables are snapshots of leftover datasets. All leftovers are datasets that are present in PhEDEx but are either not in BDS or has DBS status that is not VALID. Orphans are those datasets that are present in PhEDEx but are not present in DBS at all.')
    append_report('If you want to get full datasets for specific date, please follow [these instructions](https://github.com/vkuznet/CMSSpark/README_Leftovers.md)')

def write_report(report):
    with open('%s/CMS_Leftovers_Report.md' % get_report_dir(), 'w') as f:
        f.write(report)

def commit_report():
    os.system('(cd %s/; git add -A; git commit -m "Auto-commiting report"; git push origin master)' % get_report_dir())

def append_execution_time():
    with open('%s/%s' % (get_destination_dir(), TIME_DATA_FILE), 'r') as f:
        append_report('#### Spark job execution time: %s' % f.read())

def visualize_all_leftovers():
    df = pd.read_csv('%s/leftovers_all_df.csv' % get_destination_dir())

    append_report('## All leftovers')
    append_report('These are datasets that are present in PhEDEx but are either not in BDS or has DBS status that is not VALID.')
    append_report('')
    
    write_dataset_to_report(df, 20)

    append_report('#### Total datasets: %d' % len(df.index))
    append_report('#### Total PhEDEx size: %s' % bytes_to_readable(df.phedex_size.sum()))

def visualize_orphan_leftovers():
    df = pd.read_csv('%s/leftovers_orphans_df.csv' % get_destination_dir())

    append_report('## Orphan leftovers')
    append_report('Orphans are those datasets that are present in PhEDEx but are not present in DBS at all.')
    append_report('')
    
    write_dataset_to_report(df, 20)

    append_report('#### Total datasets: %d' % len(df.index))
    append_report('#### Total PhEDEx  size: %s' % bytes_to_readable(df.phedex_size.sum()))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true",
                        dest="commit", 
                        default=False, 
                        help="Determines whether report should be committed to Github wiki")
    opts = parser.parse_args()

    append_report_header()
  
    visualize_all_leftovers()
    visualize_orphan_leftovers()
    append_execution_time()

    write_report(report_builder.get())

    if opts.commit == True:
        commit_report()

if __name__ == '__main__':
    main()
