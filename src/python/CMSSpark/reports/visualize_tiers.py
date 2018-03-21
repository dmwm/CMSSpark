import pandas as pd

import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt
from subprocess import check_output
from report_builder import ReportBuilder
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.abspath(__file__)))))
from utils import bytes_to_readable
import shutil
import operator
import argparse

PHEDEX_TIME_DATA_FILE = 'spark_exec_time_tier_phedex.txt'
DBS_TIME_DATA_FILE = 'spark_exec_time_tier_dbs.txt'

report_builder = ReportBuilder()

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_report_dir():
    return '%s/../../../bash/CERNTasks.wiki' % get_script_dir()

def get_destination_dir():
    return '%s/../../../bash/report_tiers' % get_script_dir()

def append_report(lines):
    report_builder.append(lines)
    report_builder.append('\n')

def write_df_to_report(df, head=0):
    append_report('| Tier | Count | Size (TB) |')
    append_report('| ------- | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + index + ' | ' + str(int(row['tier_count'])) + ' | ' + str(round(row['sum_size'], 1)) + ' |')

def create_plot_dirs(phedex_plots_dir, dbs_plots_dir):
    phedex_absolute_path = '%s/images/%s' % (get_report_dir(), phedex_plots_dir)
    dbs_absolute_path = '%s/images/%s' % (get_report_dir(), dbs_plots_dir)

    if not os.path.exists(phedex_absolute_path):
        os.makedirs(phedex_absolute_path)
    if not os.path.exists(dbs_absolute_path):
        os.makedirs(dbs_absolute_path)

def append_report_header():
    append_report('# PhEDEx and DBS data aggregation results')
    append_report('Results of gathering information of all data tiers in every data site. The results are the number of entries in each data tier (tier count) and the sum of sizes of entries in that tier (size).')

def read_phedex_time_data():
    with open('%s/%s' % (get_destination_dir(), PHEDEX_TIME_DATA_FILE)) as f:
        return f.read()

def read_dbs_time_data():
    with open('%s/%s' % (get_destination_dir(), DBS_TIME_DATA_FILE)) as f:
        return f.read()

def write_report():
    with open('%s/CMS_Tier_Reports.md' % get_report_dir(), 'w') as f:
        f.write(report_builder.get())

def commit_report():
    os.system('(cd %s/; git add -A; git commit -m "Auto-commiting report"; git push origin master)' % get_report_dir())

def make_plot(result, plot_file):
    axes = result.plot(kind='bar', subplots=True, layout=(2,1), figsize=(8, 6), fontsize=6)

    axes[0][0].set_title('')
    axes[1][0].set_title('')
    axes[0][0].set_ylabel('Number')
    axes[1][0].set_ylabel('Terabytes')

    plt.xticks(rotation=45, horizontalalignment='right')
    plt.tight_layout()

    plot_filepath = '%s/images/%s' % (get_report_dir(), plot_file)
    plt.savefig(plot_filepath, dpi=120)

def analyse_phedex_data(plots_dir):
    df = pd.read_csv('%s/phedex_df.csv' % get_destination_dir())
    # sites = df.groupby(df.site.str[:2])['site'].agg(lambda x: set(x)).index.tolist()
    sites = ['T1', 'T2', 'T3']

    append_report('## PhEDEx data')

    for site in sites:
        result = df[df.site.str[:2] == site] \
            .groupby(df.dataset.str.split('/').str[3]) \
            .agg({'size': 'sum', 'site': 'count'})

        result = result.rename(columns={'size': 'sum_size', 'site': 'tier_count'})

        result.sort_values('tier_count', ascending=False, inplace=True)

        # Bytes to terabytes
        result['sum_size'] = result['sum_size'] / 1000000000000

        append_report('### Site {0}. Showing TOP 5 most significant data-tiers'.format(site))
        write_df_to_report(result, 5)

        plot_file = '%s/phedex_data_tiers_%s.jpg' % (plots_dir, site)
        make_plot(result, plot_file)

        append_report('### Plot')
        append_report('![5 most significant data-tiers](images/%s)' % plot_file)

    time = read_phedex_time_data()
    append_report('#### Spark job run time: {0}'.format(time))

def analyse_dbs_data(plots_dir):
    df = pd.read_csv('%s/dbs_df.csv' % get_destination_dir())
    result = df.groupby(df.dataset.str.split('/').str[3]).agg({'size': 'sum', 'dataset': 'count'})

    result = result.rename(columns={'size': 'sum_size', 'dataset': 'tier_count'})

    result.sort_values('tier_count', ascending=False, inplace=True)

    # Bytes to terabytes
    result['sum_size'] = result['sum_size'] / 1000000000000

    append_report('## DBS data. Showing TOP 5 most significant data-tiers')
    write_df_to_report(result, 5)

    plot_file = '%s/dbs_data_tiers.jpg' % plots_dir
    make_plot(result, plot_file)

    append_report('### Plot')
    append_report('![5 most significant data-tiers](images/%s)' % plot_file)

    time = read_dbs_time_data()
    append_report('#### Spark job run time: {0}'.format(time))

def aggregate_all_datastreams_info():
    append_report('## Sizes of all datastreams')
    append_report('| Stream | Size |')
    append_report('| ------- | ------ |')

    locations = { 
        'AAA (JSON) user logs accessing XrootD servers': 'hdfs:///project/monitoring/archive/xrootd/raw/gled',
        'EOS (JSON) user logs accesses CERN EOS': 'hdfs:///project/monitoring/archive/eos/logs/reports/cms',
        'HTCondor (JSON) CMS Jobs logs': 'hdfs:///project/monitoring/archive/condor/raw/metric',
        'FTS (JSON) CMS FTS logs': 'hdfs:///project/monitoring/archive/fts/raw/complete',
        'CMSSW (Avro) CMSSW jobs': 'hdfs:///project/awg/cms/cmssw-popularity/avro-snappy',
        'JobMonitoring (Avro) CMS Dashboard DB snapshot': 'hdfs:///project/awg/cms/jm-data-popularity/avro-snappy',
        'WMArchive (Avro) CMS Workflows archive': 'hdfs:///cms/wmarchive/avro',
        'ASO (CSV) CMS ASO accesses': 'hdfs:///project/awg/cms/CMS_ASO/filetransfersdb/merged',
        'DBS (CSV) CMS Data Bookkeeping snapshot': 'hdfs:///project/awg/cms/CMS_DBS3_PROD_GLOBAL/current',
        'PhEDEx (CSV) CMS data location DB snapshot': 'hdfs:///project/awg/cms/phedex/block-replicas-snapshots'
    }

    results = {}

    for name, location in locations.iteritems():
        out = check_output(['hadoop', 'fs', '-du', '-s', location])
        size = float(out.split(' ')[0])
        results[name] = size
   
    total_sum = sum(results.values())

    # Sort by value
    results = sorted(results.items(), key=operator.itemgetter(1), reverse=True)

    for result in results:
        append_report('| ' + result[0] + ' | ' + bytes_to_readable(result[1]) + ' |')

    append_report('| **Total** | **' + bytes_to_readable(total_sum) + '** |')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true",
                        dest="commit", 
                        default=False, 
                        help="Determines whether report should be committed to Github wiki")
    parser.add_argument("--phedex_plots_dir", action="store",
                        dest="phedex_plots_dir", default='phedex_plots', 
                        help="Determines directory where PhEDEx plots will be saved")
    parser.add_argument("--dbs_plots_dir", action="store",
                        dest="dbs_plots_dir", default='dbs_plots', 
                        help="Determines directory where DBS plots will be saved")
    opts = parser.parse_args()

    create_plot_dirs(opts.phedex_plots_dir, opts.dbs_plots_dir)
    append_report_header()
  
    analyse_phedex_data(opts.phedex_plots_dir)
    analyse_dbs_data(opts.dbs_plots_dir)
    aggregate_all_datastreams_info()

    write_report()

    if opts.commit == True:
        commit_report()

if __name__ == '__main__':
    main()
