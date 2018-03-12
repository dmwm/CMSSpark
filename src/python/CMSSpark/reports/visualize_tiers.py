import pandas as pd

import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt
from subprocess import check_output
import os
import shutil
import operator
import argparse

PHEDEX_PLOTS_PATH = 'phedex_plots/'
DBS_PLOTS_PATH = 'dbs_plots/'
PHEDEX_TIME_DATA_FILE = 'spark_exec_time_tier_phedex.txt'
DBS_TIME_DATA_FILE = 'spark_exec_time_tier_dbs.txt'

report = ''

def append_report(lines):
    global report
    report = report + lines
    report = report + '\n'

def write_df_to_report(df, head=0):
    append_report('| Tier | Count | Size (TB) |')
    append_report('| ------- | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + index + ' | ' + str(int(row['tier_count'])) + ' | ' + str(round(row['sum_size'], 1)) + ' |')

def copy_directory(src, dest):
    dest_dir = os.path.dirname(dest)
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)
    
    # Delete destination first
    shutil.rmtree(dest)

    try:
        shutil.copytree(src, dest)
    # Directories are the same
    except shutil.Error as e:
        print('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print('Directory not copied. Error: %s' % e)


def bytes_to_readable(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)

def create_plot_dirs():
    if not os.path.exists(PHEDEX_PLOTS_PATH):
        os.makedirs(PHEDEX_PLOTS_PATH)
    if not os.path.exists(DBS_PLOTS_PATH):
        os.makedirs(DBS_PLOTS_PATH)

def append_report_header():
    append_report('# PhEDEx and DBS data aggregation results')
    append_report('Results of gathering information of all data tiers in every data site. The results are the number of entries in each data tier (tier count) and the sum of sizes of entries in that tier (size).')

def read_phedex_time_data():
    with open(PHEDEX_TIME_DATA_FILE) as f:
        return f.read()

def read_dbs_time_data():
    with open(DBS_TIME_DATA_FILE) as f:
        return f.read()

def write_report():
    global report
    with open('../CERNTasks.wiki/CMS_Tier_Reports.md', 'w') as f:
        f.write(report)

def commit_report():
    os.system('(cd ../CERNTasks.wiki/; git add -A; git commit -m "Auto-commiting report"; git push origin master)')

def make_plot(result, file_path):
    axes = result.plot(kind='bar', subplots=True, layout=(2,1), figsize=(8, 6), fontsize=6)

    axes[0][0].set_title('')
    axes[1][0].set_title('')
    axes[0][0].set_ylabel('Number')
    axes[1][0].set_ylabel('Terabytes')

    plt.xticks(rotation=45, horizontalalignment='right')
    plt.tight_layout()
    plot_filename = file_path + '_plot.jpg'
    plt.savefig(plot_filename, dpi=120)

    return plot_filename

def analyse_phedex_data():
    df = pd.read_csv('phedex_df.csv')
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

        plot_filename = make_plot(result, PHEDEX_PLOTS_PATH + site)

        append_report('### Plot')
        append_report('![5 most significant data-tiers](images/%s)' % plot_filename)

    time = read_phedex_time_data()
    append_report('#### Spark job run time: {0}'.format(time))

    # Move plot files to wiki repo
    copy_directory(PHEDEX_PLOTS_PATH, '../CERNTasks.wiki/images/' + PHEDEX_PLOTS_PATH)

def analyse_dbs_data():
    df = pd.read_csv('dbs_df.csv')
    result = df.groupby(df.dataset.str.split('/').str[3]).agg({'size': 'sum', 'dataset': 'count'})

    result = result.rename(columns={'size': 'sum_size', 'dataset': 'tier_count'})

    result.sort_values('tier_count', ascending=False, inplace=True)

    # Bytes to terabytes
    result['sum_size'] = result['sum_size'] / 1000000000000

    append_report('## DBS data. Showing TOP 5 most significant data-tiers')
    write_df_to_report(result, 5)

    plot_filename = make_plot(result, DBS_PLOTS_PATH + 'dbs')

    append_report('### Plot')
    append_report('![5 most significant data-tiers](images/%s)' % plot_filename)

    time = read_dbs_time_data()
    append_report('#### Spark job run time: {0}'.format(time))

    # Move plot file to wiki repo
    copy_directory(DBS_PLOTS_PATH, '../CERNTasks.wiki/images/' + DBS_PLOTS_PATH)

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
    opts = parser.parse_args()

    create_plot_dirs()
    append_report_header()
  
    analyse_phedex_data()
    analyse_dbs_data()
    aggregate_all_datastreams_info()

    write_report()

    if opts.commit == True:
        commit_report()

if __name__ == '__main__':
    main()
