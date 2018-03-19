import pandas as pd

import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt
from subprocess import check_output
from math import log
from report_builder import ReportBuilder
import os
import shutil
import operator
import argparse

report_builder = ReportBuilder()

def append_report(lines):
    report_builder.append(lines)
    report_builder.append('\n')

def safe_round(value, decimal_points=1):
    """
    Rounds float to show at least decimal_points decimal digits.
    If all of them are zeros and -1 < value < 1, rounds to the first
    non-zero decimal digit.
    """
    sign = 1 if value >= 0 else -1
    value = abs(value)
    ndigits = int(1 - log(value, 10))
    return round(value, max(decimal_points, ndigits)) * sign

def to_pb_string(bytes, decimal_points=1):
    return str(safe_round(bytes / float(1000**5), decimal_points))

def to_pib_string(bytes, decimal_points=1):
    return str(safe_round(bytes / float(1024**5), decimal_points))

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_plots_path():
    return '%s/../../../bash/CERNTasks.wiki/images/campaign_plots/' % get_script_dir()

def write_campaigns_to_report(df, head=0):
    append_report('| Campaign | PhEDEx Size (PB - PiB) | DBS Size (PB - PiB) | Ratio | Most Significant Site | Second Most Significant Site | Most Significant Site Size (PB - PiB) | Second Most Significant Site Size (PB - PiB) | Number of Sites |')
    append_report('| ------- | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['campaign'] + 
                      ' | ' + to_pb_string(row['phedex_size']) + ' - ' + to_pib_string(row['phedex_size']) + 
                      ' | ' + to_pb_string(row['dbs_size']) + ' - ' + to_pib_string(row['dbs_size']) + 
                      ' | ' + '{:.2f}'.format(float(row['phedex_size']/row['dbs_size'])) + 
                      ' | ' + row['mss_name'] + 
                      ' | ' + row['second_mss_name'] + 
                      ' | ' + to_pb_string(row['mss']) + ' - ' + to_pib_string(row['mss']) + 
                      ' | ' + to_pb_string(row['second_mss']) + ' - ' + to_pib_string(row['second_mss']) + 
                      ' | ' + str(row['sites']) + 
                      ' |')
def write_sites_to_report(df, head=0):
    append_report('| Site | Campaign Count |')
    append_report('| ------- | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['site'] + ' | ' + str(int(row['campaign_count'])) + ' |')

def write_campaign_tier_relationship_to_report(df, head=0):
    append_report('| Campaign | Tier | DBS Size (PB - PiB) | PhEDEx Size (PB - PiB) | Size on Disk (PB - PiB) | Ratio |')
    append_report('| ------- | ------ | ------ | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['campaign'] + 
                      ' | ' + row['tier'] + 
                      ' | ' + to_pb_string(row['dbs_size']) + ' - ' + to_pib_string(row['dbs_size']) +
                      ' | ' + to_pb_string(row['phedex_size']) + ' - ' + to_pib_string(row['phedex_size']) +
                      ' | ' + to_pb_string(row['size_on_disk']) + ' - ' + to_pib_string(row['size_on_disk']) +
                      ' | ' + '{:.2f}'.format(float(row['phedex_size']/row['dbs_size'])) + 
                      ' |')

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

def create_plot_dirs():
    if not os.path.exists(get_plots_path()):
        os.makedirs(get_plots_path())

def append_report_header():
    append_report('# PhEDEx and DBS data aggregation based on campaigns for data from 2017-02-28')
    append_report('Results of gathering PhEDEx and DBS information aggregated by campaign')

def write_report(report):
    with open('%s/../../../bash/CERNTasks.wiki/CMS_Campaign_Reports.md' % get_script_dir(), 'w') as f:
        f.write(report)

def commit_report():
    os.system('(cd %s/../../../bash/CERNTasks.wiki/; git add -A; git commit -m "Auto-commiting report"; git push origin master)' % get_script_dir())

def append_campaign_execution_time():
    with open('%s/../../../bash/report_campaigns/spark_exec_time_campaigns.txt' % get_script_dir(), 'r') as f:
        append_report('#### Spark job execution time for data above: %s' % f.read())

def append_campaign_tier_execution_time():
    with open('%s/../../../bash/report_campaigns/spark_exec_time_campaign_tier.txt' % get_script_dir(), 'r') as f:
        append_report('#### Spark job execution time for data above: %s' % f.read())

def plot_pie_charts(df, file_name):
    head = df.head(6)\
             .set_index('campaign')\
             .drop(['mss_name', 'second_mss_name', 'mss', 'second_mss', 'dbs_size', 'phedex_size', 'sites'], axis=1)

    fig, axes = plt.subplots(2, 3, figsize=(30, 15))
    for i, (idx, row) in enumerate(head.iterrows()):
        ax = axes[i // 3, i % 3]
        row = row[row.gt(row.sum() * .01)]
        ax.pie(row, labels=row.index, startangle=30)
        ax.set_title(idx)
    
    plt.tight_layout()
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1, wspace=0.4)

    plot_filepath = get_plots_path() + file_name
    plt.savefig(plot_filepath, dpi=120)

def visualize_data_by_campaign():
    df = pd.read_csv('%s/../../../bash/report_campaigns/campaigns_dbs_df.csv' % get_script_dir())

    append_report('## Campaigns', )
    
    append_report('### Showing TOP 10 most significant campaigns by DBS size')
    write_campaigns_to_report(df, 10)

    # Make pie chart of sites for most significant DBS campaigns
    plot_filename = 'dbs_size_campaigns_plot.jpg'
    plot_pie_charts(df, plot_filename)

    append_report('### Plot of 6 most significant DBS campaigns')
    append_report('Each pie chart visualizes the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant DBS campaigns](images/campaign_plots/%s)' % plot_filename)

    df = pd.read_csv('%s/../../../bash/report_campaigns/campaigns_phedex_df.csv' % get_script_dir())

    append_report('### Showing TOP 10 most significant campaigns by PhEDEx size')
    write_campaigns_to_report(df, 10)

    # Make pie chart of sites for most significant PhEDEx campaigns
    plot_filename = 'phedex_size_campaigns_plot.jpg'
    plot_pie_charts(df, plot_filename)

    append_report('### Plot of 6 most significant PhEDEx campaigns')
    append_report('Each pie chart visualizes the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant PhEDEx campaigns](images/campaign_plots/%s)' % plot_filename)

def visualize_site_campaign_count():
    df = pd.read_csv('%s/../../../bash/report_campaigns/site_campaign_count_df.csv' % get_script_dir())

    append_report('## Sites')

    append_report('### Showing TOP 10 most significant sites by campaign count')

    write_sites_to_report(df, 10)

def visualize_campaign_tier_relationship():
    df = pd.read_csv('%s/../../../bash/report_campaigns/campaign_tier_df.csv' % get_script_dir())

    append_report('## Campaign sizes in data tiers')

    append_report('### Showing TOP 20 most significant campaign - tier pairs')
    
    write_campaign_tier_relationship_to_report(df, 20)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true",
                        dest="commit", 
                        default=False, 
                        help="Determines whether report should be committed to Github wiki")
    opts = parser.parse_args()

    create_plot_dirs()

    append_report_header()
  
    visualize_data_by_campaign()
    visualize_site_campaign_count()
    append_campaign_execution_time()
    visualize_campaign_tier_relationship()
    append_campaign_tier_execution_time()

    write_report(report_builder.get())

    if opts.commit == True:
        commit_report()

if __name__ == '__main__':
    main()
