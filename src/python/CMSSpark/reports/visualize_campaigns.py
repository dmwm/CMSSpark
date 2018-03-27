import pandas as pd
import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt
from report_builder import ReportBuilder
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.abspath(__file__)))))
from utils import bytes_to_pb_string, bytes_to_pib_string
import argparse

CAMPAIGNS_TIME_DATA_FILE = 'spark_exec_time_campaigns.txt'
CAMPAIGN_TIER_TIME_DATA_FILE = 'spark_exec_time_campaign_tier.txt'

report_builder = ReportBuilder()

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_report_dir():
    return '%s/../../../bash/CERNTasks.wiki' % get_script_dir()

def get_destination_dir():
    return '%s/../../../bash/report_campaigns' % get_script_dir()

def append_report(lines):
    report_builder.append(lines)
    report_builder.append('\n')

def write_campaigns_to_report(df, head=0):
    append_report('| Campaign | PhEDEx Size (PB - PiB) | DBS Size (PB - PiB) | Ratio | Most Significant Site | Second Most Significant Site | Most Significant Site Size (PB - PiB) | Second Most Significant Site Size (PB - PiB) | Number of Sites |')
    append_report('| ------- | ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['campaign'] + 
                      ' | ' + bytes_to_pb_string(row['phedex_size']) + ' - ' + bytes_to_pib_string(row['phedex_size']) + 
                      ' | ' + bytes_to_pb_string(row['dbs_size']) + ' - ' + bytes_to_pib_string(row['dbs_size']) + 
                      ' | ' + '{:.2f}'.format(float(row['phedex_size']/row['dbs_size'])) + 
                      ' | ' + row['mss_name'] + 
                      ' | ' + row['second_mss_name'] + 
                      ' | ' + bytes_to_pb_string(row['mss']) + ' - ' + bytes_to_pib_string(row['mss']) + 
                      ' | ' + bytes_to_pb_string(row['second_mss']) + ' - ' + bytes_to_pib_string(row['second_mss']) + 
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
    append_report('| Campaign | Tier | DBS Size (PB - PiB) | PhEDEx Size (PB - PiB) | Ratio | Size on Disk (PB - PiB) |')
    append_report('| ------- | ------ | ------ | ------ | ------ | ------ |')

    if head != 0:
        df = df[:head]

    for index, row in df.iterrows():
        append_report('| ' + row['campaign'] + 
                      ' | ' + row['tier'] + 
                      ' | ' + bytes_to_pb_string(row['dbs_size']) + ' - ' + bytes_to_pib_string(row['dbs_size']) +
                      ' | ' + bytes_to_pb_string(row['phedex_size']) + ' - ' + bytes_to_pib_string(row['phedex_size']) +
                      ' | ' + '{:.2f}'.format(float(row['phedex_size']/row['dbs_size'])) + 
                      ' | ' + bytes_to_pb_string(row['size_on_disk']) + ' - ' + bytes_to_pib_string(row['size_on_disk']) +
                      ' |')

def create_plot_dirs(plot_dir):
    plots_absolute_path = '%s/images/%s' % (get_report_dir(), plot_dir)

    if not os.path.exists(plots_absolute_path):
        os.makedirs(plots_absolute_path)

def append_report_header():
    append_report('# PhEDEx and DBS data aggregation based on campaigns for data from 2017-02-28')
    append_report('Results of gathering PhEDEx and DBS information aggregated by campaign')

def write_report(report):
    with open('%s/CMS_Campaigns_Report.md' % get_report_dir(), 'w') as f:
        f.write(report)

def commit_report():
    os.system('(cd %s/; git add -A; git commit -m "Auto-commiting report"; git push origin master)' % get_report_dir())

def append_campaign_execution_time():
    with open('%s/%s' % (get_destination_dir(), CAMPAIGNS_TIME_DATA_FILE), 'r') as f:
        append_report('#### Spark job execution time for data above: %s' % f.read())

def append_campaign_tier_execution_time():
    with open('%s/%s' % (get_destination_dir(), CAMPAIGN_TIER_TIME_DATA_FILE), 'r') as f:
        append_report('#### Spark job execution time for data above: %s' % f.read())

def plot_pie_charts(df, plot_file):
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

    plot_filepath = '%s/images/%s' % (get_report_dir(), plot_file)
    plt.savefig(plot_filepath, dpi=120)

def visualize_data_by_campaign(plots_dir):
    # Full datasets
    df = pd.read_csv('%s/campaigns_dbs_full_df.csv' % get_destination_dir())

    append_report('## Campaigns in all sites')
    
    append_report('### Showing TOP 10 most significant campaigns by DBS size')
    write_campaigns_to_report(df, 10)

    # Make pie chart of sites for most significant DBS campaigns
    plot_file = '%s/dbs_size_full_campaigns_plot.jpg' % plots_dir
    plot_pie_charts(df, plot_file)

    append_report('### Plot of 6 most significant DBS campaigns')
    append_report('Each pie chart visualizes the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant DBS campaigns](images/%s)' % plot_file)

    df = pd.read_csv('%s/campaigns_phedex_full_df.csv' % get_destination_dir())

    append_report('### Showing TOP 10 most significant campaigns by PhEDEx size')
    write_campaigns_to_report(df, 10)

    # Make pie chart of sites for most significant PhEDEx campaigns
    plot_file = '%s/phedex_size_full_campaigns_plot.jpg' % plots_dir
    plot_pie_charts(df, plot_file)

    append_report('### Plot of 6 most significant PhEDEx campaigns')
    append_report('Each pie chart visualizes the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant PhEDEx campaigns](images/%s)' % plot_file)

    # Only disk (tape datasets excluded)
    df = pd.read_csv('%s/campaigns_dbs_disk_only_df.csv' % get_destination_dir())

    append_report('## Campaigns only in disk')
    
    append_report('### Showing TOP 10 most significant campaigns by DBS size')
    write_campaigns_to_report(df, 10)

    # Make pie chart of sites for most significant DBS campaigns
    plot_file = '%s/dbs_size_disk_only_campaigns_plot.jpg' % plots_dir
    plot_pie_charts(df, plot_file)

    append_report('### Plot of 6 most significant DBS campaigns')
    append_report('Each pie chart visualizes the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant DBS campaigns](images/%s)' % plot_file)

    df = pd.read_csv('%s/campaigns_phedex_disk_only_df.csv' % get_destination_dir())

    append_report('### Showing TOP 10 most significant campaigns by PhEDEx size')
    write_campaigns_to_report(df, 10)

    # Make pie chart of sites for most significant PhEDEx campaigns
    plot_file = '%s/phedex_size_disk_only_campaigns_plot.jpg' % plots_dir
    plot_pie_charts(df, plot_file)

    append_report('### Plot of 6 most significant PhEDEx campaigns')
    append_report('Each pie chart visualizes the size of campaign data in each data site that campaign is present.')
    append_report('![6 most significant PhEDEx campaigns](images/%s)' % plot_file)

def visualize_site_campaign_count():
    # All sites
    df = pd.read_csv('%s/site_campaign_count_full_df.csv' % get_destination_dir())

    append_report('## All sites')

    append_report('### Showing TOP 10 most significant sites by campaign count')

    write_sites_to_report(df, 10)

    # Only disk (tape sites excluded)
    df = pd.read_csv('%s/site_campaign_count_disk_only_df.csv' % get_destination_dir())

    append_report('## Disk only sites')

    append_report('### Showing TOP 10 most significant sites by campaign count')

    write_sites_to_report(df, 10)
    
def visualize_campaign_tier_relationship():
    df = pd.read_csv('%s/campaign_tier_df.csv' % get_destination_dir())

    append_report('## Campaign sizes in data tiers')

    append_report('### Showing TOP 20 most significant campaign - tier pairs')
    
    write_campaign_tier_relationship_to_report(df, 20)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true",
                        dest="commit", 
                        default=False, 
                        help="Determines whether report should be committed to Github wiki")
    parser.add_argument("--plots_dir", action="store",
                        dest="plots_dir", default='campaign_plots', 
                        help="Determines directory where plots will be saved")
    opts = parser.parse_args()

    create_plot_dirs(opts.plots_dir)

    append_report_header()
  
    visualize_data_by_campaign(opts.plots_dir)
    visualize_site_campaign_count()
    append_campaign_execution_time()
    visualize_campaign_tier_relationship()
    append_campaign_tier_execution_time()

    write_report(report_builder.get())

    if opts.commit == True:
        commit_report()

if __name__ == '__main__':
    main()
