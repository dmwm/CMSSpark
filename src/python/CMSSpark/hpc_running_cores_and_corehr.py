#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
#
# HPC sites' CoreHrs sum and Running cores calculations.

import click
import os
from datetime import datetime, date, timedelta

import pandas as pd
import plotly.express as px
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, format_string, from_unixtime, lit, unix_timestamp, when,
    avg as _avg,
    dayofmonth as _dayofmonth,
    max as _max,
    month as _month,
    round as _round,
    sum as _sum,
    year as _year,
)
from pyspark.sql.types import (
    StructType,
    LongType,
    StringType,
    StructField,
    DoubleType,
)

DEFAULT_HDFS_FOLDER = '/project/monitoring/archive/condor/raw/metric'

# Bottom to top bar stack order which set same colors for same site always
HPC_SITES_STACK_ORDER = ['ANL', 'BSC', 'CINECA', 'HOREKA', 'NERSC', 'OSG', 'PSC', 'RWTH', 'SDSC', 'TACC']
VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
CSV_DIR = 'csv'
HTML_DIR = 'html'
SITES_HTML_DIR = 'site_htmls'


def get_spark_session():
    """Get or create the spark context and session.
    """
    sc = SparkContext(appName='cms-monitoring-hpc-cpu-corehr')
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def get_candidate_files(
    start_date, end_date, spark, base=DEFAULT_HDFS_FOLDER,
):
    """Returns a list of hdfs folders that can contain data for the given dates."""
    st_date = start_date - timedelta(days=1)
    ed_date = end_date + timedelta(days=1)
    days = (ed_date - st_date).days
    sc = spark.sparkContext
    # The candidate files are the folders to the specific dates,
    # but if we are looking at recent days the compaction procedure could
    # have not run yet, so we will consider also the .tmp folders.
    candidate_files = [
        f"{base}/{(st_date + timedelta(days=i)).strftime('%Y/%m/%d')}{{,.tmp}}"
        for i in range(0, days)
    ]
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = FileSystem.get(URI("hdfs:///"), sc._jsc.hadoopConfiguration())
    candidate_files = [url for url in candidate_files if fs.globStatus(Path(url))]
    return candidate_files


def _get_schema():
    return StructType(
        [
            StructField(
                "data",
                StructType(
                    [
                        StructField("GlobalJobId", StringType(), nullable=False),
                        StructField("RecordTime", LongType(), nullable=False),
                        StructField("Status", StringType(), nullable=True),
                        StructField("Site", StringType(), nullable=True),
                        StructField("CoreHr", DoubleType(), nullable=True),
                        StructField("WallClockHr", DoubleType(), nullable=False),
                        StructField("CpuTimeHr", DoubleType(), nullable=True),
                        StructField("RequestCpus", DoubleType(), nullable=True),
                        StructField("GLIDEIN_Site", StringType(), nullable=True),
                        StructField("MachineAttrCMSSubSiteName0", StringType(), nullable=True),
                        StructField("MachineAttrCMSProcessingSiteName0", StringType(), nullable=True),
                    ]
                ),
            ),
        ]
    )


def process_and_get_pd_dfs(spark, start_date, end_date):
    schema = _get_schema()
    raw_df = (
        spark.read.option(
            'basePath', DEFAULT_HDFS_FOLDER
        ).json(
            get_candidate_files(start_date, end_date, spark, base=DEFAULT_HDFS_FOLDER), schema=schema,
        ).select(
            'data.*'
        ).filter(
            col("RecordTime").between(f"{start_date.timestamp() * 1000}", f"{end_date.timestamp() * 1000}")
        ).filter(
            (col('Site') == 'T3_US_ANL') |  # ANL
            (col('Site') == 'T3_US_NERSC') |  # NERSC
            (col('Site') == 'T3_US_OSG') |  # OSG
            (col('Site') == 'T3_US_PSC') |  # PSC
            (col('Site') == 'T3_US_SDSC') |  # SDSC
            (col('Site') == 'T3_US_TACC') |  # TACC
            ((col('Site').endswith('_ES_PIC_BSC')) & (col('MachineAttrCMSSubSiteName0') == 'PIC-BSC')) |  # BSC
            ((col('Site') == 'T1_IT_CNAF') & (col('MachineAttrCMSSubSiteName0') == 'CNAF-CINECA')) |  # CINECA
            ((col('Site') == 'T1_DE_KIT') & (col('MachineAttrCMSSubSiteName0') == 'KIT-HOREKA')) |  # HOREKA
            ((col('Site') == 'T2_DE_RWTH') & (col('MachineAttrCMSSubSiteName0') == 'RWTH-HPC'))  # RWTH
        ).filter(
            col('Status').isin(['Running', 'Completed'])
        ).withColumn(
            'date',
            from_unixtime((col('RecordTime') / 1000))
        ).withColumn(
            'site_name',
            when(col('Site') == 'T3_US_ANL', lit("ANL"))
                .when(col('Site') == 'T3_US_NERSC', lit("NERSC"))
                .when(col('Site') == 'T3_US_OSG', lit("OSG"))
                .when(col('Site') == 'T3_US_PSC', lit("PSC"))
                .when(col('Site') == 'T3_US_SDSC', lit("SDSC"))
                .when(col('Site') == 'T3_US_TACC', lit("TACC"))
                .when(col('Site').endswith('_ES_PIC_BSC'), lit("BSC"))
                .when(col('MachineAttrCMSSubSiteName0') == 'CNAF-CINECA', lit("CINECA"))
                .when(col('MachineAttrCMSSubSiteName0') == 'KIT-HOREKA', lit("HOREKA"))
                .when(col('MachineAttrCMSSubSiteName0') == 'RWTH-HPC', lit("RWTH"))
        ).withColumn(
            "RequestCpus",
            when(col("RequestCpus").isNotNull(), col("RequestCpus")).otherwise(lit(1)),
        ).withColumn(
            'dayofmonth',
            _dayofmonth(col('date'))
        ).withColumn(
            'month',
            concat_ws('-', _year(col('date')), format_string('%02d', _month(col('date'))))  # 2-digit month, default 1
        ).drop(
            'Site', 'MachineAttrCMSSubSiteName0'
        ).withColumnRenamed('site_name', 'site')
    )

    # There should be only Completed status for a GlobalJobId
    df_core_hr = raw_df.filter(col('Status') == 'Completed') \
        .drop_duplicates(["GlobalJobId"])

    df_core_hr_daily = df_core_hr.groupby(['site', 'month', 'dayofmonth']) \
        .agg(_round(_sum("CoreHr")).alias("sum CoreHr"))

    df_core_hr_monthly = df_core_hr.groupby(['site', 'month']) \
        .agg(_round(_sum("CoreHr")).alias("sum CoreHr"))

    sec_12_min = 60 * 12
    time_window_12m = from_unixtime(unix_timestamp('date') - unix_timestamp('date') % sec_12_min)

    # 1st group-by includes GlobaljobId to get running cores of GlobaljobId without duplicates in each 12 minutes window
    # 2nd group-by gets sum of RequestCpus in 12 minutes window
    # 3rd group-by gets avg of RequestCpus(12 minutes window) for each site for each month
    df_running_cores_daily = raw_df \
        .withColumn('12m_window', time_window_12m) \
        .groupby(['site', 'month', 'dayofmonth', '12m_window', 'GlobalJobId']) \
        .agg(_max(col('RequestCpus')).alias('running_cores_of_single_job_in_12m')) \
        .groupby(['site', 'month', 'dayofmonth', '12m_window']) \
        .agg(_sum(col('running_cores_of_single_job_in_12m')).alias('running_cores_12m_sum')) \
        .groupby(['site', 'month', 'dayofmonth']) \
        .agg(_round(_avg(col('running_cores_12m_sum'))).alias('running_cores_avg_over_12m_sum'))
    return df_core_hr_daily.toPandas(), df_running_cores_daily.toPandas(), df_core_hr_monthly.toPandas()


def get_fig_core_hr_daily_of_a_month(df_month, month, output_dir, url_prefix):
    """Creates plotly figure from one month of data for daily core hours and write csv"""
    csv_fname = month + '_core_hr.csv'
    csv_output_path = os.path.join(os.path.join(output_dir, CSV_DIR), csv_fname)
    csv_link = f"{url_prefix}/{CSV_DIR}/{csv_fname}"
    fig = px.bar(df_month, x="dayofmonth", y="sum CoreHr", color='site',
                 category_orders={
                     'site': HPC_SITES_STACK_ORDER,
                     'dayofmonth': [day for day in range(1, 32)]
                 },
                 title='CoreHrs - ' + month +
                       ' <b><a href="{}">[Source]</a></b>'.format(csv_link),
                 labels={
                     'sum CoreHr': 'CoreHr',
                     'dayofmonth': 'date',
                 },
                 width=800, height=600,
                 )
    fig.update_xaxes(tickprefix=month + "-", tickangle=300, tickmode='linear')
    fig.update_yaxes(automargin=True, tickformat=".2f")
    fig.update_layout(hovermode='x')

    # Write source data to csv
    df_month.sort_values(by=['month', 'dayofmonth', 'site']).to_csv(csv_output_path, index=False)
    return fig


def get_fig_running_cores_daily_of_a_month(df_month, month, output_dir, url_prefix):
    """Creates plotly figure from one month of data for daily running cores and write csv"""
    csv_fname = month + '_running_cores.csv'
    csv_output_path = os.path.join(os.path.join(output_dir, CSV_DIR), csv_fname)
    csv_link = f"{url_prefix}/{CSV_DIR}/{csv_fname}"
    fig = px.bar(df_month, x="dayofmonth", y="running_cores_avg_over_12m_sum", color='site',
                 category_orders={
                     'site': HPC_SITES_STACK_ORDER,
                     'dayofmonth': [day for day in range(1, 32)]
                 },
                 title='Running Cores avg of 12m sum - ' + month +
                       ' <b><a href="{}">[Source]</a></b>'.format(csv_link),
                 labels={
                     'running_cores_avg_over_12m_sum': 'running_cores',
                     'dayofmonth': 'date',
                 },
                 width=800, height=600,
                 )
    fig.update_xaxes(tickprefix=month + "-", tickangle=300, tickmode='linear')
    fig.update_yaxes(automargin=True, tickformat=".2f")
    fig.update_layout(hovermode='x')

    # Write source data to csv
    df_month.sort_values(by=['month', 'dayofmonth', 'site']).to_csv(csv_output_path, index=False)
    return fig


def write_2_daily_plots_to_single_html_each_month(df_tmp_core_hr, df_tmp_running_cores, month, output_dir, url_prefix):
    """Joins core_hr and running_cores plots in a single html side by side"""
    # Get figures
    fig1 = get_fig_core_hr_daily_of_a_month(df_month=df_tmp_core_hr, month=month, output_dir=output_dir,
                                            url_prefix=url_prefix)
    fig2 = get_fig_running_cores_daily_of_a_month(df_month=df_tmp_running_cores, month=month, output_dir=output_dir,
                                                  url_prefix=url_prefix)
    # Join 2 plots in one html to show CoreHr and running cores plots side-by-side on X-axis
    html_head = '<head><style>#plot1, #plot2 {display: inline-block;width: 49%;}</style></head>'
    fig1_div = '<div id="plot1">{}</div>'.format(fig1.to_html(full_html=False, include_plotlyjs='cdn'))
    fig2_div = '<div id="plot2">{}</div>'.format(fig2.to_html(full_html=False, include_plotlyjs='cdn'))

    html_output_file = os.path.join(os.path.join(output_dir, HTML_DIR), month + '.html')
    with open(html_output_file, 'w') as f:
        f.write(html_head + fig1_div + fig2_div)


def handle_core_hr_monthly_sum_of_all(df_core_hr_monthly, sorted_months, output_dir, url_prefix):
    """Creates plotly figure from all data with monthly granularity for core hours"""
    # Name is join of first and last month names of the used data
    fname_pre = 'all_monthly_core_hr'
    csv_output_path = os.path.join(output_dir, fname_pre + '.csv')
    html_output_path = os.path.join(output_dir, fname_pre + '.html')
    csv_link = f"{url_prefix}/{fname_pre}.csv"

    fig = px.bar(df_core_hr_monthly, x="month", y="sum CoreHr", color='site',
                 category_orders={
                     'site': HPC_SITES_STACK_ORDER,
                     'month': sorted_months,
                 },
                 title='CoreHrs - monthly sum'
                       + '<b><a href="{}">[Source]</a></b>'.format(csv_link),
                 labels={
                     'sum CoreHr': 'CoreHr',
                     'month': 'date'
                 },
                 width=800, height=600,
                 )
    fig.update_xaxes(dtick="M1", tickangle=300)
    fig.update_yaxes(automargin=True, tickformat=".2f")
    fig.update_layout(hovermode='x')

    # Write html
    with open(html_output_path, 'w') as f:
        f.write(fig.to_html(full_html=True, include_plotlyjs='cdn'))
    # Write source csv
    df_core_hr_monthly.sort_values(by=['month', 'site']).to_csv(csv_output_path, index=False)


def handle_core_hr_monthly_sum_of_all_for_each_site(df_core_hr_monthly, sorted_months, output_dir):
    """Creates plotly figure from all data with monthly granularity for core hours of each individual site"""
    for site in HPC_SITES_STACK_ORDER:
        df_tmp = df_core_hr_monthly[df_core_hr_monthly['site'] == site]
        fig = px.bar(df_tmp, x="month", y="sum CoreHr",
                     category_orders={
                         'month': sorted_months,
                     },
                     title=site + ' - CoreHrs monthly sum',
                     labels={
                         'sum CoreHr': 'CoreHr',
                         'month': 'date'
                     },
                     width=800, height=600,
                     )
        fig.update_xaxes(dtick="M1", tickangle=300)
        fig.update_yaxes(automargin=True, tickformat=".2f")
        fig.update_layout(hovermode='x')
        # Write html
        fname = site + '.html'

        html_output_path = os.path.join(os.path.join(output_dir, SITES_HTML_DIR), fname)
        with open(html_output_path, 'w') as f:
            f.write(fig.to_html(full_html=True, include_plotlyjs='cdn'))


def prepare_site_urls(url_prefix):
    """Prepares site plot links"""
    html_div_site_links_block = ""
    for site in HPC_SITES_STACK_ORDER:
        site_plot_url = f"{url_prefix}/{SITES_HTML_DIR}/{site}.html"
        html_a_template = f'<a href="{site_plot_url}" target="_blank">{site}</a> '
        html_div_site_links_block += html_a_template
    return html_div_site_links_block


def add_footer(html):
    """Footer calculation for total row, requires adding html elements"""
    # Add footer for total
    current_str = '''</thead>
  <tbody>'''
    replace_str = f'''</thead>
  <tfoot>
    <tr >
      <th>TOTAL</th>
      {"".join(f"<th>{site}</th>" for site in HPC_SITES_STACK_ORDER)}
    </tr>
  </tfoot>
<tbody>'''
    html = html.replace(current_str, replace_str)
    return html


def create_main_html(df_core_hr_monthly, html_template, output_dir, url_prefix):
    """Creates main.html that shows monthly CoreHrs sums and embedded plots using datatable"""
    # Rows to columns
    df = pd.pivot_table(df_core_hr_monthly, values='sum CoreHr', index=['month'],
                        columns=['site'])
    # Remove column level and fill null values with 0
    df = df.reset_index().fillna(0)
    # Remove named column
    df.columns.name = None

    # Convert columns to integer values.
    df[HPC_SITES_STACK_ORDER] = df[HPC_SITES_STACK_ORDER].astype(int)

    main_column = df.month
    # To define selected column in JS, we need specific class name
    _formatted_col = '<a class="selname">' + main_column + '</a>'
    df['month'] = _formatted_col

    html = df.to_html(escape=False, index=False)
    html = html.replace(
        'table border="1" class="dataframe"',
        'table id="dataframe" class="display compact" style="width:100%;"',
    )
    html = html.replace('style="text-align: right;"', "")
    for site in HPC_SITES_STACK_ORDER:
        html = html.replace(f'<th>{site}</th>', f'<th class="sum">{site}</th>')

    html = add_footer(html)

    with open(html_template) as f:
        template = f.read()

    current_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    main_html = template.replace("___UPDATE_TIME___", current_date) \
        .replace("____SITE_PLOT_URLS____", prepare_site_urls(url_prefix)) \
        .replace("____MAIN_BLOCK____", html)

    with open(os.path.join(output_dir, 'main.html'), "w+") as f:
        f.write(main_html)


@click.command()
@click.option("--output_dir", required=True, default="./www/hpc_monthly", help="local output directory")
@click.option("--start_date", required=False, type=click.DateTime(VALID_DATE_FORMATS))
@click.option("--end_date", required=False, type=click.DateTime(VALID_DATE_FORMATS))
@click.option("--last_n_months", required=False, type=int, default=19, help="Last n months data will be used")
@click.option("--url_prefix", default="https://cmsdatapop.web.cern.ch/cmsdatapop/hpc_usage",
              help="CernBox eos folder link, will be used in plots to point to csv source data")
@click.option('--html_template', required=True, default=None, type=str,
              help='Path of htmltemplate.html file: ~/CMSSpark/src/html/hpc/htmltemplate.html')
@click.option('--save_pickle', required=False, default=True, type=bool,
              help='Stores df_core_hr_daily, df_running_cores_daily, df_core_hr_monthly to output_dir as pickle')
def main(start_date, end_date, output_dir, last_n_months, url_prefix, html_template, save_pickle):
    # Create subdirectories if does not exist
    os.makedirs(os.path.join(output_dir, CSV_DIR), exist_ok=True)
    os.makedirs(os.path.join(output_dir, HTML_DIR), exist_ok=True)
    os.makedirs(os.path.join(output_dir, SITES_HTML_DIR), exist_ok=True)

    url_prefix = url_prefix.strip("/")  # no slash at the end
    spark = get_spark_session()
    # Set TZ as UTC. Also set in the spark-submit confs.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # HDFS .tmp folders can be in 1 day ago, so 2 days ago is selected for latest end date
    _2_days_ago = datetime.combine(date.today() - timedelta(days=2), datetime.min.time())
    if not (start_date or end_date):
        end_date = _2_days_ago
        n_months_ago = _2_days_ago - relativedelta(months=last_n_months)
        start_date = datetime(year=n_months_ago.year, month=n_months_ago.month, day=1)  # day=1 is 1st day of the month
    elif not start_date:
        n_months_ago = end_date - relativedelta(months=last_n_months)
        start_date = datetime(year=n_months_ago.year, month=n_months_ago.month, day=1)
    elif not end_date:
        end_date = _2_days_ago
    if start_date > end_date:
        raise ValueError(
            f"start date ({start_date}) should be earlier than end date({end_date})"
        )
    print("Data will be processed between", start_date, "-", end_date)

    # Get pandas dataframes
    df_core_hr_daily, df_running_cores_daily, df_core_hr_monthly = process_and_get_pd_dfs(spark, start_date, end_date)

    # Save pickle files
    if save_pickle:
        df_core_hr_daily.to_pickle('core_hr_daily.pkl')
        df_running_cores_daily.to_pickle('running_cores_daily.pkl')
        df_core_hr_monthly.to_pickle('core_hr_monthly.pkl')
        # df_core_hr_daily = pd.read_pickle("core_hr_daily.pkl")

    # Set date order of yyyy-mm month strings
    sorted_months = sorted(df_core_hr_daily.month.unique(), key=lambda m: datetime.strptime(m, "%Y-%m"))

    # Write plots that have daily granularity for each month
    for month in sorted_months:
        # Get temporary data frames
        df_tmp_core_hr = df_core_hr_daily[df_core_hr_daily['month'] == month]
        df_tmp_running_cores = df_running_cores_daily[df_running_cores_daily['month'] == month]
        write_2_daily_plots_to_single_html_each_month(df_tmp_core_hr=df_tmp_core_hr,
                                                      df_tmp_running_cores=df_tmp_running_cores,
                                                      month=month, output_dir=output_dir, url_prefix=url_prefix)

    # Full data of monthly sum core hr
    handle_core_hr_monthly_sum_of_all(df_core_hr_monthly=df_core_hr_monthly, sorted_months=sorted_months,
                                      output_dir=output_dir, url_prefix=url_prefix)
    # Create individual plots for each site
    handle_core_hr_monthly_sum_of_all_for_each_site(df_core_hr_monthly=df_core_hr_monthly, sorted_months=sorted_months,
                                                    output_dir=output_dir)
    # Create main html
    create_main_html(df_core_hr_monthly=df_core_hr_monthly, html_template=html_template, output_dir=output_dir,
                     url_prefix=url_prefix)

    # Write whole core hr and running cores daily data to csv
    df_core_hr_daily.sort_values(by=['month', 'dayofmonth', 'site']) \
        .to_csv(os.path.join(output_dir, 'all_core_hr_daily.csv'), index=False)
    df_running_cores_daily.sort_values(by=['month', 'dayofmonth', 'site']) \
        .to_csv(os.path.join(output_dir, 'all_running_cores_daily.csv'), index=False)


if __name__ == "__main__":
    main()
