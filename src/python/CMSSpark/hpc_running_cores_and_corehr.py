#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : hpc_running_cores_and_corehr.py
Author      : Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : HPC sites' CoreHrs sum and Running cores calculations.

How "--iterative" works
  - previous pandas dataframes saved in pickles/new directory
  - in new run, Spark job runs for the data between 1st day of previous month and 2 days ago of current day
  - saved dataframes(pickles) read and last 2 months rows dropped them
  - final dataframe created by concatenation of new partial one and saved(last 2 months dropped) one
"""

# system modules
import os
import shutil
from datetime import datetime, timedelta, timezone

import click
import pandas as pd
import plotly.express as px
from pyspark.sql.functions import (
    col, concat_ws, format_string, from_unixtime, lit, unix_timestamp, when,
    avg as _avg, dayofmonth as _dayofmonth, max as _max, month as _month, round as _round, sum as _sum, year as _year,
)
from pyspark.sql.types import StructType, LongType, StringType, StructField, DoubleType

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session, get_candidate_files

# global variables

_BASE_HDFS_CONDOR = '/project/monitoring/archive/condor/raw/metric'

# Bottom to top bar stack order which set same colors for same site always
_HPC_SITES_STACK_ORDER = ['ANL', 'BSC', 'CINECA', 'HOREKA', 'NERSC', 'OSG', 'PSC', 'RWTH', 'SDSC', 'TACC']
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
_CSV_DIR = 'csv'
_HTML_DIR = 'html'
_SITES_HTML_DIR = 'site_htmls'
_YEARS_DIR = 'years'
_PICKLE_DIR = 'pickles'


def _get_schema():
    """Returns only required fields"""
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
                        StructField("RequestCpus", DoubleType(), nullable=True),
                        StructField("MachineAttrCMSSubSiteName0", StringType(), nullable=True),
                    ]
                ),
            ),
        ]
    )


def get_pandas_dfs(spark, start_date, end_date):
    schema = _get_schema()
    raw_df = (
        spark.read.option(
            'basePath', _BASE_HDFS_CONDOR
        ).json(
            get_candidate_files(start_date, end_date, spark, base=_BASE_HDFS_CONDOR, day_delta=2), schema=schema,
        ).select(
            'data.*'
        ).filter(
            (col("RecordTime") >= (start_date.replace(tzinfo=timezone.utc).timestamp() * 1000)) &
            (col("RecordTime") < (end_date.replace(tzinfo=timezone.utc).timestamp() * 1000))
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


def get_figure_of_daily_core_hr_for_one_month(df_month, month, output_dir, url_prefix):
    """Creates plotly figure from one month of data for daily core hours and write to csv
    """
    csv_fname = month + '_core_hr.csv'
    csv_output_path = os.path.join(os.path.join(output_dir, _CSV_DIR), csv_fname)
    csv_link = f"{url_prefix}/{_CSV_DIR}/{csv_fname}"
    fig = px.bar(df_month, x="dayofmonth", y="sum CoreHr", color='site',
                 category_orders={
                     'site': _HPC_SITES_STACK_ORDER,
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


def get_figure_of_daily_running_cores_for_one_month(df_month, month, output_dir, url_prefix):
    """Creates plotly figure from one month of data for daily running cores and write csv
    """
    csv_fname = month + '_running_cores.csv'
    csv_output_path = os.path.join(os.path.join(output_dir, _CSV_DIR), csv_fname)
    csv_link = f"{url_prefix}/{_CSV_DIR}/{csv_fname}"
    fig = px.bar(df_month, x="dayofmonth", y="running_cores_avg_over_12m_sum", color='site',
                 category_orders={
                     'site': _HPC_SITES_STACK_ORDER,
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


def html_join_core_hr_and_running_cores_for_one_month(df_tmp_core_hr, df_tmp_running_cores, month, output_dir,
                                                      url_prefix):
    """Joins core_hr and running_cores plots in a single html side by side
    """
    # Get figures
    fig1 = get_figure_of_daily_core_hr_for_one_month(df_month=df_tmp_core_hr, month=month, output_dir=output_dir,
                                                     url_prefix=url_prefix)
    fig2 = get_figure_of_daily_running_cores_for_one_month(df_month=df_tmp_running_cores, month=month,
                                                           output_dir=output_dir,
                                                           url_prefix=url_prefix)
    # Join 2 plots in one html to show CoreHr and running cores plots side-by-side on X-axis
    html_head = '<head><style>#plot1, #plot2 {display: inline-block;width: 49%;}</style></head>'
    fig1_div = '<div id="plot1">{}</div>'.format(fig1.to_html(full_html=False, include_plotlyjs='cdn'))
    fig2_div = '<div id="plot2">{}</div>'.format(fig2.to_html(full_html=False, include_plotlyjs='cdn'))

    html_output_file = os.path.join(os.path.join(output_dir, _HTML_DIR), month + '.html')
    with open(html_output_file, 'w') as f:
        f.write(html_head + fig1_div + fig2_div)


def produce_monthly_core_hr_of_all_years(df_core_hr_monthly, sorted_months, output_dir, url_prefix):
    """Creates plotly figure from all data with monthly granularity for core hours
    """
    # Name is join of first and last month names of the used data
    fname_pre = 'all_monthly_core_hr'
    csv_output_path = os.path.join(output_dir, fname_pre + '.csv')
    html_output_path = os.path.join(output_dir, fname_pre + '.html')
    csv_link = f"{url_prefix}/{fname_pre}.csv"

    fig = px.bar(df_core_hr_monthly, x="month", y="sum CoreHr", color='site',
                 category_orders={
                     'site': _HPC_SITES_STACK_ORDER,
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


def get_full_path(output_dir, fname, extension):
    """Returns full path of file
    """
    if not isinstance(fname, str):
        fname = str(fname)
    return os.path.join(output_dir, fname + "." + extension)


def produce_monthly_core_hr_for_each_year(df_core_hr_monthly, sorted_months, output_dir, url_prefix):
    """Creates plotly figures from all data with monthly granularity for core hours for each year
    """
    # Write years results to specific directory
    output_dir = os.path.join(output_dir, _YEARS_DIR)

    # get unique years
    years = sorted(set(datetime.strptime(m, "%Y-%m").year for m in sorted_months))

    for year in years:
        # Filter for that year
        df_tmp_year = df_core_hr_monthly[df_core_hr_monthly.month.str.startswith(str(year) + "-")]
        csv_link = f"{url_prefix}/{_YEARS_DIR}/{year}.csv"
        fig = px.bar(df_tmp_year, x="month", y="sum CoreHr", color='site',
                     category_orders={
                         'site': _HPC_SITES_STACK_ORDER,
                         'month': sorted_months,
                     },
                     title=f'CoreHrs - monthly sum - {year}'
                           + f'<b><a href="{csv_link}">[Source]</a></b>',
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
        with open(get_full_path(output_dir, year, "html"), 'w') as f:
            f.write(fig.to_html(full_html=True, include_plotlyjs='cdn'))
        # Write source csv
        df_tmp_year.sort_values(by=['month', 'site']).to_csv(get_full_path(output_dir, year, "csv"), index=False)


def process_monthly_core_hr_for_each_site(df_core_hr_monthly, sorted_months, output_dir):
    """Creates plotly figure from all data with monthly granularity for core hours of each individual site
    """
    for site in _HPC_SITES_STACK_ORDER:
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

        html_output_path = os.path.join(os.path.join(output_dir, _SITES_HTML_DIR), fname)
        with open(html_output_path, 'w') as f:
            f.write(fig.to_html(full_html=True, include_plotlyjs='cdn'))


def prepare_site_urls_html_div(url_prefix):
    """Prepares site plot links
    """
    html_div_site_links_block = ""
    for site in _HPC_SITES_STACK_ORDER:
        site_plot_url = f"{url_prefix}/{_SITES_HTML_DIR}/{site}.html"
        html_a_template = f'<a href="{site_plot_url}" target="_blank">{site}</a> '
        html_div_site_links_block += html_a_template
    return html_div_site_links_block


def prepare_year_urls_html_div(url_prefix, sorted_months):
    """Prepares year plot links
    """
    html_div_year_links_block = ""
    years = sorted(set(datetime.strptime(m, "%Y-%m").year for m in sorted_months))
    for year in years:
        year_plot_url = f"{url_prefix}/{_YEARS_DIR}/{year}.html"
        html_a_template = f'<a href="{year_plot_url}" target="_blank">{year}</a> '
        html_div_year_links_block += html_a_template
    all_core_hr_url = f"{url_prefix}/all_monthly_core_hr.html"
    html_a_template = f'<a href="{all_core_hr_url}" target="_blank">All Core Hr</a> '
    html_div_year_links_block += html_a_template
    return html_div_year_links_block


def add_footer(html):
    """Footer calculation for total row, requires adding html elements
    """
    # Add footer for total
    current_str = '''</thead>
  <tbody>'''
    replace_str = f'''</thead>
  <tfoot>
    <tr >
      <th>TOTAL</th>
      {"".join(f"<th>{site}</th>" for site in _HPC_SITES_STACK_ORDER)}
    </tr>
  </tfoot>
<tbody>'''
    html = html.replace(current_str, replace_str)
    return html


def create_main_html(df_core_hr_monthly, html_template, output_dir, url_prefix, sorted_months):
    """Creates main.html that shows monthly CoreHrs sums and embedded plots using datatable
    """
    global _HPC_SITES_STACK_ORDER
    # Rows to columns
    df = pd.pivot_table(df_core_hr_monthly, values='sum CoreHr', index=['month'],
                        columns=['site'])
    # Remove column level and fill null values with 0
    df = df.reset_index().fillna(0)
    # Remove named column
    df.columns.name = None

    # Drop columns not in default HPC site list
    not_exist = set(_HPC_SITES_STACK_ORDER) - set(df.columns)
    _HPC_SITES_STACK_ORDER = [e for e in _HPC_SITES_STACK_ORDER if e not in not_exist]
    # Convert columns to integer values.
    df[_HPC_SITES_STACK_ORDER] = df[_HPC_SITES_STACK_ORDER].astype(int)

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
    for site in _HPC_SITES_STACK_ORDER:
        html = html.replace(f'<th>{site}</th>', f'<th class="sum">{site}</th>')

    html = add_footer(html)

    with open(html_template) as f:
        template = f.read()

    current_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    main_html = template.replace("___UPDATE_TIME___", current_date) \
        .replace("____SITE_PLOT_URLS____", prepare_site_urls_html_div(url_prefix)) \
        .replace("____YEAR_PLOT_URLS____", prepare_year_urls_html_div(url_prefix, sorted_months)) \
        .replace("____MAIN_BLOCK____", html)

    with open(os.path.join(output_dir, 'main.html'), "w+") as f:
        f.write(main_html)


def dates_iterative():
    """Iteratively arrange start date and end date according
    """
    # end date is 2 days ago
    end_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    # start date is first day of previous month
    last_day_of_prev_month = datetime.today().replace(day=1) - timedelta(days=1)
    start_date = last_day_of_prev_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # current month string
    curr_month_str = datetime.today().strftime('%Y-%m')
    # previous month string
    prev_month_str = last_day_of_prev_month.strftime('%Y-%m')
    return start_date, end_date, curr_month_str, prev_month_str


@click.command()
@click.option("--output_dir", required=True, default="./www/hpc_monthly", help="local output directory")
@click.option("--start_date", required=False, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--end_date", required=False, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--iterative", required=False, is_flag=True, show_default=True, default=False,
              help="Iterative start and end date setting. Please set your cron job later than 3rd day of the month")
@click.option("--url_prefix", default="https://cmsdatapop.web.cern.ch/cmsdatapop/hpc_usage",
              help="CernBox eos folder link, will be used in plots to point to csv source data")
@click.option('--html_template', required=True, default=None, type=str,
              help='Path of htmltemplate.html file: ~/CMSSpark/src/html/hpc/htmltemplate.html')
@click.option('--save_pickle', required=False, default=True, type=bool,
              help='Stores df_core_hr_daily, df_running_cores_daily, df_core_hr_monthly to output_dir as pickle')
def main(start_date, end_date, output_dir, iterative, url_prefix, html_template, save_pickle):
    """
        Creates HPC resource usage in https://cmsdatapop.web.cern.ch/cmsdatapop/hpc_usage/main.html

        Please investigate how it is run in bin/cron4hpc_usage.sh because pages are produced in iterative mode to not
        run Spark Job on a couple of years data

        Iterative:
            The aim of iterative is not to lost old data even HDFS folders are deleted. In each run, Spark job will
            not run over all 3+ years of data. Spark job will run over last 2 months of data, and it will be concat with
            saved dataframe(in pickle format). Hence, final dataframes will consist of all data till 2 days ago.
    """
    # Set TZ as UTC. Also set in the spark-submit confs.
    spark = get_spark_session(app_name='cms-monitoring-hpc-cpu-corehr')
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    url_prefix = url_prefix.rstrip("/")  # no slash at the end
    output_dir = output_dir.rstrip("/")  # no slash at the end

    # Create subdirectories if they do not exist
    for d in [_CSV_DIR, _HTML_DIR, _SITES_HTML_DIR, _YEARS_DIR,
              os.path.join(_PICKLE_DIR, 'new'), os.path.join(_PICKLE_DIR, 'old'),
              os.path.join(_PICKLE_DIR, 'raw')]:
        # PICKLE/raw : if not iterative, start and end time provided by user, then saves pickles to raw
        # PICKLE/new : if iterative, saves new dataframes as pickle to new
        # PICKLE/old : if iterative, saves previous run's dataframes as pickle to old
        os.makedirs(os.path.join(output_dir, d), exist_ok=True)

    if iterative:
        print("[INFO] Iterative process is starting...")
        start_date, end_date, curr_month_str, prev_month_str = dates_iterative()

        # Read existing pickle files and get df
        prev_df_chd = pd.read_pickle(f'{output_dir}/{_PICKLE_DIR}/new/core_hr_daily.pkl')
        prev_df_rcd = pd.read_pickle(f'{output_dir}/{_PICKLE_DIR}/new/running_cores_daily.pkl')
        prev_df_chm = pd.read_pickle(f'{output_dir}/{_PICKLE_DIR}/new/core_hr_monthly.pkl')

        # drop last 2 months
        prev_df_core_hr_daily = prev_df_chd[~ prev_df_chd.month.isin([curr_month_str, prev_month_str])]
        prev_df_running_cores_daily = prev_df_rcd[~ prev_df_rcd.month.isin([curr_month_str, prev_month_str])]
        prev_df_core_hr_monthly = prev_df_chm[~ prev_df_chm.month.isin([curr_month_str, prev_month_str])]

        print("[INFO]", curr_month_str, "and", prev_month_str, "months are dropped from saved dataframes")
        print("[INFO] Data will be processed between", start_date, "-", end_date)

        # RUN spark for new data
        df_core_hr_daily, df_running_cores_daily, df_core_hr_monthly = get_pandas_dfs(spark, start_date, end_date)

        # Concat them with old dataframe data
        df_core_hr_daily = pd.concat([prev_df_core_hr_daily, df_core_hr_daily], ignore_index=True)
        df_running_cores_daily = pd.concat([prev_df_running_cores_daily, df_running_cores_daily], ignore_index=True)
        df_core_hr_monthly = pd.concat([prev_df_core_hr_monthly, df_core_hr_monthly], ignore_index=True)
        print("[INFO] New dataframes are concatted with previous dataframes")
        print("[INFO] df_core_hr_daily shape:", df_core_hr_daily.shape)
        print("[INFO] df_running_cores_daily shape:", df_running_cores_daily.shape)
        print("[INFO] df_core_hr_monthly shape:", df_core_hr_monthly.shape)
    else:
        # RUN spark
        df_core_hr_daily, df_running_cores_daily, df_core_hr_monthly = get_pandas_dfs(spark, start_date, end_date)
        # Save pickle files
        if save_pickle:
            df_core_hr_daily.to_pickle(f'{output_dir}/{_PICKLE_DIR}/raw/core_hr_daily.pkl')
            df_running_cores_daily.to_pickle(f'{output_dir}/{_PICKLE_DIR}/raw/running_cores_daily.pkl')
            df_core_hr_monthly.to_pickle(f'{output_dir}/{_PICKLE_DIR}/raw/core_hr_monthly.pkl')

    # Set date order of yyyy-mm month strings
    sorted_months = sorted(df_core_hr_daily.month.unique(), key=lambda m: datetime.strptime(m, "%Y-%m"))

    # Write plots that have daily granularity for each month
    for month in sorted_months:
        # Get temporary data frames
        df_tmp_core_hr = df_core_hr_daily[df_core_hr_daily['month'] == month]
        df_tmp_running_cores = df_running_cores_daily[df_running_cores_daily['month'] == month]
        html_join_core_hr_and_running_cores_for_one_month(df_tmp_core_hr=df_tmp_core_hr,
                                                          df_tmp_running_cores=df_tmp_running_cores,
                                                          month=month, output_dir=output_dir, url_prefix=url_prefix)

    # Full data of monthly sum core hr
    produce_monthly_core_hr_of_all_years(df_core_hr_monthly=df_core_hr_monthly,
                                         sorted_months=sorted_months,
                                         output_dir=output_dir,
                                         url_prefix=url_prefix)

    # Create individual plots for each site
    process_monthly_core_hr_for_each_site(df_core_hr_monthly=df_core_hr_monthly,
                                          sorted_months=sorted_months,
                                          output_dir=output_dir)

    # Create individual plots for each year
    produce_monthly_core_hr_for_each_year(df_core_hr_monthly=df_core_hr_monthly,
                                          sorted_months=sorted_months,
                                          output_dir=output_dir,
                                          url_prefix=url_prefix)

    # Create main html
    create_main_html(df_core_hr_monthly=df_core_hr_monthly,
                     html_template=html_template,
                     output_dir=output_dir,
                     url_prefix=url_prefix,
                     sorted_months=sorted_months)

    # Write whole core hr and running cores daily data to csv
    df_core_hr_daily.sort_values(by=['month', 'dayofmonth', 'site']) \
        .to_csv(os.path.join(output_dir, 'all_core_hr_daily.csv'), index=False)
    df_running_cores_daily.sort_values(by=['month', 'dayofmonth', 'site']) \
        .to_csv(os.path.join(output_dir, 'all_running_cores_daily.csv'), index=False)

    if iterative:
        # This should be run as a last step,
        # because we don't want to lost current pickle by replacing it with the new problematic df
        for pkl in ['core_hr_daily.pkl', 'running_cores_daily.pkl', 'core_hr_monthly.pkl']:
            shutil.copy2(f'{output_dir}/{_PICKLE_DIR}/new/{pkl}', f'{output_dir}/{_PICKLE_DIR}/old/{pkl}')

        # Save current dataframes to "pickles/new" directory
        df_core_hr_daily.to_pickle(f'{output_dir}/{_PICKLE_DIR}/new/core_hr_daily.pkl')
        df_running_cores_daily.to_pickle(f'{output_dir}/{_PICKLE_DIR}/new/running_cores_daily.pkl')
        df_core_hr_monthly.to_pickle(f'{output_dir}/{_PICKLE_DIR}/new/core_hr_monthly.pkl')


if __name__ == "__main__":
    main()
