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
import numpy as np
import plotly.express as px
from pyspark.sql.functions import (
    col, concat_ws, format_string, from_unixtime, lit, unix_timestamp, when,
    dayofmonth as _dayofmonth, expr as _expr, explode as _explode,
    max as _max, month as _month, round as _round, sum as _sum, year as _year,
)
from pyspark.sql.types import StructType, LongType, StringType, StructField, DoubleType

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session, get_candidate_files

# global variables

_BASE_HDFS_CONDOR = '/project/monitoring/archive/condor/raw/metric'

# Bottom to top bar stack order which set same colors for same site always
_HPC_SITES_STACK_ORDER = ['ANL', 'ANVIL', 'BSC', 'CINECA', 'HOREKA', 'NERSC', 'OSG', 'PSC', 'RWTH', 'SDSC', 'TACC']

# For new sites, please check list sizes
DISCRETE_COLOR_MAP = {site: px.colors.qualitative.Pastel[i] for i, site in enumerate(_HPC_SITES_STACK_ORDER)}

_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
_CSV_DIR = 'csv'
_HTML_DIR = 'html'
_SITES_HTML_DIR = 'site_htmls'
_YEARS_DIR = 'years'
_PICKLE_DIR = 'pickles'

# Producer cron prediod is in each 12 minutes. All time calculations are arranged according to this value.
PRODUCER_MINUTE_PERIOD = 12
NUMBER_OF_BINS_IN_DAY = (60 * 24) / PRODUCER_MINUTE_PERIOD


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


def get_raw_df(spark, start_date, end_date):
    """Creates pandas dataframes from HDFS data"""
    schema = _get_schema()
    df_raw = (
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
            (col('Site') == 'T3_US_Anvil') |  # ANVIL
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
            .when(col('Site') == 'T3_US_Anvil', lit("ANVIL"))
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
        ).withColumnRenamed('site_name', 'Site')
    )
    return df_raw.select(
        ['RecordTime', 'GlobalJobId', 'Status', 'CoreHr', 'RequestCpus', 'date', 'Site', 'month', 'dayofmonth'])


# ---------------------------------------------------------------------------------------------------------------------
#                                           PREPARE PANDAS DATAFRAMES
# ---------------------------------------------------------------------------------------------------------------------

def get_daily_template_dataframe(spark, start_date, end_date):
    """Creates a dummy dataframe from rows of all months and days between start and end time

    This is important to fill empty days and months with 0 values.
    """

    # Create a dummy dataframe which includes columns as max and min date for each site
    df_fill_na = spark.createDataFrame([(s, start_date, end_date) for s in _HPC_SITES_STACK_ORDER],
                                       ["Site", "min_date", "max_date"])

    # One column will include all days between start and end date in a list
    df_fill_na = df_fill_na.select("Site", _expr("sequence(min_date, max_date, interval 1 day)").alias("date"))

    # Explode will create a month and dayofmonth for each Site
    df_fill_na = df_fill_na \
        .withColumn("date", _explode("date")) \
        .withColumn("dayofmonth", _dayofmonth(col('date'))) \
        .withColumn('month',  # for 2-digit month, default is 1
                    concat_ws('-', _year(col('date')), format_string('%02d', _month(col('date')))))
    df_fill_na = df_fill_na.select(['Site', 'month', 'dayofmonth'])
    return df_fill_na


def get_pd_df_core_hours_sum_of_daily(spark, raw_df, start_date, end_date):
    """Returns daily and monthly sum of core hours pandas dataframes"""
    df_day_template = get_daily_template_dataframe(spark, start_date, end_date)

    # There should be only Completed status for a GlobalJobId
    df_core_hr = raw_df.filter(col('Status') == 'Completed') \
        .drop_duplicates(["GlobalJobId"])

    # Calculate sums
    df_spark_core_hr_daily = df_core_hr.groupby(['Site', 'month', 'dayofmonth']) \
        .agg(_round(_sum("CoreHr"), 1).alias("sum CoreHr"))

    # Fillna is important to fill all empty days with 0
    df_core_hr_daily = df_day_template.join(df_spark_core_hr_daily, ['Site', 'month', 'dayofmonth'], 'left') \
        .fillna(0).toPandas()

    return df_core_hr_daily


def get_core_hours_monthly_df_from_daily(df):
    """Creates monthly df from daily one"""
    return df.groupby(['Site', 'month']) \
        .agg({"sum CoreHr": np.sum}) \
        .round({'sum CoreHr': 2}) \
        .reset_index()


def get_pd_df_running_cores_avg_of_daily(spark, raw_df, start_date, end_date):
    """Prepare running cores: unique 12(currently) minutes average results

    Running cores calculation is based on finding sum results in each 12 minutes which is producer frequency
    Then, getting averages over days or months
    """
    df_day_template = get_daily_template_dataframe(spark, start_date, end_date)

    producer_seconds_period = 60 * PRODUCER_MINUTE_PERIOD
    # Currently 12 minutes windows
    time_window = from_unixtime(unix_timestamp('date') - unix_timestamp('date') % producer_seconds_period)

    # 1st group-by includes GlobaljobId to get running cores of GlobaljobId without duplicates in each 12 minutes window
    # 2nd group-by gets sum of RequestCpus in a day
    #   and then divide sum to NUMBER_OF_BINS_IN_DAY value to calculate the 12 minutes average.
    df_spark_running_cores_daily = raw_df \
        .filter(col('Status') == 'Running') \
        .withColumn('time_window', time_window) \
        .groupby(['Site', 'month', 'dayofmonth', 'time_window', 'GlobalJobId']
                 ).agg(_max(col('RequestCpus')).alias('running_cores_of_single_job_in_window')) \
        .groupby(['Site', 'month', 'dayofmonth']
                 ).agg(_round(_sum(col('running_cores_of_single_job_in_window')) / NUMBER_OF_BINS_IN_DAY, 1)
                       .alias('running_cores_daily_avg')
                       )

    # Fillna is important to fill all empty days with 0
    df_running_cores_daily = df_day_template \
        .join(df_spark_running_cores_daily, ['Site', 'month', 'dayofmonth'], 'left') \
        .fillna(0).toPandas()
    return df_running_cores_daily


def get_running_cores_monthly_df_from_daily(df):
    """Creates monthly df from daily one"""
    # group-by gets avg of RequestCpus(12 minutes window) for each site for each month
    return df.groupby(['Site', 'month']) \
        .agg({'running_cores_daily_avg': np.average}) \
        .round({'running_cores_daily_avg': 2}) \
        .reset_index()


# ---------------------------------------------------------------------------------------------------------------------
#                                 CREATE DAILY PLOTS
# ---------------------------------------------------------------------------------------------------------------------
def create_plot_of_daily_sum_of_core_hours(df_month, month, output_dir, url_prefix):
    """Creates plotly figure from one month of data for daily core hours and write to csv
    """
    csv_fname = month + '_core_hr.csv'
    csv_output_path = os.path.join(os.path.join(output_dir, _CSV_DIR), csv_fname)
    csv_link = f"{url_prefix}/{_CSV_DIR}/{csv_fname}"
    fig = px.bar(df_month, x="dayofmonth", y="sum CoreHr", color='Site', text="sum CoreHr",
                 color_discrete_map=DISCRETE_COLOR_MAP,
                 category_orders={
                     'Site': _HPC_SITES_STACK_ORDER,
                     'dayofmonth': [day for day in range(1, 32)]
                 },
                 title='Number of cores hours - ' + month +
                       ' <b><a href="{}">[Source]</a></b>'.format(csv_link),
                 labels={
                     'sum CoreHr': 'Number of core hours ',
                     'dayofmonth': 'Date ',
                 },
                 width=800, height=600,
                 )
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_xaxes(title_font=dict(size=18, family='Courier'), tickfont=dict(family='Rockwell', size=14),
                     tickprefix=month + "-", tickangle=300, tickmode='linear')
    fig.update_yaxes(separatethousands=True, title_font=dict(size=18, family='Courier'), automargin=True)
    fig.update_layout(hovermode='x')

    # Write source data to csv
    df_month.sort_values(by=['month', 'dayofmonth', 'Site']).to_csv(csv_output_path, index=False)
    return fig


def create_plot_of_daily_avg_of_running_cores(df_month, month, output_dir, url_prefix):
    """Creates plotly figure from one month of data for daily running cores and write csv
    """
    csv_fname = month + '_running_cores.csv'
    csv_output_path = os.path.join(os.path.join(output_dir, _CSV_DIR), csv_fname)
    csv_link = f"{url_prefix}/{_CSV_DIR}/{csv_fname}"
    fig = px.bar(df_month, x="dayofmonth", y="running_cores_daily_avg", color='Site',
                 text="running_cores_daily_avg",
                 color_discrete_map=DISCRETE_COLOR_MAP,
                 category_orders={
                     'Site': _HPC_SITES_STACK_ORDER,
                     'dayofmonth': [day for day in range(1, 32)]
                 },
                 title='Number of running cores - daily averages - ' + month +
                       ' <b><a href="{}">[Source]</a></b>'.format(csv_link),
                 labels={
                     'running_cores_daily_avg': 'Number of cores',
                     'dayofmonth': 'Date ',
                 },
                 width=800, height=600,
                 )
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_xaxes(title_font=dict(size=18, family='Courier'), tickfont=dict(family='Rockwell', size=14),
                     tickprefix=month + "-", tickangle=300, tickmode='linear')
    fig.update_yaxes(separatethousands=True, title_font=dict(size=18, family='Courier'), automargin=True)
    fig.update_layout(hovermode='x')

    # Write source data to csv
    df_month.sort_values(by=['month', 'dayofmonth', 'Site']).to_csv(csv_output_path, index=False)
    return fig


def create_html_of_joint_two_plots_for_monthly_row(df_tmp_core_hr, df_tmp_running_cores, month,
                                                   output_dir, url_prefix):
    """Joins core_hr and running_cores plots in a single html side by side
    """
    # Get figures
    fig1 = create_plot_of_daily_sum_of_core_hours(df_month=df_tmp_core_hr, month=month, output_dir=output_dir,
                                                  url_prefix=url_prefix)
    fig2 = create_plot_of_daily_avg_of_running_cores(df_month=df_tmp_running_cores, month=month,
                                                     output_dir=output_dir,
                                                     url_prefix=url_prefix)
    # Join 2 plots in one html to show CoreHr and running cores plots side-by-side on X-axis
    html_head = '<head><style>#plot1, #plot2 {display: inline-block;width: 49%;}</style></head>'
    fig1_div = '<div id="plot1">{}</div>'.format(fig1.to_html(full_html=False, include_plotlyjs='cdn'))
    fig2_div = '<div id="plot2">{}</div>'.format(fig2.to_html(full_html=False, include_plotlyjs='cdn'))

    html_output_file = os.path.join(os.path.join(output_dir, _HTML_DIR), month + '.html')
    with open(html_output_file, 'w') as f:
        f.write(html_head + fig1_div + fig2_div)


# ---------------------------------------------------------------------------------------------------------------------
#                                 CREATE MONTHLY PLOTS
# ---------------------------------------------------------------------------------------------------------------------

def create_plot_of_core_hour_monthly(df, sorted_months, csv_link, title_extra=""):
    fig = px.bar(df, x="month", y="sum CoreHr", color='Site', text="sum CoreHr",
                 color_discrete_map=DISCRETE_COLOR_MAP,
                 category_orders={
                     'Site': _HPC_SITES_STACK_ORDER,
                     'month': sorted_months,
                 },
                 title='Number of cores hours - Monthly sum' + title_extra
                       + '<b><a href="{}">[Source]</a></b>'.format(csv_link),
                 labels={
                     'sum CoreHr': 'Number of cores hours ',
                     'month': 'Date '
                 },
                 width=800, height=600,
                 )
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_xaxes(title_font=dict(size=18, family='Courier'), tickfont=dict(size=14, family='Rockwell'),
                     dtick="M1", tickangle=300)
    fig.update_yaxes(separatethousands=True, title_font=dict(size=18, family='Courier'), automargin=True)
    fig.update_layout(hovermode='x')
    return fig


def create_plot_of_running_cores_monthly(df, sorted_months, csv_link, title_extra=""):
    fig = px.bar(df, x="month", y="running_cores_daily_avg", color='Site',
                 text="running_cores_daily_avg",
                 color_discrete_map=DISCRETE_COLOR_MAP,
                 category_orders={
                     'Site': _HPC_SITES_STACK_ORDER,
                     'month': sorted_months,
                 },
                 title='Number of running cores - Monthly average' + title_extra
                       + '<b><a href="{}">[Source]</a></b>'.format(csv_link),
                 labels={
                     'running_cores_daily_avg': 'Number of cores',
                     'month': 'Date '
                 },
                 width=800, height=600,
                 )
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_xaxes(title_font=dict(size=18, family='Courier'), tickfont=dict(size=14, family='Rockwell'),
                     dtick="M1", tickangle=300)
    fig.update_yaxes(separatethousands=True, title_font=dict(size=18, family='Courier'), automargin=True)
    fig.update_layout(hovermode='x')
    return fig


def create_html_of_monthly_two_plots(df_tmp_core_hr, df_tmp_running_cores, sorted_months, output_dir, url_prefix,
                                     name_prefix, title_extra):
    """Joins core_hr and running_cores plots in a single html side by side

    output_dir: output directory of all csv and html files
    name_prefix: will be used as prefix to csv files but full name to html file
    """

    # Core hr
    csv_fname = name_prefix + '_monthly_core_hr.csv'
    csv_output_path = os.path.join(output_dir, csv_fname)
    csv_link = f"{url_prefix}/{csv_fname}"
    df_tmp_core_hr.sort_values(by=['month', 'Site']).to_csv(csv_output_path, index=False)
    fig1 = create_plot_of_core_hour_monthly(df=df_tmp_core_hr, sorted_months=sorted_months, csv_link=csv_link,
                                            title_extra=title_extra)

    # Running cores
    csv_fname = name_prefix + '_monthly_running_cores.csv'
    csv_output_path = os.path.join(output_dir, csv_fname)
    csv_link = f"{url_prefix}/{csv_fname}"
    df_tmp_running_cores.sort_values(by=['month', 'Site']).to_csv(csv_output_path, index=False)
    fig2 = create_plot_of_running_cores_monthly(df=df_tmp_running_cores, sorted_months=sorted_months, csv_link=csv_link,
                                                title_extra=title_extra)

    # Join 2 plots in one html to show CoreHr and running cores plots side-by-side on X-axis
    html_head = '<head><style>#plot1, #plot2 {display: inline-block;width: 49%;}</style></head>'
    fig1_div = '<div id="plot1">{}</div>'.format(fig1.to_html(full_html=False, include_plotlyjs='cdn'))
    fig2_div = '<div id="plot2">{}</div>'.format(fig2.to_html(full_html=False, include_plotlyjs='cdn'))

    html_output_file = os.path.join(output_dir, name_prefix + '.html')
    with open(html_output_file, 'w') as f:
        f.write(html_head + fig1_div + fig2_div)


def create_and_save_plots_of_monthly_plots_for_all(df_core_hr_monthly, df_running_cores_monthly,
                                                   sorted_months, output_dir, url_prefix):
    """Creates plotly figure from all data with monthly granularity for core hours
    """
    name_prefix = "all"
    create_html_of_monthly_two_plots(df_core_hr_monthly, df_running_cores_monthly, sorted_months, output_dir,
                                     url_prefix, name_prefix, title_extra="")


def create_and_save_monthly_plots_for_each_year(df_core_hr_monthly, df_running_cores_monthly,
                                                sorted_months, output_dir, url_prefix):
    """Creates plotly figures from all data with monthly granularity for core hours for each year
    """
    # Write years results to specific directory
    output_dir = os.path.join(output_dir, _YEARS_DIR)
    url_prefix = f"{url_prefix}/{_YEARS_DIR}"

    # get unique years
    years = sorted(set(datetime.strptime(m, "%Y-%m").year for m in sorted_months))

    for year in years:
        # Filter for that year
        year = str(year)
        name_prefix = year
        df_year_core_hr = df_core_hr_monthly[df_core_hr_monthly.month.str.startswith(year + "-")]
        df_year_running_cores = df_running_cores_monthly[df_running_cores_monthly.month.str.startswith(year + "-")]
        create_html_of_monthly_two_plots(df_year_core_hr, df_year_running_cores, sorted_months, output_dir,
                                         url_prefix, name_prefix, title_extra=f" {year} - ")


def create_and_save_monthly_core_hours_for_each_site(df_core_hr_monthly, df_running_cores_monthly,
                                                     sorted_months, output_dir, url_prefix):
    """Creates plotly figure from all data with monthly granularity for core hours of each individual site
    """
    # Write years results to specific directory
    output_dir = os.path.join(output_dir, _SITES_HTML_DIR)
    url_prefix = f"{url_prefix}/{_SITES_HTML_DIR}"

    for site in _HPC_SITES_STACK_ORDER:
        name_prefix = site
        df1_core_hr = df_core_hr_monthly[df_core_hr_monthly['Site'] == site]
        df2_running_cores = df_running_cores_monthly[df_running_cores_monthly['Site'] == site]
        create_html_of_monthly_two_plots(df1_core_hr, df2_running_cores, sorted_months, output_dir,
                                         url_prefix, name_prefix, title_extra=f" {site} - ")


def get_full_path(output_dir, fname, extension):
    """Returns full path of file"""
    if not isinstance(fname, str):
        fname = str(fname)
    return os.path.join(output_dir, fname + "." + extension)


def prepare_site_urls_html_div(url_prefix):
    """Prepares sites' plots links. Will be replaced with ____SITE_PLOT_URLS____ in html template"""
    html_div_site_links_block = []
    for site in _HPC_SITES_STACK_ORDER:
        site_plot_url = f"{url_prefix}/{_SITES_HTML_DIR}/{site}.html"
        html_a = f'<a href="{site_plot_url}" target="_blank">{site}</a>'
        html_div_site_links_block.append(html_a)
    return " &#9550; ".join(html_div_site_links_block)


def prepare_year_urls_html_div(url_prefix, sorted_months):
    """Prepares each year's plot links. Will be replaced with ____YEAR_PLOT_URLS____ in html template"""
    html_div_year_links_block = []
    # all of them
    all_url = f"{url_prefix}/all.html"
    html_a_template = f'<a href="{all_url}" target="_blank">ALL MONTHLY</a> '
    html_div_year_links_block.append(html_a_template)
    # years
    years = sorted(set(datetime.strptime(m, "%Y-%m").year for m in sorted_months))
    for year in years:
        year_plot_url = f"{url_prefix}/{_YEARS_DIR}/{year}.html"
        html_a_template = f'<a href="{year_plot_url}" target="_blank">{year}</a> '
        html_div_year_links_block.append(html_a_template)
    return " &#9550; ".join(html_div_year_links_block)


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
                        columns=['Site'])
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

    current_date = datetime.utcnow().strftime("%Y-%m-%d")
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
        run Spark Job over a couple of years data each daily run.

        Iterative:
            The aim of iterative process is not to lost old data even HDFS folders are deleted.
            In each run, Spark job will not run over all 3+ years of data. Spark job will run over last 2 months
            of data, and it will be concatenated with saved dataframes(in pickle format).
            Therefore, final dataframes will consist of all data till 2 days ago.
    """
    # Set TZ as UTC. Also set in the spark-submit confs.
    spark = get_spark_session(app_name='cms-monitoring-hpc-cpu-corehr')
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    url_prefix = url_prefix.rstrip("/")  # no slash at the end
    output_dir = output_dir.rstrip("/")  # no slash at the end

    # get raw df
    df_raw = get_raw_df(spark, start_date, end_date)

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

        # drop last 2 months
        prev_df_core_hr_daily = prev_df_chd[~ prev_df_chd.month.isin([curr_month_str, prev_month_str])]
        prev_df_running_cores_daily = prev_df_rcd[~ prev_df_rcd.month.isin([curr_month_str, prev_month_str])]

        print("[INFO]", curr_month_str, "and", prev_month_str, "months are dropped from saved dataframes")
        print("[INFO] Data will be processed between", start_date, "-", end_date)

        # RUN spark for new data
        df_core_hr_daily = get_pd_df_core_hours_sum_of_daily(spark, df_raw, start_date, end_date)
        df_running_cores_daily = get_pd_df_running_cores_avg_of_daily(spark, df_raw, start_date, end_date)

        # Concat new dataframes with old dataframes
        df_core_hr_daily = pd.concat([prev_df_core_hr_daily, df_core_hr_daily], ignore_index=True)
        df_running_cores_daily = pd.concat([prev_df_running_cores_daily, df_running_cores_daily], ignore_index=True)

        # Create monthlies from dailies
        df_core_hr_monthly = get_core_hours_monthly_df_from_daily(df_core_hr_daily)
        df_running_cores_monthly = get_running_cores_monthly_df_from_daily(df_running_cores_daily)

        print("[INFO] New dataframes are concatenated with previous dataframes")
        print("[INFO] df_core_hr_daily shape:", df_core_hr_daily.shape)
        print("[INFO] df_running_cores_daily shape:", df_running_cores_daily.shape)
        print("[INFO] df_core_hr_monthly shape:", df_core_hr_monthly.shape)
        print("[INFO] df_core_hr_monthly shape:", df_running_cores_monthly.shape)
    else:
        # RUN spark for new data
        df_core_hr_daily = get_pd_df_core_hours_sum_of_daily(spark, df_raw, start_date, end_date)
        df_running_cores_daily = get_pd_df_running_cores_avg_of_daily(spark, df_raw, start_date, end_date)
        df_core_hr_monthly = get_core_hours_monthly_df_from_daily(df_core_hr_daily)
        df_running_cores_monthly = get_running_cores_monthly_df_from_daily(df_running_cores_daily)

        # Save pickle files
        if save_pickle:
            df_core_hr_daily.to_pickle(f'{output_dir}/{_PICKLE_DIR}/raw/core_hr_daily.pkl')
            df_running_cores_daily.to_pickle(f'{output_dir}/{_PICKLE_DIR}/raw/running_cores_daily.pkl')

    # Set date order of yyyy-mm month strings
    sorted_months = sorted(df_core_hr_daily.month.unique(), key=lambda m: datetime.strptime(m, "%Y-%m"))

    # Write plots that have daily granularity for each month
    for month in sorted_months:
        # Get temporary data frames
        df_tmp_core_hr = df_core_hr_daily[df_core_hr_daily['month'] == month]
        df_tmp_running_cores = df_running_cores_daily[df_running_cores_daily['month'] == month]
        create_html_of_joint_two_plots_for_monthly_row(df_tmp_core_hr=df_tmp_core_hr,
                                                       df_tmp_running_cores=df_tmp_running_cores,
                                                       month=month, output_dir=output_dir,
                                                       url_prefix=url_prefix)

    # Full data of monthly sum core hr
    create_and_save_plots_of_monthly_plots_for_all(df_core_hr_monthly=df_core_hr_monthly,
                                                   df_running_cores_monthly=df_running_cores_monthly,
                                                   sorted_months=sorted_months,
                                                   output_dir=output_dir,
                                                   url_prefix=url_prefix)

    # Create individual plots for each site
    create_and_save_monthly_core_hours_for_each_site(df_core_hr_monthly=df_core_hr_monthly,
                                                     df_running_cores_monthly=df_running_cores_monthly,
                                                     sorted_months=sorted_months,
                                                     output_dir=output_dir, url_prefix=url_prefix)

    # Create individual plots for each year
    create_and_save_monthly_plots_for_each_year(df_core_hr_monthly=df_core_hr_monthly,
                                                df_running_cores_monthly=df_running_cores_monthly,
                                                sorted_months=sorted_months,
                                                output_dir=output_dir,
                                                url_prefix=url_prefix)

    # Create main html
    create_main_html(df_core_hr_monthly=df_core_hr_monthly,
                     html_template=html_template,
                     output_dir=output_dir,
                     url_prefix=url_prefix,
                     sorted_months=sorted_months)

    if iterative:
        # This should be run as a last step,
        # because we don't want to lost current pickle by replacing it with the new problematic df
        for pkl in ['core_hr_daily.pkl', 'running_cores_daily.pkl', 'core_hr_monthly.pkl']:
            shutil.copy2(f'{output_dir}/{_PICKLE_DIR}/new/{pkl}', f'{output_dir}/{_PICKLE_DIR}/old/{pkl}')

        # Save current dataframes to "pickles/new" directory
        df_core_hr_daily.to_pickle(f'{output_dir}/{_PICKLE_DIR}/new/core_hr_daily.pkl')
        df_running_cores_daily.to_pickle(f'{output_dir}/{_PICKLE_DIR}/new/running_cores_daily.pkl')


if __name__ == "__main__":
    main()
