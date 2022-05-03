import os

import click
import plotly.express as px
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, concat_ws, from_unixtime, lit, unix_timestamp, when,
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

DEFAULT_HDFS_FOLDER = "/project/monitoring/archive/condor/raw/metric"

# Bottom to top bar stack order which set same colors for same site always
HPC_SITES_STACK_ORDER = ['ANL', 'BSC', 'CINECA', 'HOREKA', 'NERSC', 'OSG', 'PSC', 'RWTH', 'SDSC', 'TACC']
VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]


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
            ((col('Site') == 'T3_ES_PIC_BSC') & (col('MachineAttrCMSSubSiteName0') == 'PIC-BSC')) |  # BSC
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
                .when(col('Site') == 'T3_ES_PIC_BSC', lit("BSC"))
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
            concat_ws('-', _year(col('date')), _month(col('date')))
        ).drop(
            'Site', 'MachineAttrCMSSubSiteName0'
        ).withColumnRenamed('site_name', 'site')
    ).cache()

    # There should be only Completed status for a GlobalJobId
    df_core_hr = raw_df.filter(col('Status') == 'Completed') \
        .drop_duplicates(["GlobalJobId"])

    df_core_hr_DAILY = df_core_hr.groupby(['site', 'month', 'dayofmonth']) \
        .agg(_round(_sum("CoreHr")).alias("sum CoreHr"))

    df_core_hr_MONTHLY = df_core_hr.groupby(['site', 'month']) \
        .agg(_round(_sum("CoreHr")).alias("sum CoreHr"))

    sec_12_min = 60 * 12
    time_window_12m = from_unixtime(unix_timestamp('date') - unix_timestamp('date') % sec_12_min)

    # 1st group-by includes GlobaljobId to get running cores of GlobaljobId without duplicates in each 12 minutes window
    # 2nd group-by gets sum of RequestCpus in 12 minutes window
    # 3rd group-by gets avg of RequestCpus(12 minutes window) for each site for each month
    df_running_cores = raw_df \
        .withColumn('12m_window', time_window_12m) \
        .groupby(['site', 'month', 'dayofmonth', '12m_window', 'GlobalJobId']) \
        .agg(_max(col('RequestCpus')).alias('running_cores_of_single_job_in_12m')) \
        .groupby(['site', 'month', 'dayofmonth', '12m_window']) \
        .agg(_sum(col('running_cores_of_single_job_in_12m')).alias('running_cores_12m_sum')) \
        .groupby(['site', 'month', 'dayofmonth']) \
        .agg(_round(_avg(col('running_cores_12m_sum'))).alias('running_cores_avg_over_12m_sum'))
    return df_core_hr_DAILY.toPandas(), df_running_cores.toPandas(), df_core_hr_MONTHLY.toPandas()


def get_fig_core_hr_of_daily_one_month(df_month, month, sites_stack_order):
    fig = px.bar(df_month, x="dayofmonth", y="sum CoreHr", color='site',
                 category_orders={
                     'site': sites_stack_order,
                     'dayofmonth': [day for day in range(1, 32)]
                 },
                 title="CoreHrs - " + month,
                 labels={
                     "sum CoreHr": "CoreHr",
                     "dayofmonth": "date",
                 },
                 width=600, height=400,
                 )
    fig.update_xaxes(tickprefix=month + "-", tickangle=300, tickmode='linear')
    fig.update_yaxes(automargin=True, tickformat=".2f")
    fig.update_layout(hovermode='x')
    return fig


def get_fig_core_hr_monthly(df_all, sites_stack_order, sorted_months):
    fig = px.bar(df_all, x="month", y="sum CoreHr", color='site',
                 category_orders={
                     'site': sites_stack_order,
                     'month': sorted_months,
                 },
                 title="CoreHrs - monthly sum",
                 labels={
                     "sum CoreHr": "CoreHr",
                     "month": "date",
                 },
                 width=600, height=400,
                 )
    fig.update_xaxes(dtick="M1", tickangle=300)
    fig.update_yaxes(automargin=True, tickformat=".2f")
    fig.update_layout(hovermode='x')
    return fig


def get_fig_running_cores_of_one_month(df_month, month, sites_stack_order):
    fig = px.bar(df_month, x="dayofmonth", y="running_cores_avg_over_12m_sum", color='site',
                 category_orders={
                     'site': sites_stack_order,
                     'dayofmonth': [day for day in range(1, 32)]
                 },
                 title="Running Cores avg of 12m sum - " + month,
                 labels={
                     "running_cores_avg_over_12m_sum": "running_cores",
                     "dayofmonth": "date",
                 },
                 width=600, height=400,
                 hover_data=['site', 'running_cores_avg_over_12m_sum'],
                 )
    fig.update_xaxes(tickprefix=month + "-", tickangle=300, tickmode='linear')
    fig.update_yaxes(automargin=True, tickformat=".2f")
    fig.update_layout(hovermode='x')
    return fig


@click.command()
@click.option("--start_date", type=click.DateTime(VALID_DATE_FORMATS))
@click.option("--end_date", type=click.DateTime(VALID_DATE_FORMATS))
@click.option("--output_dir", default="./www/hpc_monthly", help="local output directory")
@click.option("--last_n_months", type=int, default=19, help="Last n months data will be used")
def main(start_date=None, end_date=None, output_dir="./www/hpc_monthly", last_n_months=19, ):
    spark = get_spark_session()

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
        end_date = min(start_date + relativedelta(months=last_n_months), _2_days_ago)
    if start_date > end_date:
        raise ValueError(
            f"start date ({start_date}) should be earlier than end date({end_date})"
        )
    print("Data will be processed between", start_date, "-", end_date)
    # Get pandas dataframes
    df_core_hr_daily, df_running_cores, df_core_hr_monthly = process_and_get_pd_dfs(spark, start_date, end_date)

    # df_core_hr.head(1)
    # site  month   dayofmonth  sum CoreHr
    # SDSC  2021-9  22          99362.0
    #
    # df_running_cores.head(1)
    # site   month   dayofmonth running_cores_avg_over_12m_sum
    # CINECA 2021-1	 9          2650.0

    # Set date order of yyyy-mm month strings
    sorted_months = sorted(df_core_hr_daily.month.unique(), key=lambda m: datetime.strptime(m, "%Y-%M"))

    # CSV file names
    f_name_monthly_core_hr = 'monthly_core_hr_' + sorted_months[0] + '_' + sorted_months[-1]
    f_name_daily_core_hr = 'daily_core_hr_' + sorted_months[0] + '_' + sorted_months[-1]
    f_name_daily_running_cores = 'daily_running_cores_' + sorted_months[0] + '_' + sorted_months[-1]

    # Write main dataframe data to CSV
    df_core_hr_monthly.sort_values(by=['month', 'site']) \
        .to_csv(os.path.join(output_dir, f_name_monthly_core_hr + '.csv'), index=False)
    df_core_hr_daily.sort_values(by=['month', 'dayofmonth', 'site']) \
        .to_csv(os.path.join(output_dir, f_name_daily_core_hr + '.csv'), index=False)
    df_running_cores.sort_values(by=['month', 'dayofmonth', 'site']) \
        .to_csv(os.path.join(output_dir, f_name_daily_running_cores + '.csv'), index=False)

    # Write daily granularity plots for each month
    for month in sorted_months:
        #
        df_tmp_core_hr = df_core_hr_daily[df_core_hr_daily['month'] == month]
        fig1 = get_fig_core_hr_of_daily_one_month(df_tmp_core_hr, month, HPC_SITES_STACK_ORDER)
        #
        df_tmp_running_cores = df_running_cores[df_running_cores['month'] == month]
        fig2 = get_fig_running_cores_of_one_month(df_tmp_running_cores, month, HPC_SITES_STACK_ORDER)
        with open(os.path.join(output_dir, month + '.html'), 'w') as f:
            f.write(fig1.to_html(full_html=False, include_plotlyjs='cdn'))
            f.write(fig2.to_html(full_html=False, include_plotlyjs='cdn'))

    # Write monthly granularity Core Hrs plot
    fig3 = get_fig_core_hr_monthly(df_core_hr_monthly, HPC_SITES_STACK_ORDER, sorted_months)
    with open(os.path.join(output_dir, f_name_monthly_core_hr + '.html'), 'w') as f:
        f.write(fig3.to_html(full_html=True, include_plotlyjs='cdn'))


if __name__ == "__main__":
    main()
