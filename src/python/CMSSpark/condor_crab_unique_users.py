#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
"""
Generate datasets and plots for CRAB unique users count
either by week of year or by month for a given calendar year.
"""
import os
from datetime import datetime, timezone, timedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    LongType,
    StringType,
    StructField,
    DoubleType,
)
from pyspark.sql.functions import (
    regexp_extract,
    date_format,
    from_unixtime,
    substring_index,
    col,
    input_file_name,
    concat,
    year,
    month,
    weekofyear,
    lpad,
    countDistinct,
)
import matplotlib.pyplot as plt
import seaborn as sns
import click

_BASE_PATH = "/project/monitoring/archive/condor/raw/metric"
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
_VALID_BY = ("weekofyear", "month")


def get_spark_session(yarn=True, verbose=False):
    """
        Get or create the spark context and session.
    """
    sc = SparkContext(appName="cms-crab-unique-users")
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def _get_candidate_files(start_date, end_date, spark, base=_BASE_PATH):
    """
    Returns a list of hdfs folders that can contain data for the given dates.
    """
    st_date = start_date - timedelta(days=1)
    ed_date = end_date + timedelta(days=1)
    days = (ed_date - st_date).days
    pre_candidate_files = [
        "{base}/{day}{{,.tmp}}".format(
            base=base, day=(st_date + timedelta(days=i)).strftime("%Y/%m/%d")
        )
        for i in range(0, days)
    ]
    sc = spark.sparkContext
    # The candidate files are the folders to the specific dates,
    # but if we are looking at recent days the compaction procedure could
    # have not run yet so we will considerate also the .tmp folders.
    candidate_files = [
        "/project/monitoring/archive/condor/raw/metric/{}{{,.tmp}}".format(
            (st_date + timedelta(days=i)).strftime("%Y/%m/%d")
        )
        for i in range(0, days)
    ]
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = FileSystem.get(URI("hdfs:///"), sc._jsc.hadoopConfiguration())
    candidate_files = [url for url in candidate_files if fs.globStatus(Path(url))]
    return candidate_files


def _get_crab_condor_schema():
    """
    Define the subset of the condor schema that we will use
    in this application.
    """
    schema = StructType(
        [
            StructField(
                "data",
                StructType(
                    [
                        StructField("GlobalJobId", StringType(), nullable=False),
                        StructField("RecordTime", LongType(), nullable=False),
                        StructField("CRAB_UserHN", StringType(), nullable=True),
                        StructField("Status", StringType(), nullable=True),
                        StructField("Site", StringType(), nullable=True),
                        StructField("Type", StringType(), nullable=True),
                    ]
                ),
            ),
        ]
    )
    return schema


def get_crab_unique_users(
    start_date, end_date, by="month", verbose=False, base=_BASE_PATH
):
    """
    Query the hdfs data and returns a pandas dataframe with:
    year, [weekofyear/month], count(DISTINCT CRAB_UserHN)
    args:
        - start_date datetime Start of the query period (RecordTime)
        - end_date datetime End of the query period
    """
    if by not in _VALID_BY:
        raise ValueError("by must be one of %r." % _VALID_BY)
    start = int(start_date.timestamp() * 1000)
    end = int(end_date.timestamp() * 1000)
    spark = get_spark_session(yarn=True, verbose=verbose)

    dfs_raw = (
        spark.read.option("basePath", base)
        .json(
            _get_candidate_files(start_date, end_date, spark, base=base),
            schema=_get_crab_condor_schema(),
        )
        .select("data.*")
        .filter(
            f"""Status='Completed'
              AND Type='analysis'
              AND Site rlike '^T2_.*$'
              AND NOT Site rlike '.*_CERN.*'
              AND RecordTime>={start}
              AND RecordTime<{end}
              """
        )
        .withColumn("RecordDate", from_unixtime(col("RecordTime") / 1000))
        .withColumn(
            by, month("RecordDate") if by == "month" else weekofyear("RecordDate"),
        )
        .withColumn("year", year("RecordDate"))
    )
    grouped_sdf = (
        dfs_raw.dropDuplicates(["GlobalJobId"])
        .groupBy(["year", by])
        .agg(countDistinct("CRAB_UserHN"))
    )
    return grouped_sdf.toPandas()


def generate_plot(pdf, by, output_folder, filename):
    """
    Generates and save in the output_folder a bar plot
    for the given dataset. The dataset must include year,
    either a month or a weekofyear, and count(DISTINCT CRAB_UserHN).
    If the year is the same for all the records, only the month/weekofyear
    will be used in the labels, otherwise it will show year-month or
    year-weekofyear.
    """
    group_field = by if pdf.year.nunique() == 1 else "period"
    if group_field == "period":
        pdf["period"] = pdf.year.astype(str) + "-" + pdf["by"].str.zfill(2)
    sorted_pd = pdf.sort_values(group_field, ascending=False)
    _dims = (14, 8.5)
    fig, ax = plt.subplots(figsize=_dims)
    plot = sns.barplot(
        data=sorted_pd,
        x=group_field,
        y="count(DISTINCT CRAB_UserHN)",
        color="tab:blue",
    )
    fig.savefig(
        os.path.join(output_folder, f"{filename}.png"), bbox_inches="tight",
    )


@click.command()
@click.argument("start_date", nargs=1, type=click.DateTime(_VALID_DATE_FORMATS))
@click.argument("end_date", nargs=1, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option(
    "--by",
    default="month",
    type=click.Choice(_VALID_BY),
    help="Either weekofyear or month",
)
@click.option(
    "--generate_plots",
    default=False,
    is_flag=True,
    help="Additional to the csv, generate the plot(s)",
)
@click.option("--output_folder", default="./output", help="local output directory")
def main(start_date, end_date, output_folder, by="month", generate_plots=False):
    """
        This script will generate a dataset with the number of unique users of CRAB
        either by month or by weekofyear.
    """
    cp_pdf = get_crab_unique_users(start_date, end_date, by)
    os.makedirs(output_folder, exist_ok=True)
    filename = f"UniqueUsersBy_{by}_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
    cp_pdf.to_csv(os.path.join(output_folder, f"{filename}.csv"))
    if generate_plots:
        generate_plot(cp_pdf, by, output_folder, filename)


if __name__ == "__main__":
    main()
