#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : condor_hs06coreHrPlot.py
Author      : Christian Ariza <christian.ariza AT gmail [DOT] com>
Description : Generate datasets and plots for HS06 core hours ...
              ... either by week of year or by month for a given calendar year.
"""

# system modules
import os

import click
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import from_unixtime, col, year, month, weekofyear
from pyspark.sql.types import StructType, LongType, StringType, StructField, DoubleType

# CMSSpark modules
from CMSSpark.spark_utils import get_candidate_files, get_spark_session

# global variables
_BASE_HDFS_CONDOR = "/project/monitoring/archive/condor/raw/metric"
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
_VALID_BY = ("weekofyear", "month")


def _get_hs06_condor_schema():
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
                        StructField("HS06CpuTimeHr", DoubleType(), nullable=True),
                        StructField("Status", StringType(), nullable=True),
                        StructField("Site", StringType(), nullable=True),
                        StructField("Type", StringType(), nullable=True),
                    ]
                ),
            )
        ]
    )
    return schema


def get_hs06CpuTImeHr(start_date, end_date, by="month", base=_BASE_HDFS_CONDOR, include_re="^T2_.*$",
                      exclude_re=".*_CERN.*"):
    """Query the hdfs data and returns a pandas dataframe

    with: year, [weekofyear/month], sum(HS06CpuTimeHr)
    args:
        - start_date datetime Start of the query period (RecordTime)
        - end_date datetime End of the query period
    """
    if by not in _VALID_BY:
        raise ValueError(f"by must be one of {_VALID_BY}")
    start = int(start_date.timestamp() * 1000)
    end = int(end_date.timestamp() * 1000)
    spark = get_spark_session(app_name="cms-hs06corehrs_plot")

    dfs_raw = (
        spark.read.option("basePath", base)
        .json(
            get_candidate_files(start_date, end_date, spark, base=base, day_delta=1), schema=_get_hs06_condor_schema()
        )
        .select("data.*")
        .filter(
            f"""Status='Completed'
                  AND Site rlike '{include_re}' 
                  AND NOT Site rlike '{exclude_re}'
                  AND RecordTime>={start}
                  AND RecordTime<{end}
                  """
        )
        .withColumn("RecordDate", from_unixtime(col("RecordTime") / 1000))
        .withColumn("weekofyear", weekofyear("RecordDate"))
        .withColumn("month", month("RecordDate"))
        .withColumn("year", year("RecordDate"))
    )
    grouped_sdf = (
        dfs_raw.drop_duplicates(["GlobalJobId"])
        .groupby(["year", by])
        .agg({"HS06CpuTimeHr": "sum"})
    )
    return grouped_sdf.toPandas()


def generate_plot(pdf, by, output_folder, filename):
    """
    Generates and save in the output_folder a bar plot
    for the given dataset. The dataset must include year,
    either a month or a weekofyear, and sum(HS06 kdays).
    If the year is the same for all the records, only the month/weekofyear
    will be used in the labels, otherwise it will show year-month or
    year-weekofyear.
    """
    group_field = by if pdf.year.nunique() == 1 else "period"
    if group_field == "period":
        pdf["period"] = pdf.year.astype(str) + "-" + pdf[by].astype(str).str.zfill(2)
    sorted_pd = pdf.sort_values(group_field)
    _dims = (18, 8.5)
    fig, ax = plt.subplots(figsize=_dims)
    plot = sns.barplot(data=sorted_pd, x=group_field, y="HS06 kdays", color="tab:blue")
    fig.text(0.1, -0.08, filename, fontsize=10)
    fig.savefig(os.path.join(output_folder, f"{filename}.png"), bbox_inches="tight")


@click.command()
@click.option("--start_date", type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--end_date", type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--by", default="month", type=click.Choice(_VALID_BY), show_default=True,
              help="Either weekofyear or month")
@click.option("--include_re", default="^T2_.*$", show_default=True,
              help="Regular expression to select the sites to include in the plot")
@click.option("--exclude_re", default=".*_CERN.*", show_default=True,
              help="Regular expression to select the sites to exclude of the plot")
@click.option("--generate_plots", default=False, is_flag=True,
              help="Additional to the csv, generate the plot(s)")
@click.option("--output_folder", default="./output", help="local output directory")
def main(start_date, end_date, output_folder, by="month", generate_plots=False, include_re=".*", exclude_re=".*"):
    """This script will generate a dataset with the number of unique users of CRAB

        either by month or by weekofyear.
    """
    cp_pdf = get_hs06CpuTImeHr(start_date, end_date, by, include_re=include_re, exclude_re=exclude_re)

    cp_pdf["HS06 kdays"] = cp_pdf["sum(HS06CpuTimeHr)"] / 24000.0
    os.makedirs(output_folder, exist_ok=True)
    filename = f"HS06CpuTimeHr_{by}_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
    cp_pdf.to_csv(os.path.join(output_folder, f"{filename}.csv"))
    if generate_plots:
        generate_plot(cp_pdf, by, output_folder, filename)


if __name__ == "__main__":
    main()
