#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position,C0330

"""
File        : dbs_hdfs_crab.py
Author      : Christian Ariza <christian.ariza AT gmail [DOT] com>
Description : Generate datasets and plots for CRAB data popularity based on condor data in hdfs.
"""

# system modules
import os

import click
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract,
    max as _max,
    min as _min,
    col,
    lit,
    count,
    sum as _sum,
    countDistinct,
)
from pyspark.sql.types import StructType, LongType, StringType, StructField


# CMSSpark modules
from CMSSpark.spark_utils import get_candidate_files

# global variables
_BASE_HDFS_CONDOR = "/project/monitoring/archive/condor/raw/metric"
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]


def get_spark_session():
    """Get or create the spark context and session."""
    sc = SparkContext(appName="cms-crab-dataset")
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def _get_crab_condor_schema():
    schema = StructType(
        [
            StructField(
                "metadata",
                StructType(
                    [
                        StructField("_id", StringType(), nullable=False),
                        StructField("timestamp", LongType(), nullable=False),
                    ]
                ),
            ),
            StructField(
                "data",
                StructType(
                    [
                        StructField(
                            "GlobalJobId", StringType(), nullable=False
                        ),
                        StructField("RecordTime", LongType(), nullable=False),
                        StructField("Status", StringType(), nullable=True),
                        StructField(
                            "ChirpCMSSWReadBytes", LongType(), nullable=True
                        ),
                        StructField(
                            "CRAB_DataBlock", StringType(), nullable=True
                        ),
                        StructField(
                            "CRAB_Workflow", StringType(), nullable=True
                        ),
                        StructField(
                            "CMSPrimaryPrimaryDataset",
                            StringType(),
                            nullable=True,
                        ),
                    ]
                ),
            ),
        ]
    )
    return schema


def get_crab_popularity_ds(start_date, end_date, base=_BASE_HDFS_CONDOR):
    """Query the hdfs data and returns a pandas dataframe

    with, Datatier, Dataset, CMSPrimaryPrimaryDataset, job_count, workflow_count, ChirpCMSSWReadBytes

    args:
        - start_date datetime Start of the query period (RecordTime)
        - end_date datetime End of the query period
    """
    start = int(start_date.timestamp() * 1000)
    end = int(end_date.timestamp() * 1000)
    spark = get_spark_session()

    dfs_crabdb = (
        spark.read.option("basePath", base)
        .json(get_candidate_files(start_date, end_date, spark, base=base, day_delta=1),
              schema=_get_crab_condor_schema()
              )
        .select("metadata.timestamp", "data.*")
        .filter(
            """Status in ('Completed', 'Removed') AND
                              CRAB_DataBlock is not NULL  AND
                              timestamp >= {} AND
                              timestamp <= {}""".format(
                start, end
            )
        )
        .repartition("CRAB_DataBlock")
        .drop_duplicates(["GlobalJobId"])
        .withColumnRenamed("CMSPrimaryPrimaryDataset", "PrimaryDataset")
        .withColumn("Dataset", regexp_extract("CRAB_DataBlock", "^(.*)/([^/]*)#.*$", 1))
        .withColumn("Datatier", regexp_extract("CRAB_DataBlock", "^(.*)/([^/]*)#.*$", 2))
    )
    dfs_crabdb = (
        dfs_crabdb.groupBy("Datatier", "PrimaryDataset", "Dataset")
        .agg(
            _max(col("RecordTime")),
            _min(col("RecordTime")),
            count(lit(1)),
            countDistinct("CRAB_Workflow"),
            _sum(col("ChirpCMSSWReadBytes")),
        )
        .withColumnRenamed("count(1)", "job_count")
        .withColumnRenamed("count(DISTINCT CRAB_Workflow)", "workflow_count")
        .withColumnRenamed("sum(ChirpCMSSWReadBytes)", "ChirpCMSSWReadBytes")
        .na.fill("Unknown", ["Datatier", "PrimaryDataset", "Dataset"])
    )
    return dfs_crabdb.toPandas()


def generate_top_datasets_plot(pdf, output_folder, filename):
    datablocks_pd = (
        pdf.groupby("Dataset")
        .agg(
            {
                "job_count": "sum",
                "max(RecordTime)": "max",
                "min(RecordTime)": "min",
            }
        )
        .sort_values("job_count", ascending=False)
        .reset_index()
    )
    _dims = (14, 8.5)
    fig, ax = plt.subplots(figsize=_dims)
    plot = sns.barplot(
        data=datablocks_pd[:20], x="job_count", y="Dataset"
    )
    fig.savefig(
        os.path.join(output_folder, f"{filename}_top_jc.png"),
        bbox_inches="tight",
    )


@click.command()
@click.argument("start_date", nargs=1, type=click.DateTime(_VALID_DATE_FORMATS))
@click.argument("end_date", nargs=1, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--generate_plots", default=False, is_flag=True, help="Additional to the csv, generate the plot(s)")
@click.option("--output_folder", default="./output", help="local output directory")
def main(start_date, end_date, output_folder, generate_plots=False):
    cp_pdf = get_crab_popularity_ds(start_date, end_date)
    os.makedirs(output_folder, exist_ok=True)
    filename = f"CRAB_popularity_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
    cp_pdf.to_csv(os.path.join(output_folder, f"{filename}.csv"))
    if generate_plots:
        generate_top_datasets_plot(cp_pdf, output_folder, filename)


if __name__ == "__main__":
    main()
