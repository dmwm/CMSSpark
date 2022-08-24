#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position,C0330

"""
File        : dbs_hdfs_eos.py
Author      : Christian Ariza <christian.ariza AT gmail [DOT] com>
Description : Define functions to aggregate EOS dataset and generate reports from it.
"""

# system modules
import click
import os

import matplotlib

matplotlib.use("Agg")  # this should be called before use seaborn

# from matplotlib.backends.backend_pdf import PdfPages
from pyspark.sql.functions import regexp_extract
import seaborn as sns

# CMSSpark modules
from CMSSpark.spark_utils import eos_tables, dbs_tables, get_spark_session

# global variables
DEFAULT_PARQUET_LOCATION = "hdfs:///cms/eos/full.parquet"


def generate_parquet(date, hdir="hdfs:///project/monitoring/archive/eos-report/logs/cms",
                     parquet_location=DEFAULT_PARQUET_LOCATION, spark=None, mode="append", verbose=False):
    """Creates or append to the given parquet file.
    
    Args:
        date: date string in format yyyy/MM/dd or a date glob expresion,
                 e.g. '2019/[0-1][0-9]/[0-3][0-9]', '2019/02/18'
        hdir: raw eos dataset location in hdfs
        parquet_location: location of the parquet dataset in hdfs
        spark: the spark session object.
        mode: write mode, it could be 'append' to add records to the existing file (in new partitions),
              'overwrite' to replace the current parquet file
               or 'ErrorIfExists' to fail it the parquet file already exists.
        verbose: True if you want to see aditional output, False otherwise.
                       The verbose mode increases the execution time.
                       
    Notes: hdir="hdfs:///project/monitoring/archive/eos/logs/reports/cms", before 2020
    """
    if spark is None:
        spark = get_spark_session(app_name="cms-eos-dataset")
    # spark.conf.set('spark.sql.session.timeZone', 'UTC')
    tables = eos_tables(spark, date=date, verbose=verbose, hdir=hdir)
    df = tables["eos_df"]
    # Repartition by day could reduce the number of files written and improve query time,
    # but will make this process slower
    write_df = df.write.partitionBy("day").mode(mode)
    if mode == "overwrite":
        # This will make that the overwrite affect only specific partitions
        # otherwise it will delete the existing dataset and create a new one.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        write_df = write_df.option("partitionOverwriteMode", "dynamic")

    write_df.parquet(parquet_location)


def generate_dataset_totals_pandasdf(period=("20200101", "20200131"), is_cms_user=False,
                                     parquet_location=DEFAULT_PARQUET_LOCATION, spark=None, verbose=False):
    """Query the parquet dataset for a given period grouping by dataset, and aplication.
       
       This will omit files that doesn't match to dbs files  (e.g. files without dataset)
       For a full report you should use generate_dataset_file_days
    """
    if spark is None:
        spark = get_spark_session(app_name="cms-eos-dataset")

    eos_df = spark.read \
        .option("basePath", parquet_location) \
        .parquet(parquet_location) \
        .filter("day between {} AND {}".format(*period))

    eos_df = eos_df.groupby(
        "session", "file_lfn", "application", "user", "user_dn"
    ).agg(
        {
            "rt": "sum",
            "rb": "sum",
            "wb": "sum",
            "wt": "sum",
            "rb_max": "max",
            "timestamp": "max",
        }
    )
    eos_df = eos_df.selectExpr(
        *[
            x
            if "(" not in x
            else "`{}` as {}".format(
                x, x.replace("max(", "").replace("sum(", "").replace(")", "")
            )
            for x in eos_df.columns
        ]
    )
    eos_df.registerTempTable("eos_df")
    tables = dbs_tables(spark, tables=["ddf", "fdf"])
    if verbose:
        print(tables)
    grouped = spark.sql(
        """
    select d_dataset,
           application,
           count(distinct(session)) as nevents,
           sum(rb)/(1024*1024) as total_rb,
           sum(wb)/(1024*1024) as total_wb,
           sum(rt)/1000 as total_rt,
           sum(wt)/1000 as total_wt
    from eos_df join fdf on file_lfn = concat('/eos/cms',f_logical_file_name)
                        join ddf on d_dataset_id = f_dataset_id
    where user {} like 'cms%' -- THIS IS EQUIVALENT TO is_cms_user  IN THE OLD QUERY
    group by d_dataset, application
    """.format(
            "" if is_cms_user else "NOT"
        )
    )
    grouped = grouped.na.fill("/Unknown", "d_dataset")
    grouped = grouped.withColumn(
        "data_tier", regexp_extract("d_dataset", ".*/([^/]*)$", 1)
    )
    _datasets_totals = grouped.toPandas()
    return _datasets_totals


def generate_dataset_file_days(period=("20190101", "20190131"), app_filter=None,
                               parquet_location=DEFAULT_PARQUET_LOCATION, spark=None, verbose=False, ):
    """Generate a pandas dataset 
    
    with dataset, filename, day, application, avg_size, total_rb (read bytes), total_wb (written bytes,
    total_rt (read time), and  total_wt (write time)
    """
    if spark is None:
        spark = get_spark_session(app_name="cms-eos-dataset")
    df = spark.read.parquet(parquet_location).filter(
        "day between {} AND {}".format(*period)
    )
    if app_filter is not None:
        df = df.filter(df.application.like(app_filter))
    df.registerTempTable("eos_df")
    tables = dbs_tables(spark, tables=["ddf", "fdf"])
    if verbose:
        print(tables)
    grouped = spark.sql(
        """
        select d_dataset,
           file_lfn,
           day,
           application,
           count(distinct(session)) as nevents,
           mean(csize) as avg_size,
           sum(rb) as total_rb,
           sum(wb) as total_wb,
           sum(rt) as total_rt,
           sum(wt) as total_wt
        from eos_df left join fdf on file_lfn = concat('/eos/cms',f_logical_file_name)
                            left join ddf on d_dataset_id = f_dataset_id
        group by d_dataset, file_lfn, day, application
        """
    )
    grouped = grouped.withColumn(
        "data_tier", regexp_extract("d_dataset", ".*/([^/]*)$", 1)
    )
    return grouped.toPandas()


@click.group()
@click.option("--verbose", default=False, is_flag=True)
@click.option("--parquet_location", default=DEFAULT_PARQUET_LOCATION, envvar="PARQUET_LOCATION")
@click.pass_context
def cli(ctx, verbose, parquetlocation):
    """Main Click command"""
    ctx.obj["VERBOSE"] = verbose
    ctx.obj["SPARK"] = get_spark_session(app_name="cms-eos-dataset")
    ctx.obj["PARQUET_LOCATION"] = parquetlocation


@cli.command()
@click.option("--mode", default="append", type=click.Choice(["append", "overwrite", "fail"]),
              help="write mode for the index")
@click.argument("date")
@click.pass_context
def run_update(ctx, date, mode):
    """Click function to update the eos dataset."""
    generate_parquet(date, mode=mode, spark=ctx.obj["SPARK"], parquet_location=ctx.obj["PARQUET_LOCATION"])


@cli.command()
@click.option("--outputDir", default=".", help="local output directory")
@click.option("--only_csv", default=False, is_flag=True, help="only output the csv (no the png with the graph)")
@click.argument("period", nargs=2, type=str)
@click.pass_context
def run_report_totals(ctx, period, outputdir, only_csv):
    """Click funtion to create the totals report"""
    _datasets_totals = generate_dataset_totals_pandasdf(
        period=period,
        verbose=ctx.obj["VERBOSE"],
        spark=ctx.obj["SPARK"],
        parquet_location=ctx.obj["PARQUET_LOCATION"],
    )

    _datasets_totals.to_csv(os.path.join(outputdir, "dataset_totals.csv"))
    if not only_csv:
        _format = "png"
        _dims = (14, 8.5)
        fig, ax = matplotlib.pyplot.subplots(figsize=_dims)
        top_dataset_by_rb = _datasets_totals.groupby("d_dataset") \
            .agg({"total_rb": "sum"}) \
            .reset_index() \
            .sort_values("total_rb", ascending=False) \
            .head(10)

        palette = sns.color_palette("Blues", n_colors=10)
        palette.reverse()
        sns.set_palette(palette)
        # sns.palplot(sns.color_palette('Blues', n_colors=10))
        sns.barplot(y="d_dataset", x="total_rb", data=top_dataset_by_rb)
        fig.savefig(
            os.path.join(
                outputdir, "top_total_rb_{}-{}.{}".format(period[0], period[1], _format)
            ),
            format=_format,
            bbox_inches="tight",
        )


@cli.command()
@click.option("--outputDir", default=".", help="local output directory")
@click.option("--appfilter", default=None, help="a like expression to filter the filename e.g. %rm")
@click.argument("period", nargs=2, type=str)
@click.pass_context
def get_filenames_per_day(ctx, period, outputdir, appfilter):
    """Generate a csv file with filenames/day/app
    
    This is a costly operation, It should be modified either to save to hdfs (or to a database directly)
    or to only be used with filters (small time periods or with an specified app).
    """
    _datasets_filenames = generate_dataset_file_days(
        period=period,
        app_filter=appfilter,
        verbose=ctx.obj["VERBOSE"],
        spark=ctx.obj["SPARK"],
        parquet_location=ctx.obj["PARQUET_LOCATION"],
    )
    os.makedirs(outputdir, exist_ok=True)
    _datasets_filenames.to_csv(
        os.path.join(
            outputdir,
            "filenames_{start}_{end}.csv.gz".format(start=period[0], end=period[1]),
        ),
        compression="gzip",
    )


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    cli(obj={})
