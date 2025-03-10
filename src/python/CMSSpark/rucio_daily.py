#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : rucio_daily.py
Author      : David Lange <david.lange AT cern [DOT] ch>
Description : Rucio daily dumps. More explanation required!
"""

# system modules
import logging
import time
from datetime import date

import click
import pyspark.sql.functions as fn
import pyspark.sql.types as types
from pyspark.sql import SparkSession

# CMSSpark modules
from CMSSpark import schemas

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

# global variables
RUCIO_HDFS_FOLDER = "/project/awg/cms/rucio/{fdate}/replicas/part*.avro"
CMS_DBS_HDFS_FOLDER = "/project/awg/cms/dbs/PROD_GLOBAL/{fdate}/FILES/*.gz"
_VALID_DATE_FORMATS = ["%Y-%m-%d"]


def run(rucio_path, dbs_path, output):
    start = time.time()
    spark = SparkSession.builder.appName("rucio_dumps_test").getOrCreate()
    csvreader = spark.read.format("csv") \
        .option("nullValue", "null") \
        .option("mode", "FAILFAST")
    avroreader = spark.read.format("avro")
    rucio_info = avroreader.load(rucio_path) \
        .withColumn("filename", fn.input_file_name())
    logger.debug("Rucio data types")
    logger.debug(rucio_info.dtypes)
    # rucio_info.show(5, False)
    dbs_files = csvreader.schema(schemas.schema_files()) \
        .load(dbs_path) \
        .select("f_logical_file_name", "f_dataset_id")
    # dbs_files.show(5, False)
    rucio_df = (rucio_info.withColumn("tmp1", fn.substring_index("filename", "/rucio/", -1))
                .withColumn("tally_date", fn.substring_index("tmp1", "/", 1))
                .withColumn('create_day', fn.date_format(fn.to_date((rucio_info.CREATED_AT / fn.lit(1000))
                                                                    .cast(types.LongType())
                                                                    .cast(types.TimestampType())),
                                                         'yyyyMMdd')
                            )
                .withColumn('tally_day', fn.date_format(fn.to_date("tally_date", "yyyy-MM-dd"), 'yyyyMMdd'))
                .select("RSE_ID", "BYTES", "NAME", "SCOPE", "tally_day", "create_day")
                )
    # rucio_df.show(5, False)
    rucio_df = rucio_df \
        .join(dbs_files, dbs_files.f_logical_file_name == rucio_df.NAME) \
        .groupBy("RSE_ID", "f_dataset_id", "SCOPE", "tally_day", "create_day") \
        .agg(fn.sum("BYTES").alias("rep_size"))
    # rucio_df.show(5, False)
    rucio_df.write.option("compression", "snappy").parquet(output, mode="overwrite")
    end = time.time()
    logger.info("Elapsed Time: {min} min, {sec} sec.".format(min=(end - start) // 60, sec=(end - start) % 60))


@click.command()
@click.option("--fdate", required=False, default=date.today().strftime("%Y-%m-%d"),
              type=click.DateTime(_VALID_DATE_FORMATS),
              help="YYYY-MM-DD date of the dbs dump file date. Default is current day.")
@click.option("--output", required=True, help="HDFS path: /cms/rucio_daily")
@click.option("--verbose", is_flag=True, default=False, required=False, help="Prints additional logging info")
def main(fdate, output, verbose):
    """Main function"""
    # click.DateTime returns date with time
    fdate = fdate.strftime("%Y-%m-%d")

    click.echo('rucio_daily')
    click.echo('This script dumps Rucio daily data')
    click.echo(f'Input Arguments: fdate:{fdate}, output:{output}, verbose:{verbose}')
    if verbose:
        logger.setLevel(logging.INFO)
    if not output.endswith("/"):
        output = output + "/"
    rucio_path = RUCIO_HDFS_FOLDER.format(fdate=fdate)
    dbs_path = CMS_DBS_HDFS_FOLDER.format(fdate=fdate)
    output = output + "rucio/" + fdate.replace("-", "/") + "/"
    logger.info("Input rucio path: %s", rucio_path)
    logger.info("Input dbs path: %s", dbs_path)
    logger.info("Output path: %s", output)
    run(rucio_path, dbs_path, output)


if __name__ == "__main__":
    main()
