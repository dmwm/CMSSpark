#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: David Lange <david.lange AT cern [DOT] ch>

"""
Rucio daily dumps. More explanation required!
"""

from __future__ import print_function
import argparse
import os
import time
import logging
from datetime import timedelta, date, datetime
from dateutil.relativedelta import relativedelta

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import pyspark.sql.types as types

from CMSSpark import schemas

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
RUCIO_HDFS_FOLDER = "/project/awg/cms/rucio/{fdate}/replicas/part*.avro"
CMS_DBS_HDFS_FOLDER = "/project/awg/cms/CMS_DBS3_PROD_GLOBAL/old/FILES/part-m-00000"


def valid_date(str_date):
    """Is the string a valid date in the desired format?"""
    try:
        datetime.strptime(str_date, "%Y-%m-%d")
        return str_date
    except ValueError:
        msg = "Not a valid month: '{0}'.".format(str_date)
        raise argparse.ArgumentTypeError(msg)


class OptionParser:
    """
    Custom option parser.
    """

    def __init__(self):
        """User based option parser"""
        desc = """This script dumps Rucio daily data."""
        self.parser = argparse.ArgumentParser(prog="Rucio daily dumps", usage=desc)
        self.parser.add_argument(
            "--fdate",
            action="store",
            dest="fdate",
            default=None,
            help="""
            Date of the dbs dump file date. Eg. 2021-01-25.
            Default is current day.
            """,
            type=valid_date,
        )
        self.parser.add_argument(
            "--output_folder",
            action="store",
            dest="output_folder",
            default=None,
            help="HDFS path.",
        )
        self.parser.add_argument(
            "--verbose",
            action="store_true",
            help="Prints additional logging info",
            default=False,
        )


def run(rucio_path, dbs_path, output, verbose):
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


def main():
    """Main function"""
    verbose = False
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    if opts.verbose:
        logger.setLevel(logging.INFO)
        verbose = True
    if not opts.fdate:
        opts.fdate = date.today().strftime("%Y-%m-%d")
    if not opts.output_folder.endswith("/"):
        opts.output_folder = opts.output_folder + "/"
    rucio_path = RUCIO_HDFS_FOLDER.format(fdate=opts.fdate)
    output = opts.output_folder + "rucio/" + opts.fdate.replace("-", "/") + "/"
    logger.info("\n%s", opts)
    logger.info("Input rucio path: %s", rucio_path)
    logger.info("Input dbs path: %s", CMS_DBS_HDFS_FOLDER)
    logger.info("Output path: %s\n", output)
    run(rucio_path, CMS_DBS_HDFS_FOLDER, output, verbose)


if __name__ == "__main__":
    main()

