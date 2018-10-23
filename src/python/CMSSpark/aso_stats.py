#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark script to parse ASO records on HDFS.
"""

# system modules
import os
import re
import sys
import gzip
import time
import json

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.functions import sum as agg_sum
from pyspark.sql.functions import countDistinct, count, col, lit, mean
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# CMSSpark modules
from CMSSpark.spark_utils import fts_tables
from CMSSpark.spark_utils import aso_tables, print_rows
from CMSSpark.spark_utils import spark_context, unpack_struct
from CMSSpark.utils import info
from CMSSpark.conf import OptionParser

def aso_date(date):
    "Convert given date into ASO date format"
    if  not date:
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    return '%s/%s/%s' % (year, month, day)

def aso_date_unix(date):
    "Convert ASO date into UNIX timestamp"
    return time.mktime(time.strptime(date, '%Y/%m/%d'))

def run(date, fout, yarn=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = SQLContext(ctx)

    # read ASO and FTS tables
    date = aso_date(date)
    tables = {}
    tables.update(fts_tables(sqlContext, date=date, verbose=verbose))
    fts_df = tables['fts_df'] # fts table
    print_rows(fts_df, 'fts_df', verbose)
    tables.update(aso_tables(sqlContext, verbose=verbose))
    aso_df = tables['aso_df'] # aso table
    print_rows(aso_df, 'aso_df', verbose)

    fts = fts_df.select(['job_metadata.issuer', 'job_id', 'src_url', 
                           't_final_transfer_state', 'tr_timestamp_start', 
                           'tr_timestamp_complete'])

    fts = fts.filter("issuer = 'ASO'")
    fts_udf = udf(lambda x: x.split("/")[-1], StringType())
    fts = fts.withColumn("filename", fts_udf(fts.src_url))
    fts = fts.withColumn("fts_duration", (fts.tr_timestamp_complete-fts.tr_timestamp_start)*1./1000)

    aso = aso_df.select(['tm_source_lfn', 'tm_fts_id', 'tm_jobid', 'tm_type' ,'tm_last_update', 
                         'tm_start_time', 'tm_transfer_state', 'tm_source', 'tm_destination',
                         'tm_transfer_retry_count', 'tm_publish']).withColumnRenamed('tm_fts_id','job_id')

    aso_udf = udf(lambda x: x.split("/")[-1], StringType())
    aso = aso.withColumn("filename", aso_udf(aso.tm_source_lfn))
    aso = aso.filter((col("tm_transfer_state") == 3) | (col("tm_transfer_state") == 2))
    aso = aso.filter((aso.tm_transfer_state.isNotNull()))
    aso = aso.filter((aso.job_id.isNotNull()))

    new_df = fts.join(aso, on=['filename','job_id'], how='left_outer')

    new_df = new_df.groupby(['job_id', 'tm_transfer_state', 'tm_publish', 'tm_transfer_retry_count']).agg(
                        count(lit(1)).alias('Num Of Records'),
                        mean(aso.tm_last_update-aso.tm_start_time).alias('Aso duration'),
                        mean(new_df.tr_timestamp_start*1./1000-new_df.tm_start_time).alias('Aso delay start'),
                        mean(new_df.tm_last_update-new_df.tr_timestamp_complete*1./1000).alias('Aso delay'),
                        mean(new_df.fts_duration).alias('fts duration'),
                        )

    # keep table around
    new_df.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        new_df.write.format("com.databricks.spark.csv")\
                    .option("header", "true").save(fout)

    ctx.stop()

@info
def main():
    "Main function"
    optmgr = OptionParser('aso_stats')
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    run(opts.date, opts.fout, opts.yarn, opts.verbose)

if __name__ == '__main__':
    main()
