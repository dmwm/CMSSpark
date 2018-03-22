#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
import os
import re
import sys
import time
import json

# pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, sum, count, col, split
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# CMSSpark modules
from CMSSpark.spark_utils import phedex_summary_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import info
from CMSSpark.conf import OptionParser

def dateStamp(date):
    "Convert YYYYMMDD into sec since epoch"
    sec = time.mktime(time.strptime(date, '%Y%m%d'))
    return sec

def days_present(min_d, max_d, min_rd, max_rd):
    "Find total number of days"
    print("### days: %s %s %s %s" % (min_d, max_d, min_rd, max_rd))
    if min_d and min_rd and max_d and max_rd:
        minv = min(int(min_d), int(min_rd))
        maxv = max(int(max_d), int(max_rd))
        secs = dateStamp(str(maxv)) - dateStamp(str(minv))
        secd = 60*60*24
        return int(round(secs/secd))
    return -1

def run(fout, yarn=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    # read Phedex tables
    tables = {}
    hdir = 'hdfs:///cms/users/vk/phedex'
    tables.update(phedex_summary_tables(sqlContext, hdir=hdir, verbose=verbose))
    phedex_summary_df = tables['phedex_summary_df']
    phedex_summary_df.persist(StorageLevel.MEMORY_AND_DISK)
    print("### schema", phedex_summary_df.printSchema())
    phedex_summary_df.show(5)

    # register user defined function
    days = udf(days_present, IntegerType())

    # aggregate phedex info into dataframe
#    pdf = phedex_summary_df\
#            .groupBy(['site', 'dataset'])\
#            .agg(
#                    F.min(col('date')).alias('min_date'),
#                    F.max(col('date')).alias('max_date'),
#                    F.min(col('replica_date')).alias('min_rdate'),
#                    F.max(col('replica_date')).alias('max_rdate'),
#                    F.max(col('size')).alias('max_size'),
#                    F.min(col('size')).alias('min_size')
#                )\
#            .withColumn('days', days(col('min_date'), col('max_date'), col('min_rdate'), col('max_rdate')))
    pdf = phedex_summary_df\
            .groupBy(['site', 'dataset', 'size'])\
            .agg(
                    F.min(col('date')).alias('min_date'),
                    F.max(col('date')).alias('max_date'),
                    F.min(col('replica_date')).alias('min_rdate'),
                    F.max(col('replica_date')).alias('max_rdate')
                )\
            .withColumn('days', days(col('min_date'), col('max_date'), col('min_rdate'), col('max_rdate')))
    pdf.persist(StorageLevel.MEMORY_AND_DISK)
    pdf.show(5)
    print_rows(pdf, 'pdf', verbose=1)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        pdf.write.format("com.databricks.spark.csv")\
            .option("header", "true").save(fout)

    ctx.stop()

@info
def main():
    "Main function"
    optmgr  = OptionParser('phedex_agg')
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    run(opts.fout, opts.yarn, opts.verbose)

if __name__ == '__main__':
    main()
