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
import argparse
from types import NoneType

# pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, sum, count, col, split
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# CMSSpark modules
from CMSSpark.spark_utils import phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import elapsed_time

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process DBS+PhEDEx metadata"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'phedex.csv'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select CMSSW data for specific date (YYYYMMDD)')
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def dateStamp(date):
    "Convert YYYYMMDD into sec since epoch"
    sec = time.mktime(time.strptime(date, '%Y%m%d'))
    return sec

def unix2human(tstamp):
    "Convert unix time stamp into human readable format"
    return time.strftime('%Y%m%d', time.gmtime(tstamp))

def run(date, fout, yarn=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    # read Phedex tables
    tables = {}
    tables.update(phedex_tables(sqlContext, verbose=verbose))
    phedex_df = tables['phedex_df']

    # register user defined function
    unix2date = udf(unix2human, StringType())
    one_day = 60*60*24

    # aggregate phedex info into dataframe
    cols = ['node_name', 'dataset_name', 'block_bytes', 'replica_time_create']
    pdf = phedex_df.select(cols)\
            .groupBy(['node_name', 'dataset_name', 'replica_time_create'])\
            .agg({'block_bytes':'sum'})\
            .withColumn('date', lit(date))\
            .withColumn('replica_date', unix2date(col('replica_time_create')))\
            .withColumnRenamed('sum(block_bytes)', 'size')\
            .withColumnRenamed('dataset_name', 'dataset')\
            .withColumnRenamed('node_name', 'site')
    pdf.registerTempTable('pdf')
    pdf.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        out = '%s/phedex/%s' % (fout, date)
        cols = ['site','dataset','size','date','replica_date']
        pdf.select(cols)\
            .write.format("com.databricks.spark.csv")\
            .option("header", "true").save(out)

    ctx.stop()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    fout = opts.fout
    date = opts.date
    verbose = opts.verbose
    yarn = opts.yarn
    run(date, fout, yarn, verbose)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
