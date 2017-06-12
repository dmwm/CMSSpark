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
import argparse
from types import NoneType

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import sum as agg_sum

# CMSSpark modules
from CMSSpark.spark_utils import fts_tables, print_rows
from CMSSpark.spark_utils import spark_context, unpack_struct
from CMSSpark.utils import elapsed_time

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process ASO metadata"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = ''
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
    sqlContext = HiveContext(ctx)

    # read ASO tables
    date = aso_date(date)
    tables = {}
    tables.update(asodb_tables(sqlContext, date=date, verbose=verbose))
    aso_df = tables['aso_df'] # fts table

    # we can use aso_df directly for groupby/aggregated tasks
    fjoin = aso_df.groupBy(['tm_destination','tm_source'])\
            .agg(agg_sum(aso_df.tm_filesize).alias("sum_f_size"))

    # keep table around
    fjoin.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        fjoin.write.format("com.databricks.spark.csv")\
                .option("header", "true").save(fout)

    ctx.stop()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    run(opts.date, opts.fout, opts.yarn, opts.verbose)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()


