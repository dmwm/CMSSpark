#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : dbs_jm.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Spark script to parse DBS and JobMonitoring monitoring content on HDFS
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
from pyspark.sql.functions import lit

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, jm_tables
from CMSSpark.utils import elapsed_time 

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'jm_datasets.csv'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select JobMonitoring data for specific date (YYYYMMDD)')
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def jm_date(date):
    "Convert given date into JobMonitoring date format"
    if  not date:
        date = time.strftime("year=%Y/month=%-m/date=%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = int(date[4:6])
    day = int(date[6:])
    return 'year=%s/month=%s/day=%s' % (year, month, day)

def jm_date_unix(date):
    "Convert JobMonitoring date into UNIX timestamp"
    return time.mktime(time.strptime(date, 'year=%Y/month=%m/day=%d'))

def run(date, fout, yarn=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    # read DBS and JobMonitoring tables
    tables = {}

    # read JobMonitoring avro rdd
    date = jm_date(date)
    jm_df = jm_tables(ctx, sqlContext, date=date, verbose=verbose)

    # DBS tables
    tables.update(dbs_tables(sqlContext, verbose=verbose))
    ddf = tables['ddf'] # dataset table
    fdf = tables['fdf'] # file table

    # merge DBS and JobMonitoring data
    cols = ['d_dataset','d_dataset_id','f_logical_file_name','FileName','FileType','Type','SiteName','WrapWC','WrapCPU','JobExecExitCode']
    stmt = 'SELECT %s FROM ddf JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id JOIN jm_df ON fdf.f_logical_file_name = jm_df.FileName' % ','.join(cols)
    joins = sqlContext.sql(stmt)
    print_rows(joins, stmt, verbose)

    # perform aggregation
    fjoin = joins.groupBy(['SiteName','JobExecExitCode','FileType','Type','d_dataset'])\
            .agg({'WrapWC':'sum','WrapCPU':'sum','JobExecExitCode':'count','FileType':'count','Type':'count'})\
            .withColumnRenamed('sum(WrapWC)', 'tot_wc')\
            .withColumnRenamed('sum(WrapCPU)', 'tot_cpu')\
            .withColumnRenamed('count(JobExecExitCode)', 'ecode_count')\
            .withColumnRenamed('count(FileType)', 'file_type_count')\
            .withColumnRenamed('count(Type)', 'type_count')\
            .withColumnRenamed('d_dataset', 'dataset')\
            .withColumn('date', lit(jm_date_unix(date)))\
            .withColumn('count_type', lit('jm'))\

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
    fout = opts.fout
    verbose = opts.verbose
    yarn = opts.yarn
    date = opts.date
    run(opts.date, opts.fout, opts.yarn, opts.verbose)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
