#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : dbs_cmssw.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Spark script to parse DBS and CMSSW monitoring content on HDFS
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

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows, spark_context, cmssw_tables
from CMSSpark.utils import elapsed_time, cern_monit

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'cmssw_datasets.csv'
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
        msg = "Send results via StompAMQ to a broker, provide broker credentials in JSON file"
        self.parser.add_argument("--amq", action="store",
            dest="amq", default="", help=msg)

def cmssw_date(date):
    "Convert given date into CMSSW date format"
    if  not date:
        return None
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = int(date[4:6])
    day = int(date[6:])
    return 'year=%s/month=%s/day=%s' % (year, month, day)

def run(fout, verbose=None, yarn=None, date=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    time0 = time.time()

    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sqlContext, verbose=verbose))
    ddf = tables['ddf'] # dataset table
    fdf = tables['fdf'] # file table

    # read CMSSW avro rdd
    cmssw_df = cmssw_tables(ctx, sqlContext, date=cmssw_date(date), verbose=verbose)

    # merge DBS and CMSSW data
    cols = ['d_dataset','d_dataset_id','f_logical_file_name','FILE_LFN','SITE_NAME']
    stmt = 'SELECT %s FROM ddf JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id JOIN cmssw_df ON fdf.f_logical_file_name = cmssw_df.FILE_LFN' % ','.join(cols)
    joins = sqlContext.sql(stmt)
    print_rows(joins, stmt, verbose)

    # perform aggregation
    fjoin = joins.groupBy(['SITE_NAME','d_dataset'])\
            .agg({'FILE_LFN':'count'})\
            .withColumnRenamed('count(FILE_LFN)', 'cmssw_count')\

    # keep table around
    fjoin.persist(StorageLevel.MEMORY_AND_DISK)

    if  not date:
        date = time.strftime("year=%Y/month=%-m/date=%d", time.gmtime(time.time()-60*60*24))

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        fjoin.write.format("com.databricks.spark.csv")\
                .option("header", "true").save(fout)

    ctx.stop()
    if  verbose:
        print("Elapsed time %s" % elapsed_time(time0))
    return fjoin

def tmp():
    # output results
    out = []
    idx = 0
    for row in fjoin.collect():
        rdict = row.asDict()
        out.append(rdict)
        if  verbose and idx < 5:
            print(rdict)
        idx += 1

    # write out output
    if  fout:
        with open(fout, 'w') as ostream:
            headers = sorted(out[0].keys())
            ostream.write(','.join(headers)+'\n')
            for rdict in out:
                arr = []
                for key in headers:
                    arr.append(str(rdict[key]))
                ostream.write(','.join(arr)+'\n')

    ctx.stop()
    if  verbose:
        print("Elapsed time %s" % elapsed_time(time0))
    return out

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
    res = run(fout, verbose, yarn, date)
    if  opts.amq:
        cern_monit(res)

    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
