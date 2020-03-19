#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : convert_cmssw.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import re
import sys
import time
import json
import argparse

# pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark.spark_utils import cmssw_tables, spark_context


class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input file")
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select CMSSW data for specific date (YYYYMMDD)')
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytix cluster via yarn resource manager")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help="Output file")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

#[{u'USER_DN': u'/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=amaltaro/CN=718748/CN=Alan Malta Rodrigues/CN=1968084961/CN=1153133833/CN=2121994674/CN=1843002439', u'FILE_SIZE': u'0', u'READ_VECTOR_COUNT_SIGMA': u'10.4623', u'CLIENT_HOST': u'hammer-a123', u'READ_SINGLE_AVERAGE': u'74414.3', u'SERVER_DOMAIN': u'unknown', u'END_DATE': 1577919600000, u'START_TIME': u'1577921926', u'READ_BYTES_AT_CLOSE': u'2241128612', u'SERVER_HOST': u'unknown', u'READ_VECTOR_OPERATIONS': u'195', u'READ_VECTOR_AVERAGE': u'10838900', u'READ_SINGLE_OPERATIONS': u'1714', u'SITE_NAME': u'T2_US_Purdue', u'INSERT_DATE': 1577923511000, u'READ_VECTOR_COUNT_AVERAGE': u'9.46154', u'CLIENT_DOMAIN': u'unknown', u'READ_VECTOR_SIGMA': u'7081840', u'END_TIME': u'1577923200', u'START_DATE': 1577918326000, u'READ_VECTOR_BYTES': u'2113582443', u'READ_SINGLE_BYTES': u'127546169', u'APP_INFO': None, u'FILE_LFN': u'../cmsRun3/RAWSIMoutput.root', u'READ_BYTES': u'2241128612', u'READ_SINGLE_SIGMA': u'271279', u'FALLBACK': u'-', u'UNIQUE_ID': u'AFB0B1C9-30EA-8E4F-A4F3-88B6BCB5D84E-0'}]

def transform(rec):
    xdf = {}
    for key, val in rec.items():
        xdf[key.lower()] = val
    tstamp = long(time.time())*1000
    mid = "43b78d48-7509-e1cc-dc2c-a071d2cfdd7c"
    metadata = {"_id":"43b78d48-7509-e1cc-dc2c-a071d2cfdd7c",
        "hostname":"monit.cern.ch",
        "kafka_timestamp": tstamp,
        "partition":"1",
        "producer":"convert_avro2json",
        "timestamp": tstamp,
        "topic":"cmssw_pop_raw_metric",
        "type":"metric",
        "type_prefix":"raw",
        "version":"001"}
    return {"data":xdf, "metadata": metadata}

def convert(ctx, sqlContext, date, fin, fout, verbose):
    "Helper function to convert fin (avro) to fout (json)"
    pdf = cmssw_tables(ctx, sqlContext, date=date)
    xdf = pdf['cmssw_df']
    if verbose:
        records = xdf.take(1)
        print("original record", records)
    new_rdd = xdf.rdd.map(lambda row: row.asDict(True))
    new_rdd = new_rdd.map(lambda row: transform(row))
    if verbose:
        records = new_rdd.take(1)
        print("converted record", records)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        xdf = sqlContext.createDataFrame(new_rdd)
        xdf.write.format("json").save(fout)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', opts.yarn, opts.verbose)
    sqlContext = SQLContext(ctx)

    convert(ctx, sqlContext, opts.date, opts.fin, opts.fout, opts.verbose)

if __name__ == '__main__':
    main()
