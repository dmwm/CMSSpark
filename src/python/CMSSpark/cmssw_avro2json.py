#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : cmssw_avro2json.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: convert avro file to json on HDFS. Here is usage example
run_spark cmssw_avro2json.py --yarn --date "20200102" --verbose --fout /cms/tmp/cmssw/
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
    if fin:
        pdf = cmssw_tables(ctx, sqlContext, hdir=fin, date=date)
    else:
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
