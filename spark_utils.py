#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : spark_utils.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Set of spark utils
"""

# system modules
import os
import re
import sys
import time
import json
from datetime import datetime as dt
from types import NoneType
from subprocess import Popen, PIPE

# local modules
from schemas import schema_processing_eras, schema_dataset_access_types
from schemas import schema_acquisition_eras,  schema_datasets, schema_blocks
from schemas import schema_files, schema_mod_configs, schema_out_configs
from schemas import schema_rel_versions, schema_phedex

from pyspark import SparkContext, StorageLevel
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, BooleanType, LongType

class SparkLogger(object):
    "Control Spark Logger"
    def __init__(self, ctx):
        self.logger = ctx._jvm.org.apache.log4j
        self.rlogger = self.logger.LogManager.getRootLogger()

    def set_level(self, level):
        "Set Spark Logger level"
        self.rlogger.setLevel(getattr(self.logger.Level, level))

    def lprint(self, stream, msg):
        "Print message via Spark Logger to given stream"
        getattr(self.rlogger, stream)(msg)

    def info(self, msg):
        "Print message via Spark Logger to info stream"
        self.lprint('info', msg)

    def error(self, msg):
        "Print message via Spark Logger to error stream"
        self.lprint('error', msg)

    def warning(self, msg):
        "Print message via Spark Logger to warning stream"
        self.lprint('warning', msg)

def apath(hdir, name):
    "Helper function to construct attribute path"
    return os.path.join(hdir, name)

def files(path, verbose=0):
    "Return list of files for given HDFS path"
    hpath = "hadoop fs -ls %s | awk '{print $8}'" % path
    if  verbose:
        print("Lookup area: %s" % hpath)
    pipe = Popen(hpath, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    pipe.wait()
    fnames = [f for f in pipe.stdout.read().split('\n') if f.find('part') != -1]
    return fnames

def avro_files(path, verbose=0):
    "Return list of files for given HDFS path"
    hpath = "hadoop fs -ls %s | awk '{print $8}'" % path
    if  verbose:
        print("### Avro files area: %s" % hpath)
    pipe = Popen(hpath, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    pipe.wait()
    fnames = [f for f in pipe.stdout.read().split('\n') if f.endswith('avro')]
    return fnames

def unionAll(dfs):
    """
    Unions snapshots in one dataframe

    :param item: list of dataframes
    :returns: union of dataframes
    """
    return reduce(DataFrame.unionAll, dfs)

def file_list(basedir, fromdate=None, todate=None):
    """
    Finds snapshots in given directory by interval dates

    :param basedir: directory where snapshots are held
    :param fromdate: date from which snapshots are filtered
    :param todate: date until which snapshots are filtered
    :returns: array of filtered snapshots paths
    :raises ValueError: if unparsable date format
    """
    dirs = os.popen("hadoop fs -ls %s | sed '1d;s/  */ /g' | cut -d\  -f8" % basedir).read().splitlines()
    # if files are not in hdfs --> dirs = os.listdir(basedir)

    # by default we'll use yesterday date on HDFS to avoid clashes
    day = time.strftime("%Y-%m-%d", time.gmtime(time.time()-60*60*24))
    if  not fromdate:
        fromdate = day
    if  not todate:
        todate = day

    o_fromdate = fromdate
    o_todate = todate
    try:
        fromdate = dt.strptime(fromdate, "%Y-%m-%d")
        todate = dt.strptime(todate, "%Y-%m-%d")
    except ValueError as err:
        raise ValueError("Unparsable date parameters. Date should be specified in form: YYYY-mm-dd")		
 		
    pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

    dirdate_dic = {}
    from_match = 0
    to_match = 0
    for idir in dirs:
        if  idir.find(o_fromdate) != -1:
            from_match = 1
        if  idir.find(o_todate) != -1:
            to_match = 1
        matching = pattern.search(idir)
        if matching:
            dirdate_dic[idir] = dt.strptime(matching.group(1), "%Y-%m-%d")

    if  not from_match:
        raise Exception("Unable to find fromdate=%s are on HDFS %s" % (o_fromdate, basedir))
    if  not to_match:
        raise Exception("Unable to find todate=%s are on HDFS %s" % (o_todate, basedir))
    return [k for k, v in dirdate_dic.items() if v >= fromdate and v <= todate]		

def print_rows(df, dfname, verbose, head=5):
    if  verbose:
        print("First rows of %s" % dfname)
        for row in df.head(head):
            print("### row", row)

def spark_context(appname='cms', yarn=None, verbose=False):
    # define spark context, it's main object which allow
    # to communicate with spark
#    ctx = SparkSession.builder.appName(appname).enableHiveSupport().getOrCreate().sparkContext()
    ctx = SparkContext(appName=appname)
    logger = SparkLogger(ctx)
    if  not verbose:
        logger.set_level('ERROR')
    if yarn:
        logger.info("YARN client mode enabled")
    return ctx

def phedex_tables(sqlContext, hdir='hdfs:///project/awg/cms', verbose=False):
    """
    Return dictionary of spark dbs tables
    """
    phxdir = hdir+'/phedex/block-replicas-snapshots/csv/'

    # phedex data
    pfiles = file_list(phxdir)
    msg = "Phedex snapshot found %d directories" % len(pfiles)
    print(msg)
    phedex_df = unionAll([sqlContext.read.format('com.databricks.spark.csv')
                    .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                    .load(file_path, schema = schema_phedex()) \
                    for file_path in pfiles])

    # Register temporary tables to be able to use sqlContext.sql
    phedex_df.registerTempTable('phedex_df')

    tables = {'phedex_df':phedex_df}
    return tables

def dbs_tables(sqlContext, hdir='hdfs:///project/awg/cms', verbose=False):
    """
    Return dictionary of spark dbs tables
    """
    dbsdir = hdir+'/CMS_DBS3_PROD_GLOBAL/current'
    paths = {'dpath':apath(dbsdir, 'DATASETS'),
             'bpath':apath(dbsdir, 'BLOCKS'),
             'fpath':apath(dbsdir, 'FILES'),
             'apath':apath(dbsdir, 'ACQUISITION_ERAS'),
             'ppath':apath(dbsdir, 'PROCESSING_ERAS'),
             'mcpath':apath(dbsdir, 'DATASET_OUTPUT_MOD_CONFIGS'),
             'ocpath':apath(dbsdir, 'OUTPUT_MODULE_CONFIGS'),
             'rvpath':apath(dbsdir, 'RELEASE_VERSIONS'),
             'dapath':apath(dbsdir, 'DATASET_ACCESS_TYPES')}
    print("Use the following data on HDFS")
    for key, val in paths.items():
        print(val)

    # define DBS tables
    daf = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_dataset_access_types()) \
                        for path in files(paths['dapath'], verbose)])
    ddf = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_datasets()) \
                        for path in files(paths['dpath'], verbose)])
    bdf = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_blocks()) \
                        for path in files(paths['bpath'], verbose)])
    fdf = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_files()) \
                        for path in files(paths['fpath'], verbose)])
    aef = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_acquisition_eras()) \
                        for path in files(paths['apath'], verbose)])
    pef = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_processing_eras()) \
                        for path in files(paths['ppath'], verbose)])

    mcf = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_mod_configs()) \
                        for path in files(paths['mcpath'], verbose)])
    ocf = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_out_configs()) \
                        for path in files(paths['ocpath'], verbose)])
    rvf = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_rel_versions()) \
                        for path in files(paths['rvpath'], verbose)])

    # Register temporary tables to be able to use sqlContext.sql
    daf.registerTempTable('daf')
    ddf.registerTempTable('ddf')
    bdf.registerTempTable('bdf')
    fdf.registerTempTable('fdf')
    aef.registerTempTable('aef')
    pef.registerTempTable('pef')
    mcf.registerTempTable('mcf')
    ocf.registerTempTable('ocf')
    rvf.registerTempTable('rvf')

    print("### ddf from dbs_tables", ddf, type(ddf))

    tables = {'daf':daf, 'ddf':ddf, 'bdf':bdf, 'fdf':fdf, 'aef':aef, 'pef':pef, 'mcf':mcf, 'ocf':ocf, 'rvf':rvf}
    return tables

def cmssw_tables(ctx, sqlContext,
        schema_file='hdfs:///cms/wmarchive/avro/schemas/cmssw.avsc',
        hdir='hdfs:///project/awg/cms/cmssw-popularity/avro-snappy', day=None, verbose=None):

    if  not day:
        day = time.strftime("year=%Y/month=%-m/day=%d", time.gmtime(time.time()-60*60*24))
    path = '%s/%s' % (hdir, day)
    # get avro files from HDFS
    afiles = avro_files(path, verbose=verbose)
    print("### avro_files", afiles)

    # load FWJR schema
    rdd = ctx.textFile(schema_file, 1).collect()

    # define input avro schema, the rdd is a list of lines (sc.textFile similar to readlines)
    avsc = reduce(lambda x, y: x + y, rdd) # merge all entries from rdd list
    schema = ''.join(avsc.split()) # remove spaces in avsc map
    conf = {"avro.schema.input.key": schema}

    # define newAPIHadoopFile parameters, java classes
    aformat="org.apache.avro.mapreduce.AvroKeyInputFormat"
    akey="org.apache.avro.mapred.AvroKey"
    awrite="org.apache.hadoop.io.NullWritable"
    aconv="org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter"

    # load data from HDFS
    rdd = ctx.union([ctx.newAPIHadoopFile(f, aformat, akey, awrite, aconv, conf=conf) for f in afiles])
    # the CMSSW are stored as [(dict, None), (dict, None)], therefore we take first element
    # and assign them to new rdd
    avro_rdd = rdd.map(lambda x: x[0])
    records = avro_rdd.take(1) # take function will return list of records
    if  verbose:
        print("### cmssw avro records", records, type(records))

#    reckeys = records[0].keys() # we get keys of first record
#    Record = Row(*reckeys)
#    if  verbose:
#        print("### inferred Record", Record, type(Record))
#    data = avro_rdd.map(lambda r: Record(*r))

    # create new spark DataFrame
#    avro_df = sqlContext.createDataFrame(data)
#    avro_df = sqlContext.createDataFrame(records)
    avro_df = sqlContext.createDataFrame(avro_rdd)
    avro_df.registerTempTable('avro_df')
    if  verbose:
        print("### avro_df", avro_df, type(avro_df))

    return avro_df

def aaa_tables(sqlContext, hdir='hdfs:///project/monitoring/archive/xrootd/raw/gled', verbose=False):
    """
    Return dictionary of spark tables from AAA data
    """
    if  not day:
        # by default we read yesterday data
        day = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))
    hpath = '%s/%s' % (hdir, day)
    aaa_df = unionAll([sqlContext.jsonFile(path) for path in files(hpath, verbose)])
    aaa_df.registerTempTable('aaa_df')
    tables = {'aaa_df':aaa_df}
    return tables

def eos_tables(sqlContext, hdir='hdfs:///project/monitoring/archive/eos/logs/reports/cms', day=None, verbose=False):
    """
    Return dictionary of spark tables from EOS data
    """
    if  not day:
        # by default we read yesterday data
        day = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))
    hpath = '%s/%s' % (hdir, day)
    eos_df = unionAll([sqlContext.jsonFile(path) for path in files(hpath, verbose)])
    eos_df.registerTempTable('eos_df')
    tables = {'eos_df':eos_df}
    return tables

