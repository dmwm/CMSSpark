#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : myspark.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Example file to run basic spark job via pyspark

This code is based on example provided at
https://github.com/apache/spark/blob/master/examples/src/main/python/avro_inputformat.py

PySpark APIs:
https://spark.apache.org/docs/0.9.0/api/pyspark/index.html
"""

# system modules
import os
import sys
import imp
import pwd
import time
import json
import urllib
import urllib2
import httplib
import argparse

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, BooleanType, LongType
from pyspark.sql.functions import udf, from_unixtime, date_format, regexp_extract, when, lit, lag, lead, coalesce, sum, rowNumber

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms/CMS_DBS3_PROD_GLOBAL/test'
        dpath = '%s/datasets' % hdir
        msg = "Input datasets location on HDFS, default %s" % dpath
        self.parser.add_argument("--dpath", action="store",
            dest="dpath", default=dpath, help=msg)
        bpath = '%s/blocks' % hdir
        msg = "Input blocks location on HDFS, default %s" % bpath
        self.parser.add_argument("--bpath", action="store",
            dest="bpath", default=bpath, help=msg)
        fpath = '%s/files' % hdir
        msg = "Input files location on HDFS, default %s" % fpath
        self.parser.add_argument("--fpath", action="store",
            dest="fpath", default=fpath, help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def x509():
    "Helper function to get x509 either from env or tmp file"
    proxy = os.environ.get('X509_USER_PROXY', '')
    if  not proxy:
        proxy = '/tmp/x509up_u%s' % pwd.getpwuid( os.getuid() ).pw_uid
        if  not os.path.isfile(proxy):
            return ''
    return proxy

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided
    key/ca information
    """
    def __init__(self, key=None, cert=None, level=0):
        if  level > 1:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

def postdata(url, data, ckey=None, cert=None, verbose=0):
    """
    POST data into given url
    """
    headers = {'Content-type':'application/json','Accept':'application/json'}
    req = urllib2.Request(url)
    for key, val in headers.iteritems():
        req.add_header(key, val)
    if  verbose > 1:
        handler = urllib2.HTTPHandler(debuglevel=1)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    if  ckey and cert:
        handler = HTTPSClientAuthHandler(ckey, cert, verbose)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    data = urllib2.urlopen(req, json.dumps(data))

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

def schema_datasets():
    """
    ==> /data/wma/dbs/hdfs/large/datasets.attrs <==
    dataset_id,dataset,is_dataset_valid,primary_ds_id,processed_ds_id,data_tier_id,datset_access_type_id,acquitiion_era_id,processing_era_id,physics_group_id,xtcrosssection,prep_id,createion_date,create_by,last_modification_date,last_modified_by

    ==> /data/wma/dbs/hdfs/large/datasets.csv <==
    48,/znn4j_1600ptz3200-alpgen/CMSSW_1_4_9-CSA07-4157/GEN-SIM,1,15537,17760,109,81,202,1,37,null,null,1206050276,/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=aresh/CN=669724/CN=Aresh Vedaee,1261148491,/DC=org/DC=doegrids/OU=People/CN=Si Xie 523253

 DATASET_ID NOT NULL NUMBER(38)
 DATASET NOT NULL VARCHAR2(700)
 IS_DATASET_VALID NOT NULL NUMBER(38)
 PRIMARY_DS_ID NOT NULL NUMBER(38)
 PROCESSED_DS_ID NOT NULL NUMBER(38)
 DATA_TIER_ID NOT NULL NUMBER(38)
 DATASET_ACCESS_TYPE_ID NOT NULL NUMBER(38)
 ACQUISITION_ERA_ID NUMBER(38)
 PROCESSING_ERA_ID NUMBER(38)
 PHYSICS_GROUP_ID NUMBER(38)
 XTCROSSSECTION FLOAT(126)
 PREP_ID VARCHAR2(256)
 CREATION_DATE NUMBER(38)
 CREATE_BY VARCHAR2(500)
 LAST_MODIFICATION_DATE NUMBER(38)
 LAST_MODIFIED_BY VARCHAR2(500)
    """
    return StructType([
            StructField("d_dataset_id", IntegerType(), True),
            StructField("d_dataset", StringType(), True),
            StructField("d_is_dataset_valid", IntegerType(), True),
            StructField("d_primary_ds_id", IntegerType(), True),
            StructField("d_processed_ds_id", IntegerType(), True),
            StructField("d_data_tier_id", IntegerType(), True),
            StructField("d_dataset_access_type_id", IntegerType(), True),
            StructField("d_acquisition_era_id", IntegerType(), True),
            StructField("d_processing_era_id", IntegerType(), True),
            StructField("d_physics_group_id", IntegerType(), True),
            StructField("d_xtcrosssection", DoubleType(), True),
            StructField("d_prep_id", StringType(), True),
            StructField("d_creation_date", DoubleType(), True),
            StructField("d_create_by", StringType(), True),
            StructField("d_last_modification_date", DoubleType(), True),
            StructField("d_last_modified_by", StringType(), True)
        ])

def schema_blocks():
    """
    ==> /data/wma/dbs/hdfs/large/blocks.attrs <==
    block_id,block_name,dataset_id,open_for_writing,origin_site_name,block_size,file_count,creation_date,create_by,last_modification_date,last_modified_by

    ==> /data/wma/dbs/hdfs/large/blocks.csv <==
    555044,/Cosmics/Commissioning09-v1/RAW#72404277-dfe7-4405-9623-f240b21b60bc,13392,0,UNKNOWN,103414137568,30,1236228037,/DC=ch/DC=cern/OU=computers/CN=vocms39.cern.ch,1239909571,/DC=ch/DC=cern/OU=computers/CN=vocms39.cern.ch

 BLOCK_ID NOT NULL NUMBER(38)
 BLOCK_NAME NOT NULL VARCHAR2(500)
 DATASET_ID NOT NULL NUMBER(38)
 OPEN_FOR_WRITING NOT NULL NUMBER(38)
 ORIGIN_SITE_NAME NOT NULL VARCHAR2(100)
 BLOCK_SIZE NUMBER(38)
 FILE_COUNT NUMBER(38)
 CREATION_DATE NUMBER(38)
 CREATE_BY VARCHAR2(500)
 LAST_MODIFICATION_DATE NUMBER(38)
 LAST_MODIFIED_BY VARCHAR2(500)
    """
    return StructType([
            StructField("b_block_id", IntegerType(), True),
            StructField("b_block_name", StringType(), True),
            StructField("b_dataset_id", IntegerType(), True),
            StructField("b_open_for_writing", IntegerType(), True),
            StructField("b_origin_site_name", StringType(), True),
            StructField("b_block_size", DoubleType(), True),
            StructField("b_file_count", IntegerType(), True),
            StructField("b_creation_date", DoubleType(), True),
            StructField("b_create_by", StringType(), True),
            StructField("b_last_modification_date", DoubleType(), True),
            StructField("b_last_modified_by", StringType(), True)
        ])               
                         
def schema_files():
    """
    ==> /data/wma/dbs/hdfs/large/files.attrs <==
    file_id,logical_file_name,is_file_valid,dataset_id,block_id,file_type_id,check_sum,event_count,file_size,branch_hash_id,adler32,md5,auto_cross_section,creation_date,create_by,last_modification_date,last_modified_by

    ==> /data/wma/dbs/hdfs/large/files.csv <==
    11167853,/store/data/Commissioning08/Cosmics/RECO/CruzetAll_HLT_L1Basic-v1/000/058/546/E2813760-1C0D-DE11-AA92-001617DBD5AC.root,1,13615,574289,1,1934797535,24043,2886176192,null,NOTSET,NOTSET,null,1236656156,/DC=ch/DC=cern/OU=computers/CN=vocms39.cern.ch,1239909559,/DC=ch/DC=cern/OU=computers/CN=vocms39.cern.ch

 FILE_ID NOT NULL NUMBER(38)
 LOGICAL_FILE_NAME NOT NULL VARCHAR2(500)
 IS_FILE_VALID NOT NULL NUMBER(38)
 DATASET_ID NOT NULL NUMBER(38)
 BLOCK_ID NOT NULL NUMBER(38)
 FILE_TYPE_ID NOT NULL NUMBER(38)
 CHECK_SUM NOT NULL VARCHAR2(100)
 EVENT_COUNT NOT NULL NUMBER(38)
 FILE_SIZE NOT NULL NUMBER(38)
 BRANCH_HASH_ID NUMBER(38)
 ADLER32 VARCHAR2(100)
 MD5 VARCHAR2(100)
 AUTO_CROSS_SECTION FLOAT(126)
 CREATION_DATE NUMBER(38)
 CREATE_BY VARCHAR2(500)
 LAST_MODIFICATION_DATE NUMBER(38)
 LAST_MODIFIED_BY VARCHAR2(500)
    """
    return StructType([
            StructField("f_file_id", IntegerType(), True),
            StructField("f_logical_file_name", StringType(), True),
            StructField("f_is_file_valid", IntegerType(), True),
            StructField("f_dataset_id", IntegerType(), True),
            StructField("f_block_id", IntegerType(), True),
            StructField("f_file_type_id", IntegerType(), True),
            StructField("f_check_sum", StringType(), True),
            StructField("f_event_count", IntegerType(), True),
            StructField("f_file_size", DoubleType(), True),
            StructField("f_branch_hash_id", IntegerType(), True),
            StructField("f_adler32", StringType(), True),
            StructField("f_md5", StringType(), True),
            StructField("f_auto_cross_section", DoubleType(), True),
            StructField("f_creation_date", DoubleType(), True),
            StructField("f_create_by", StringType(), True),
            StructField("f_last_modification_date", DoubleType(), True),
            StructField("f_last_modified_by", StringType(), True)
        ])

def run(dpath, bpath, fpath, verbose=None, yarn=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    if  verbose:
        print("### datasets", dpath)
        print("### blocks", bpath)
        print("### files", fpath)
    # output
    out = []

    time0 = time.time()
    # pyspark modules
    from pyspark import SparkContext

    # define spark context, it's main object which allow
    # to communicate with spark
    ctx = SparkContext(appName="dbs")
    logger = SparkLogger(ctx)
    if  not verbose:
        logger.set_level('ERROR')
    if yarn:
        logger.info("YARN client mode enabled")

    sqlContext = HiveContext(ctx)

    ddf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(dpath, schema = schema_datasets())
    bdf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(bpath, schema = schema_blocks())
    fdf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(fpath, schema = schema_files())

    # Register temporary tables to be able to use sqlContext.sql
    ddf.registerTempTable('ddf')
    bdf.registerTempTable('bdf')
    fdf.registerTempTable('fdf')

    # join tables, final dataframe (ndf) is a joint table from datasets-blocks-files tables
    jtype = 'inner' # 'left_outer' # 'inner'
#    dbdf = ddf.join(bdf, ddf.dataset_id == bdf.dataset_id, how=jtype)
#    ndf = dbdf.join(dbdf, [dbdf.block_id == fdf.block_id, dbdf.dataset_id == fdf.dataset_id], how=jtype)
    ndf = fdf.join(ddf, fdf.f_dataset_id == ddf.d_dataset_id, how=jtype).select('d_dataset', 'd_last_modification_date', 'f_event_count').distinct().where('d_dataset like "%/RAW"')
    print('ndf size', ndf.count())
    for row in ndf.head(5):
        print(row)
    tot = ndf.groupBy('d_dataset').sum('f_event_count').collect()
    print("total size:", tot)

    ctx.stop()
    if  verbose:
        logger.info("Elapsed time %s" % htime(time.time()-time0))
    return out

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    time0 = time.time()
    results = run(opts.dpath, opts.bpath, opts.fpath, opts.verbose, opts.yarn)

if __name__ == '__main__':
    main()
