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
import gzip
import time
import json
import argparse
from types import NoneType

from pyspark import SparkContext, StorageLevel
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, BooleanType, LongType

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms/CMS_DBS3_PROD_GLOBAL/test'
        msg = 'Location of DBS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'dbs_datasets.csv'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--tier", action="store",
            dest="tier", default="", help='Select datasets for given data-tier, use comma-separated list if you want to handle multiple data-tiers')
        self.parser.add_argument("--era", action="store",
            dest="era", default="", help='Select datasets for given acquisition era')
        self.parser.add_argument("--patterns", action="store",
            dest="patterns", default="", help='Select datasets patterns')
        self.parser.add_argument("--antipatterns", action="store",
            dest="antipatterns", default="", help='Select datasets antipatterns')
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def apath(hdir, name):
    "Helper function to construct attribute path"
    return os.path.join(hdir, name)

class GzipFile(gzip.GzipFile):
    def __enter__(self):
        "Context manager enter method"
        if self.fileobj is None:
            raise ValueError("I/O operation on closed GzipFile object")
        return self

    def __exit__(self, *args):
        "Context manager exit method"
        self.close()

def fopen(fin, mode='r'):
    "Return file descriptor for given file"
    if  fin.endswith('.gz'):
        stream = gzip.open(fin, mode)
        # if we use old python we switch to custom gzip class to support
        # context manager and with statements
        if  not hasattr(stream, "__exit__"):
            stream = GzipFile(fin, mode)
    else:
        stream = open(fin, mode)
    return stream

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

def schema_processing_eras():
    """
    ==> /data/wma/dbs/hdfs/large/processing_eras.attrs <==
    processing_era_id,processing_version,creation_date,create_by,description

    ==> /data/wma/dbs/hdfs/large/processing_eras.csv <==
    1,0,null,null,null

 PROCESSING_ERA_ID NOT NULL NUMBER(38)
 PROCESSING_ERA_NAME NOT NULL VARCHAR2(120)
 CREATION_DATE NOT NULL INTEGER
 CREATE_BY NOT NULL VARCHAR2(500)
 DESCRIPTION NOT NULL VARCHAR2(40)
    """
    return StructType([
            StructField("processing_era_id", IntegerType(), True),
            StructField("processing_version", StringType(), True),
            StructField("creation_date", IntegerType(), True),
            StructField("create_by", StringType(), True),
            StructField("description", StringType(), True)
        ])

def schema_acquisition_eras():
    """
    ==> /data/wma/dbs/hdfs/large/acquisition_eras.attrs <==
    acquisition_era_id,acquisition_era_name,start_date,end_date,creation_date,create_by,description

    ==> /data/wma/dbs/hdfs/large/acquisition_eras.csv <==
    202,DBS2_UNKNOWN_ACQUISION_ERA,0,null,null,null,null

 ACQUISITION_ERA_ID NOT NULL NUMBER(38)
 ACQUISITION_ERA_NAME NOT NULL VARCHAR2(120)
 START_DATE NOT NULL INTEGER
 END_DATE NOT NULL INTEGER
 CREATION_DATE NOT NULL INTEGER
 CREATE_BY NOT NULL VARCHAR2(500)
 DESCRIPTION NOT NULL VARCHAR2(40)
    """
    return StructType([
            StructField("acquisition_era_id", IntegerType(), True),
            StructField("acquisition_era_name", StringType(), True),
            StructField("start_date", IntegerType(), True),
            StructField("end_date", IntegerType(), True),
            StructField("creation_date", IntegerType(), True),
            StructField("create_by", StringType(), True),
            StructField("description", StringType(), True)
        ])

def schema_dataset_access_types():
    """
    ==> /data/wma/dbs/hdfs/large/dataset_access_types.attrs <==
    dataset_access_type_id,dataset_access_type

    ==> /data/wma/dbs/hdfs/large/dataset_access_types.csv <==
    1,VALID

 DATASET_ACCESS_TYPE_ID NOT NULL NUMBER(38)
 DATASET_ACCESS_TYPE NOT NULL VARCHAR2(100)
    """
    return StructType([
            StructField("dataset_access_type_id", IntegerType(), True),
            StructField("dataset_access_type", StringType(), True)
        ])

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

def run(paths, fout, verbose=None, yarn=None, tier=None, era=None, patterns=[], antipatterns=[]):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    print("Use the following data on HDFS")
    for key, val in paths.items():
        print(val)
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

    daf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(paths['dapath'], schema = schema_dataset_access_types())
    ddf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(paths['dpath'], schema = schema_datasets())
    bdf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(paths['bpath'], schema = schema_blocks())
    fdf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(paths['fpath'], schema = schema_files())
    aef = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(paths['apath'], schema = schema_acquisition_eras())
    pef = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(paths['ppath'], schema = schema_processing_eras())

    # Register temporary tables to be able to use sqlContext.sql
    daf.registerTempTable('daf')
    ddf.registerTempTable('ddf')
    bdf.registerTempTable('bdf')
    fdf.registerTempTable('fdf')
    aef.registerTempTable('aef')
    pef.registerTempTable('pef')

    # join tables, final dataframe (ndf) is a joint table from datasets-blocks-files tables
#    dbdf = ddf.join(bdf, ddf.dataset_id == bdf.dataset_id, how=jtype)
#    ndf = dbdf.join(dbdf, [dbdf.block_id == fdf.block_id, dbdf.dataset_id == fdf.dataset_id], how=jtype)

    cols = ['d_dataset','d_creation_date','d_is_dataset_valid','f_event_count','f_file_size','dataset_access_type','acquisition_era_name']
    join1 = ddf.join(daf, ddf.d_dataset_access_type_id == daf.dataset_access_type_id)
    join2 = join1.join(fdf, join1.d_dataset_id == fdf.f_dataset_id)
    join3 = join2.join(aef, join2.d_acquisition_era_id == aef.acquisition_era_id, how='left_outer')
#    join4 = join3.select(cols).where('d_is_dataset_valid = 1')
    join4 = join3.select(cols).where('dataset_access_type = "VALID"')
    if  era:
        join5 = join4.where('acquisition_era_name like "%s"' % era.replace('*', '%%'))
    else:
        join5 = join4
    for name in patterns:
        join5 = join5.where('d_dataset LIKE "%%%s%%"' % name.replace('*', ''))
    for name in antipatterns:
        join5 = join5.where('d_dataset NOT LIKE "%%%s%%"' % name.replace('*', ''))
    fjoin = join5.distinct()
    fjoin.persist(StorageLevel.MEMORY_AND_DISK)
    if  verbose:
        for row in fjoin.head(5):
            print("### row", row)
    tiers = tier.split(',')
    if  tiers:
        for tier in tiers:
            tier_stats(tier, fjoin, fout)
    else:
        tier_stats(None, fjoin, fout)

    ctx.stop()
    if  verbose:
        logger.info("Elapsed time %s" % htime(time.time()-time0))
    return out

def tier_stats(tier, ndf, fout):
    "Collect tier stats and write them out"
    if  tier:
        xdf = ndf.where('d_dataset like "%%/%s"' % tier).distinct()
        fdir, fname = os.path.split(fout)
        fout = os.path.join(fdir, '%s_%s' % (tier, fname))
    else:
        xdf = ndf
    rdf = xdf.groupBy('d_dataset')\
            .agg({'f_event_count':'sum', 'f_file_size':'sum', 'd_creation_date':'max'})\
            .withColumnRenamed('sum(f_event_count)', 'evts')\
            .withColumnRenamed('sum(f_file_size)', 'size')\
            .withColumnRenamed('max(d_creation_date)', 'date')\
            .collect()
    tot_size = 0
    tot_evts = 0
    with fopen(fout, 'w') as ostream:
        ostream.write("dataset,size,evts,date\n")
        for row in rdf:
            size = row['size']
            evts = row['evts']
            if  isinstance(size, NoneType):
                size = 0
            if  isinstance(evts, NoneType):
                evts = 0
            ostream.write('%s,%s,%s,%s\n' % (row['d_dataset'], size, evts, row['date']))
            tot_size += float(size)
            tot_evts += int(evts)
    if  not tier:
        tier = 'ALL'
    print('%s,%s,%s' % (tier, tot_size, tot_evts))

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    paths = {'dpath':apath(opts.hdir, 'datasets'),
             'bpath':apath(opts.hdir, 'blocks'),
             'fpath':apath(opts.hdir, 'files'),
             'apath':apath(opts.hdir, 'acquisition_eras'),
             'ppath':apath(opts.hdir, 'processing_eras'),
             'dapath':apath(opts.hdir, 'dataset_access_types')}
    fout = opts.fout
    verbose = opts.verbose
    yarn = opts.yarn
    tier = opts.tier
    era = opts.era
    patterns = opts.patterns.split(',') if opts.patterns else []
    antipatterns = opts.antipatterns.split(',') if opts.antipatterns else []
    results = run(paths, fout, verbose, yarn, tier, era, patterns, antipatterns)

if __name__ == '__main__':
    main()
