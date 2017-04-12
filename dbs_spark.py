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
from subprocess import Popen, PIPE

from pyspark import SparkContext, StorageLevel
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, BooleanType, LongType

# WMCore modules
try:
    # stopmAMQ API
    from WMCore.Services.StompAMQ.StompAMQ import StompAMQ
except ImportError:
    StompAMQ = None

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms/CMS_DBS3_PROD_GLOBAL/current'
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
        self.parser.add_argument("--release", action="store",
            dest="release", default="", help='Select datasets for given CMSSW release')
        self.parser.add_argument("--cdate", action="store",
            dest="cdate", default="", help='Select datasets starting given creation date in YYYYMMDD format')
        self.parser.add_argument("--patterns", action="store",
            dest="patterns", default="", help='Select datasets patterns')
        self.parser.add_argument("--antipatterns", action="store",
            dest="antipatterns", default="", help='Select datasets antipatterns')
        msg = 'Perform action over DBS info on HDFS: tier_stats, dataset_stats'
        self.parser.add_argument("--action", action="store",
            dest="action", default="tier_stats", help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")
        msg = "Send results via StompAMQ to a broker, provide broker credentials in JSON file"
        self.parser.add_argument("--amq", action="store",
            dest="amq", default="", help=msg)

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
    dataset_id,dataset,is_dataset_valid,primary_ds_id,processed_ds_id,data_tier_id,datset_access_type_id,acqusition_era_id,processing_era_id,physics_group_id,xtcrosssection,prep_id,createion_date,create_by,last_modification_date,last_modified_by

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

def schema_mod_configs():
    return StructType([
            StructField("mc_ds_output_mod_config_id", IntegerType(), True),
            StructField("mc_dataset_id", IntegerType(), True),
            StructField("mc_output_mod_config_id", IntegerType(), True)
        ])

def schema_out_configs():
    return StructType([
            StructField("oc_output_mod_config_id", IntegerType(), True),
            StructField("oc_app_exec_id", IntegerType(), True),
            StructField("oc_release_version_id", IntegerType(), True),
            StructField("oc_parameter_set_hash_id", IntegerType(), True),
            StructField("oc_output_module_label", StringType(), True),
            StructField("oc_global_tag", StringType(), True),
            StructField("oc_scenario", StringType(), True),
            StructField("oc_creation_date", IntegerType(), True),
            StructField("oc_create_by", StringType(), True)
        ])

def schema_rel_versions():
    return StructType([
            StructField("r_release_version_id", IntegerType(), True),
            StructField("r_release_version", StringType(), True)
        ])

def files(path, verbose=0):
    "Return list of files for given HDFS path"
    hpath = "hadoop fs -ls %s | awk '{print $8}'" % path
    if  verbose:
        print("Lookup area: %s" % hpath)
    pipe = Popen(hpath, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    pipe.wait()
    fnames = [f for f in pipe.stdout.read().split('\n') if f.find('part') != -1]
    return fnames

def unionAll(dfs):
    """
    Unions snapshots in one dataframe

    :param item: list of dataframes
    :returns: union of dataframes
    """
    return reduce(DataFrame.unionAll, dfs)

def htime(seconds):
    "Convert given seconds into human readable form of N hour(s), N minute(s), N second(s)"
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    def htimeformat(msg, key, val):
        "Helper function to proper format given message/key/val"
        if  val:
            if  msg:
                msg += ', '
            msg += '%d %s' % (val, key)
            if  val > 1:
                msg += 's'
        return msg

    out = ''
    out = htimeformat(out, 'day', days)
    out = htimeformat(out, 'hour', hours)
    out = htimeformat(out, 'minute', minutes)
    out = htimeformat(out, 'second', secs)
    return out

def elapsed_time(time0):
    "Return elapsed time from given time stamp"
    return htime(time.time()-time0)

def unix_tstamp(date):
    "Convert given date into unix seconds since epoch"
    if  not isinstance(date, str):
        raise NotImplementedError('Given date %s is not in string YYYYMMDD format' % date)
    if  len(date) == 8:
        return int(time.mktime(time.strptime(date, '%Y%m%d')))
    elif len(date) == 10: # seconds since epoch
        return int(date)
    else:
        raise NotImplementedError('Given date %s is not in string YYYYMMDD format' % date)

def run(paths, fout, action,
        verbose=None, yarn=None, tier=None, era=None,
        release=None, cdate=None, patterns=[], antipatterns=[]):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    print("Use the following data on HDFS")
    for key, val in paths.items():
        print(val)

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

    # join tables
    cols = ['*'] # to select all fields from table
    cols = ['d_dataset_id', 'd_dataset','d_creation_date','d_is_dataset_valid','f_event_count','f_file_size','dataset_access_type','acquisition_era_name','processing_version']

    # join tables
    stmt = 'SELECT %s FROM ddf JOIN fdf on ddf.d_dataset_id = fdf.f_dataset_id JOIN daf ON ddf.d_dataset_access_type_id = daf.dataset_access_type_id JOIN aef ON ddf.d_acquisition_era_id = aef.acquisition_era_id JOIN pef ON ddf.d_processing_era_id = pef.processing_era_id' % ','.join(cols)
    print(stmt)
    joins = sqlContext.sql(stmt)

    # keep joins table around
    joins.persist(StorageLevel.MEMORY_AND_DISK)

    # print out rows
    if  verbose:
        print("### First rows of joint table, nrows", joins.count())
        for row in joins.head(5):
            print("### row", row)

    # construct conditions
    cond = 'dataset_access_type = "VALID" AND d_is_dataset_valid = 1'
    if  era:
        cond += ' AND acquisition_era_name like "%s"' % era.replace('*', '%')
    cond_pat = []
    for name in patterns:
        if  name.find('*') != -1:
            cond_pat.append('d_dataset LIKE "%s"' % name.replace('*', '%'))
        else:
            cond_pat.append('d_dataset="%s"' % name)
    if  cond_pat:
        cond += ' AND (%s)' % ' OR '.join(cond_pat)
    for name in antipatterns:
        if  name.find('*') != -1:
            cond += ' AND d_dataset NOT LIKE "%s"' % name.replace('*', '%')
        else:
            cond += ' AND d_dataset!="%s"' % name
    if  cdate:
        dates = cdate.split('-')
        if  len(dates) == 2:
            cond += ' AND d_creation_date > %s AND d_creation_date < %s' \
                    % (unix_tstamp(dates[0]), unix_tstamp(dates[1]))
            joins = joins.where(joins.d_creation_date>unix_tstamp(dates[0]))
        elif len(dates) == 1:
            cond += ' AND d_creation_date > %s' % unix_tstamp(dates[0])
        else:
            raise NotImplementedError("Given dates are not supported, please either provide YYYYMMDD date or use dash to define a dates range.")

    print("Applied condition %s" % cond)

    if  tier:
        tiers = tier.split(',')
        gen_cond = cond
        for tier in tiers:
            cond = gen_cond + ' AND d_dataset like "%%/%s"' % tier
            fjoin = joins.where(cond).distinct().select(cols)
    else:
        fjoin = joins.where(cond).distinct().select(cols)

    # at this step we have fjoin table with Row(d_dataset_id=9413359, d_dataset=u'/SingleMu/CMSSW_7_1_0_pre9-GR_R_71_V4_RelVal_mu2012D_TEST-v6000/DQM', d_creation_date=1406060166.0, d_is_dataset_valid=1, f_event_count=5318, f_file_size=21132638.0, dataset_access_type=u'DELETED', acquisition_era_name=u'CMSSW_7_1_0_pre9', processing_version=u'6000'))

    newdf = fjoin\
            .groupBy(['d_dataset','d_dataset_id','dataset_access_type','acquisition_era_name','processing_version'])\
            .agg({'f_event_count':'sum', 'f_file_size':'sum', 'd_creation_date':'max'})\
            .withColumnRenamed('sum(f_event_count)', 'evts')\
            .withColumnRenamed('sum(f_file_size)', 'size')\
            .withColumnRenamed('max(d_creation_date)', 'date')


    # at this point we have ndf dataframe with our collected stats for every dataset
    # let's join it with release info
    newdf.registerTempTable('newdf')
    cols = ['d_dataset_id','d_dataset','evts','size','date','dataset_access_type','acquisition_era_name','processing_version','r_release_version']
    stmt = 'SELECT %s FROM newdf JOIN mcf ON newdf.d_dataset_id = mcf.mc_dataset_id JOIN ocf ON mcf.mc_output_mod_config_id = ocf.oc_output_mod_config_id JOIN rvf ON ocf.oc_release_version_id = rvf.r_release_version_id' % ','.join(cols)
    joins = sqlContext.sql(stmt)

    # keep joins table around
    joins.persist(StorageLevel.MEMORY_AND_DISK)

    # print out rows
    if  verbose:
        print("### First rows of joint table, nrows", joins.count())
        for row in joins.head(5):
            print("### row", row)

    # collect results and perform re-mapping
    out = []
    idx = 0
    site = '' # will fill it out when process Phedex data
    naccess = 0 # will fill it out when process Phedex+DBS data and calc naccess
    njobs = 0 # will fill it out when process JobMonitoring data
    cpu = 0 # will fill it out when process JobMonitoring data
    atype = '' # will fill out when process DBS data
    pver = '' # will fill out when process DBS data
    rver = '' # will fill out when process DBS data
    drop_cols = ['d_dataset_id','d_dataset','dataset_access_type','acquisition_era_name','processing_version','r_release_version']
    for row in joins.collect():
        rdict = row.asDict()
        _, primds, procds, tier = rdict['d_dataset'].split('/')
        rdict['primds'] = primds
        rdict['procds'] = procds
        rdict['tier'] = tier
        rdict['era'] = rdict.get('acquisition_era_name', era)
        rdict['atype'] = rdict.get('dataset_access_type', atype)
        rdict['pver'] = rdict.get('processing_version', pver)
        rdict['release'] = rdict.get('r_release_version', rver)
        rdict['site'] = site
        rdict['naccess'] = naccess
        rdict['njobs'] = njobs
        rdict['cpu'] = cpu
        for key in drop_cols:
            if  key in rdict:
                del rdict[key]
        out.append(rdict)
        if  verbose and idx < 5:
            print(rdict)
        idx += 1

    ctx.stop()
    if  verbose:
        logger.info("Elapsed time %s" % elapsed_time(time0))
    return out

def credentials(fname=None):
    "Read credentials from PBR_BROKER environment"
    if  not fname:
        fname = os.environ.get('PBR_BROKER', '')
    if  not os.path.isfile(fname):
        return {}
    with open(fname, 'r') as istream:
        data = json.load(istream)
    return data

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    paths = {'dpath':apath(opts.hdir, 'DATASETS'),
             'bpath':apath(opts.hdir, 'BLOCKS'),
             'fpath':apath(opts.hdir, 'FILES'),
             'apath':apath(opts.hdir, 'ACQUISITION_ERAS'),
             'ppath':apath(opts.hdir, 'PROCESSING_ERAS'),
             'mcpath':apath(opts.hdir, 'DATASET_OUTPUT_MOD_CONFIGS'),
             'ocpath':apath(opts.hdir, 'OUTPUT_MODULE_CONFIGS'),
             'rvpath':apath(opts.hdir, 'RELEASE_VERSIONS'),
             'dapath':apath(opts.hdir, 'DATASET_ACCESS_TYPES')}
    fout = opts.fout
    verbose = opts.verbose
    yarn = opts.yarn
    tier = opts.tier
    era = opts.era
    release = opts.release
    cdate = opts.cdate
    action = opts.action
    patterns = opts.patterns.split(',') if opts.patterns else []
    antipatterns = opts.antipatterns.split(',') if opts.antipatterns else []
    res = run(paths, fout, action, verbose, yarn, tier, era, release, cdate, patterns, antipatterns)

    if opts.amq:
        creds = credentials(opts.amq)
        host, port = creds['host_and_ports'].split(':')
        port = int(port)
        if  creds and StompAMQ:
            print("### Send %s docs via StompAMQ" % len(res))
            amq = StompAMQ(creds['username'], creds['password'], \
                creds['producer'], creds['topic'], [(host, port)])
            data = []
            for doc in res:
                hid = doc.get("hash", 1)
                data.append(amq.make_notification(doc, hid))
            results = amq.send(data)
            print("### results sent by AMQ", len(results))
    else:
        print("### Collected", len(res), "results")
        if len(res)>0:
            print(res[0])


if __name__ == '__main__':
    main()
