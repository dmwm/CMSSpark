#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark utililities
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
from CMSSpark.schemas import schema_processing_eras, schema_dataset_access_types
from CMSSpark.schemas import schema_acquisition_eras,  schema_datasets, schema_blocks
from CMSSpark.schemas import schema_files, schema_mod_configs, schema_out_configs
from CMSSpark.schemas import schema_rel_versions, schema_file_lumis, schema_phedex
from CMSSpark.schemas import schema_jm, schema_cmssw, schema_asodb

from pyspark import SparkContext, StorageLevel
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, BooleanType, LongType
from pyspark.sql.functions import split, col

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


def unionAll_cols(dfs, cols):
    """
    Unions snapshots in one dataframe

    :param item: list of dataframes
    :returns: union of dataframes
    """

    result = unionAll(df.select(cols) for df in dfs)

    return result


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
    date = time.strftime("%Y-%m-%d", time.gmtime(time.time()-60*60*24))
    if  not fromdate:
        fromdate = date
    if  not todate:
        todate = date

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
    "Helper function to print rows from a given dataframe"
    if  verbose:
        print('First %s rows of %s' % (head, dfname))
        for i, row in enumerate(df.head(head)):
            print('%s. %s' % (i, row))

def spark_context(appname='cms', yarn=None, verbose=False, python_files=[]):
    # define spark context, it's main object which allow
    # to communicate with spark
    if  python_files:
        ctx = SparkContext(appName=appname, pyFiles=python_files)
    else:
        ctx = SparkContext(appName=appname)
    logger = SparkLogger(ctx)
    if  not verbose:
        logger.set_level('ERROR')
    if yarn:
        logger.info("YARN client mode enabled")
    return ctx


def delete_hadoop_directory(path):
    os.popen("hadoop fs -rm -r \"" + path + "\"")


def phedex_tables(sqlContext, hdir='hdfs:///project/awg/cms', verbose=False):
    """
    Parse PhEDEx records on HDFS via mapping PhEDEx tables to Spark SQLContext.
    :returns: a dictionary with PhEDEx Spark DataFrame.
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

def dbs_tables(sqlContext, hdir='hdfs:///project/awg/cms', inst='GLOBAL', verbose=False):
    """
    Parse DBS records on HDFS via mapping DBS tables to Spark SQLContext.
    :returns: a dictionary with DBS Spark DataFrame.
    """
    dbsdir = hdir+'/CMS_DBS3_PROD_%s/current' % inst
    paths = {'dpath':apath(dbsdir, 'DATASETS'),
             'bpath':apath(dbsdir, 'BLOCKS'),
             'fpath':apath(dbsdir, 'FILES'),
             'apath':apath(dbsdir, 'ACQUISITION_ERAS'),
             'ppath':apath(dbsdir, 'PROCESSING_ERAS'),
             'mcpath':apath(dbsdir, 'DATASET_OUTPUT_MOD_CONFIGS'),
             'ocpath':apath(dbsdir, 'OUTPUT_MODULE_CONFIGS'),
             'rvpath':apath(dbsdir, 'RELEASE_VERSIONS'),
             'flpath':apath(dbsdir, 'FILE_LUMIS'),
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
    flf = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(path, schema = schema_file_lumis()) \
                        for path in files(paths['flpath'], verbose)])

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
    flf.registerTempTable('flf')

    tables = {'daf':daf, 'ddf':ddf, 'bdf':bdf, 'fdf':fdf, 'aef':aef, 'pef':pef, 'mcf':mcf, 'ocf':ocf, 'rvf':rvf, 'flf': flf}
    return tables

def cmssw_tables(ctx, sqlContext,
        hdir='hdfs:///project/awg/cms/cmssw-popularity/avro-snappy', date=None, verbose=None):
    """
    Parse CMSSW HDFS records.

    Example of CMSSW record on HDFS
    {"UNIQUE_ID":"08F8DD3A-0FFE-E611-B710-BC305B3909F1-1","FILE_LFN":"/s.root",
    "FILE_SIZE":"3865077537","CLIENT_DOMAIN":"in2p3.fr","CLIENT_HOST":"sbgwn141",
    "SERVER_DOMAIN":"in2p3.fr","SERVER_HOST":"sbgse20","SITE_NAME":"T2_FR_IPHC",
    "READ_BYTES_AT_CLOSE":"438385807","READ_BYTES":"438385807",
    "READ_SINGLE_BYTES":"8913451","READ_SINGLE_OPERATIONS":"19",
    "READ_SINGLE_AVERAGE":"469129","READ_SINGLE_SIGMA":"1956390","READ_VECTOR_BYTES":"429472356",
    "READ_VECTOR_OPERATIONS":"58","READ_VECTOR_AVERAGE":"7404700","READ_VECTOR_SIGMA":"6672770",
    "READ_VECTOR_COUNT_AVERAGE":"37.4138","READ_VECTOR_COUNT_SIGMA":"35.242","FALLBACK":"-",
    "USER_DN":"/DC=1846615186/CN=2041527197","APP_INFO":"3809_https://glidein.cern.ch/3809/DSm:4b_0",
    "START_TIME":"1488325657","END_TIME":"1488326400","START_DATE":1488322057000,
    "END_DATE":1488322800000,"INSERT_DATE":1488323999000}

    :returns: a dictionary with CMSSW Spark DataFrame
    """
    rdd = avro_rdd(ctx, sqlContext, hdir, date, verbose)

    # create new spark DataFrame
    jdf = sqlContext.createDataFrame(rdd, schema=schema_cmssw())
    df = jdf.withColumn("READ_BYTES", jdf["READ_BYTES"].cast(LongType()))\
            .withColumn("READ_BYTES_AT_CLOSE", jdf["READ_BYTES_AT_CLOSE"].cast(LongType()))\
            .withColumn("READ_SINGLE_BYTES", jdf["READ_SINGLE_BYTES"].cast(LongType()))\
            .withColumn("READ_SINGLE_OPERATIONS", jdf["READ_SINGLE_OPERATIONS"].cast(DoubleType()))\
            .withColumn("READ_SINGLE_AVERAGE", jdf["READ_SINGLE_AVERAGE"].cast(DoubleType()))\
            .withColumn("READ_SINGLE_SIGMA", jdf["READ_SINGLE_SIGMA"].cast(DoubleType()))\
            .withColumn("READ_VECTOR_BYTES", jdf["READ_VECTOR_BYTES"].cast(LongType()))\
            .withColumn("READ_VECTOR_OPERATIONS", jdf["READ_VECTOR_OPERATIONS"].cast(DoubleType()))\
            .withColumn("READ_VECTOR_AVERAGE", jdf["READ_VECTOR_AVERAGE"].cast(DoubleType()))\
            .withColumn("READ_VECTOR_SIGMA", jdf["READ_VECTOR_SIGMA"].cast(DoubleType()))\
            .withColumn("READ_VECTOR_COUNT_AVERAGE", jdf["READ_VECTOR_COUNT_AVERAGE"].cast(DoubleType()))\
            .withColumn("READ_VECTOR_COUNT_SIGMA", jdf["READ_VECTOR_COUNT_SIGMA"].cast(DoubleType()))
    df = sqlContext.createDataFrame(rdd, schema=schema_cmssw())
    df.registerTempTable('cmssw_df')
    tables = {'cmssw_df': df}
    return tables

def jm_tables(ctx, sqlContext,
        hdir='hdfs:///project/awg/cms/jm-data-popularity/avro-snappy', date=None, verbose=None):
    """
    Parse JobMonitoring popularity HDFS records.

    Example of jm-data-popularity record on HDFS
    {"JobId":"1672451388","FileName":"//store/file.root","IsParentFile":"0","ProtocolUsed":"Remote",
    "SuccessFlag":"1","FileType":"EDM","LumiRanges":"unknown","StrippedFiles":"0","BlockId":"602064",
    "StrippedBlocks":"0","BlockName":"Dummy","InputCollection":"DoesNotApply","Application":"CMSSW",
    "Type":"reprocessing","SubmissionTool":"wmagent","InputSE":"","TargetCE":"","SiteName":"T0_CH_CERN",
    "SchedulerName":"PYCONDOR","JobMonitorId":"unknown","TaskJobId":"1566463230",
    "SchedulerJobIdV2":"664eef36-f1c3-11e6-88b9-02163e0184a6-367_0","TaskId":"35076445",
    "TaskMonitorId":"wmagent_pdmvserv_task_S_640","JobExecExitCode":"0",
    "JobExecExitTimeStamp":1488375506000,"StartedRunningTimeStamp":1488374686000,
    "FinishedTimeStamp":1488375506000,"WrapWC":"820","WrapCPU":"1694.3","ExeCPU":"0",
    "UserId":"124370","GridName":"Alan Malta Rodrigues"}

    :returns: a dictionary with JobMonitoring Spark DataFrame
    """
    rdd = avro_rdd(ctx, sqlContext, hdir, date, verbose)

    # create new spark DataFrame
    jdf = sqlContext.createDataFrame(rdd, schema=schema_jm())
    df = jdf.withColumn("WrapWC", jdf["WrapWC"].cast(DoubleType()))\
            .withColumn("WrapCPU", jdf["WrapCPU"].cast(DoubleType()))\
            .withColumn("ExeCPU", jdf["ExeCPU"].cast(DoubleType()))
    df.registerTempTable('jm_df')
    tables = {'jm_df': df}
    return tables

def avro_rdd(ctx, sqlContext, hdir, date=None, verbose=None):
    """
    Parse avro-snappy files on HDFS
    :returns: a Spark RDD object
    """

    if  date == None:
        date = time.strftime("year=%Y/month=%-m/date=%d", time.gmtime(time.time()-60*60*24))
        path = '%s/%s' % (hdir, date)
    else:
        path = hdir
        if  date:
            path = '%s/%s' % (hdir, date)

    print("### hdir", path)
    if  isinstance(path, list):
        afiles = path
    else:
        # get avro files from HDFS
        afiles = avro_files(path, verbose=verbose)
    print("### avro_files", afiles)

    # define newAPIHadoopFile parameters, java classes
    aformat="org.apache.avro.mapreduce.AvroKeyInputFormat"
    akey="org.apache.avro.mapred.AvroKey"
    awrite="org.apache.hadoop.io.NullWritable"
    aconv="org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter"

    # load data from HDFS
    rdd = ctx.union([ctx.newAPIHadoopFile(f, aformat, akey, awrite, aconv) for f in afiles])

    # the records are stored as [(dict, None), (dict, None)], therefore we take first element
    # and assign them to new rdd
    avro_rdd = rdd.map(lambda x: x[0])
    records = avro_rdd.take(1) # take function will return list of records
    if  verbose:
        print("### avro records", records, type(records))
    return avro_rdd

def aaa_tables(sqlContext,
        hdir='hdfs:///project/monitoring/archive/xrootd/raw/gled',
        date=None, verbose=False):
    """
    Parse AAA HDFS records.

    Example of AAA (xrootd) JSON record on HDFS
    {"data":{"activity":"r","app_info":"","client_domain":"cern.ch","client_host":"b608a4fe55","end_time":1491789715000,"file_lfn":"/eos/cms/store/hidata/PARun2016C/PAEGJet1/AOD/PromptReco-v1/000/286/471/00000/7483FE13-28BD-E611-A2BD-02163E01420E.root","file_size":189272229,"is_transfer":true,"operation_time":690,"read_average":0.0,"read_bytes":0,"read_bytes_at_close":189272229,"read_max":0,"read_min":0,"read_operations":0,"read_sigma":0.0,"read_single_average":0.0,"read_single_bytes":0,"read_single_max":0,"read_single_min":0,"read_single_operations":0,"read_single_sigma":0.0,"read_vector_average":0.0,"read_vector_bytes":0,"read_vector_count_average":0.0,"read_vector_count_max":0,"read_vector_count_min":0,"read_vector_count_sigma":0.0,"read_vector_max":0,"read_vector_min":0,"read_vector_operations":0,"read_vector_sigma":0.0,"remote_access":false,"server_domain":"cern.ch","server_host":"p05799459u51457","server_username":"","start_time":1491789025000,"throughput":274307.57826086954,"unique_id":"03404bbc-1d90-11e7-9717-47f48e80beef-2e48","user":"","user_dn":"","user_fqan":"","user_role":"","vo":"","write_average":0.0,"write_bytes":0,"write_bytes_at_close":0,"write_max":0,"write_min":0,"write_operations":0,"write_sigma":0.0},"metadata":{"event_timestamp":1491789715000,"hostname":"monit-amqsource-fafa51de8d.cern.ch","kafka_timestamp":1491789741627,"original-destination":"/topic/xrootd.cms.eos","partition":"10","producer":"xrootd","timestamp":1491789740015,"topic":"xrootd_raw_gled","type":"gled","type_prefix":"raw","version":"003"}}

    :returns: a dictionary with AAA Spark DataFrame
    """
    if  not date:
        # by default we read yesterdate data
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))

    hpath = '%s/%s' % (hdir, date)
    rdd = unionAll([sqlContext.jsonFile(path) for path in files(hpath, verbose)])
    aaa_rdd = rdd.map(lambda r: r['data'])
    records = aaa_rdd.take(1) # take function will return list of records
    if  verbose:
        print("### aaa_rdd records", records, type(records))

    # create new spark DataFrame
    aaa_df = sqlContext.createDataFrame(aaa_rdd)
    aaa_df.registerTempTable('aaa_df')
    tables = {'aaa_df':aaa_df}
    return tables

def aaa_tables_enr(sqlContext,
        hdir='hdfs:///project/monitoring/archive/xrootd/enr/gled',
        date=None, verbose=False):
    """
    Parse AAA HDFS records.

    Example of AAA (xrootd) JSON record on HDFS
    {"data":{"activity":"r","app_info":"","client_domain":"cern.ch","client_host":"b608a4fe55","end_time":1491789715000,"file_lfn":"/eos/cms/store/hidata/PARun2016C/PAEGJet1/AOD/PromptReco-v1/000/286/471/00000/7483FE13-28BD-E611-A2BD-02163E01420E.root","file_size":189272229,"is_transfer":true,"operation_time":690,"read_average":0.0,"read_bytes":0,"read_bytes_at_close":189272229,"read_max":0,"read_min":0,"read_operations":0,"read_sigma":0.0,"read_single_average":0.0,"read_single_bytes":0,"read_single_max":0,"read_single_min":0,"read_single_operations":0,"read_single_sigma":0.0,"read_vector_average":0.0,"read_vector_bytes":0,"read_vector_count_average":0.0,"read_vector_count_max":0,"read_vector_count_min":0,"read_vector_count_sigma":0.0,"read_vector_max":0,"read_vector_min":0,"read_vector_operations":0,"read_vector_sigma":0.0,"remote_access":false,"server_domain":"cern.ch","server_host":"p05799459u51457","server_username":"","start_time":1491789025000,"throughput":274307.57826086954,"unique_id":"03404bbc-1d90-11e7-9717-47f48e80beef-2e48","user":"","user_dn":"","user_fqan":"","user_role":"","vo":"","write_average":0.0,"write_bytes":0,"write_bytes_at_close":0,"write_max":0,"write_min":0,"write_operations":0,"write_sigma":0.0},"metadata":{"event_timestamp":1491789715000,"hostname":"monit-amqsource-fafa51de8d.cern.ch","kafka_timestamp":1491789741627,"original-destination":"/topic/xrootd.cms.eos","partition":"10","producer":"xrootd","timestamp":1491789740015,"topic":"xrootd_raw_gled","type":"gled","type_prefix":"raw","version":"003"}}

    :returns: a dictionary with AAA Spark DataFrame
    """
    if  not date:
        # by default we read yesterdate data
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))

    hpath = '%s/%s' % (hdir, date)
    cols = ['data.src_experiment_site', 'data.user_dn', 'data.file_lfn']
    aaa_df = unionAll_cols([sqlContext.jsonFile(path) for path in files(hpath, verbose)], cols)

    aaa_df.registerTempTable('aaa_df')
    tables = {'aaa_df':aaa_df}
    return tables

def eos_tables(sqlContext,
        hdir='hdfs:///project/monitoring/archive/eos/logs/reports/cms',
        date=None, verbose=False):
    """
    Parse EOS HDFS records

    Example of EOS JSON record on HDFS
    {"data":"\"log=9e7436fe-1d8e-11e7-ba07-a0369f1fbf0c&path=/store/mc/PhaseISpring17GS/MinBias_TuneCUETP8M1_13TeV-pythia8/GEN-SIM/90X_upgrade2017_realistic_v20-v1/50000/72C78841-2110-E711-867F-F832E4CC4D39.root&ruid=8959&rgid=1399&td=nobody.693038:472@fu-c2e05-24-03-daq2fus1v0--cms&host=p05798818q44165.cern.ch&lid=1048850&fid=553521212&fsid=18722&ots=1491788403&otms=918&cts=1491789688&ctms=225&rb=19186114&rb_min=104&rb_max=524288&rb_sigma=239596.05&wb=0&wb_min=0&wb_max=0&wb_sigma=0.00&sfwdb=7576183815&sbwdb=6313410471&sxlfwdb=7575971197&sxlbwdb=6313300667&nrc=72&nwc=0&nfwds=24&nbwds=10&nxlfwds=12&nxlbwds=4&rt=9130.44&wt=0.00&osize=3850577700&csize=3850577700&sec.prot=gsi&sec.name=cmsprd&sec.host=cms-ucsrv-c2f46-32-07.cern.ch&sec.vorg=&sec.grps=&sec.role=&sec.info=/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=amaltaro/CN=718748/CN=Alan Malta Rodrigues&sec.app=\"","metadata":{"host":"eoscms-srv-m1.cern.ch","kafka_timestamp":1491789692305,"partition":"14","path":"cms","producer":"eos","timestamp":1491789689562,"topic":"eos_logs","type":"reports","type_prefix":"logs"}}

    The EOS record consist of data and metadata parts where data part squashed
    into single string all requested parameters.

    :returns: a dictionary with eos Spark DataFrame
    """
    if  not date:
        # by default we read yesterdate data
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))

    hpath = '%s/%s' % (hdir, date)
    rdd = unionAll([sqlContext.jsonFile(path) for path in files(hpath, verbose)])
    def parse_log(r):
        "Local helper function to parse EOS record and extract intersting fields"
        rdict = {}
        for item in r['data'].split('&'):
            if  item.startswith('path='):
                rdict['file_lfn'] = item.split('path=')[-1]
            if  item.startswith('sec.info='):
                rdict['user_dn'] = item.split('sec.info=')[-1]
            if  item.startswith('sec.host='):
                rdict['host'] = item.split('sec.host=')[-1]

        rdict['timestamp'] = r['metadata']['timestamp']

        return rdict

    eos_rdd = rdd.map(lambda r: parse_log(r))

    records = eos_rdd.take(1) # take function will return list of records
    if  verbose:
        print("### eos_rdd records", records, type(records))

    # create new spark DataFrame
    eos_df = sqlContext.createDataFrame(eos_rdd)
    eos_df.registerTempTable('eos_df')
    tables = {'eos_df':eos_df}
    return tables

def fts_tables(sqlContext,
        hdir='hdfs:///project/monitoring/archive/fts/raw/complete',
        date=None, verbose=False):
    """
    Parse fts HDFS records

    Example of fts JSON record on HDFS
    {"data":{"activity":"Data Consolidation","block_size":0,"buf_size":0,"channel_type":"urlcopy","chk_timeout":0,"dest_srm_v":"2.2.0","dst_hostname":"grid002.ft.uam.es","dst_se":"srm://grid002.ft.uam.es","dst_site_name":"","dst_url":"srm://grid002.ft.uam.es:8443/srm/managerv2?SFN=/pnfs/ft.uam.es/data/atlas/atlasdatadisk/rucio/mc15_13TeV/60/36/EVNT.11173001._023578.pool.root.1","endpnt":"fts3.cern.ch","f_size":10556847,"file_id":"1461431003","file_metadata":{"activity":"Data Consolidation","adler32":"d02c0375","dest_rse_id":"a07afdb2953442f78bd87136e32c2674","dst_rse":"UAM-LCG2_DATADISK","dst_type":"DISK","filesize":1.0556847E7,"name":"EVNT.11173001._023578.pool.root.1","request_id":"faebf1c016ad4c8a89d4be53b67347ce","request_type":"transfer","scope":"mc15_13TeV","src_rse":"INFN-T1_DATADISK","src_rse_id":"8ebac4c63f98418a8e4b7e9da4ced658","src_type":"DISK","verify_checksum":true},"file_size":10556847,"ipv6":false,"job_id":"f9b18c10-0132-51df-b341-3031a0a9ea39","job_metadata":{"issuer":"rucio","multi_sources":true},"job_state":"FINISHED","latency":9960,"log_link":"https://fts3.cern.ch:8449/fts3/ftsmon/#/f9b18c10-0132-51df-b341-3031a0a9ea39","nstreams":1,"operation_time":2308,"remote_access":true,"retry":0,"retry_max":0,"src_hostname":"storm-fe.cr.cnaf.infn.it","src_se":"srm://storm-fe.cr.cnaf.infn.it","src_site_name":"","src_srm_v":"2.2.0","src_url":"srm://storm-fe.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/atlas/atlasdatadisk/rucio/mc15_13TeV/60/36/EVNT.11173001._023578.pool.root.1","srm_finalization_time":9371,"srm_overhead_percentage":95.6715802107948,"srm_overhead_time":51014,"srm_preparation_time":41643,"srm_space_token_dst":"ATLASDATADISK","srm_space_token_src":"","t__error_message":"","t_channel":"storm-fe.cr.cnaf.infn.it__grid002.ft.uam.es","t_error_code":"0","t_failure_phase":"","t_final_transfer_state":"Ok","t_final_transfer_state_flag":1,"t_timeout":621,"tcp_buf_size":0,"time_srm_fin_end":1493217343934,"time_srm_fin_st":1493217334563,"time_srm_prep_end":1493217332255,"time_srm_prep_st":1493217290612,"timestamp_checksum_dest_ended":1493217343970,"timestamp_checksum_dest_st":1493217343934,"timestamp_checksum_dst_diff":36,"timestamp_checksum_src_diff":124,"timestamp_chk_src_ended":1493217290736,"timestamp_chk_src_st":1493217290612,"timestamp_tr_comp":1493217334563,"timestamp_tr_st":1493217332255,"tr_bt_transfered":10556847,"tr_error_category":"","tr_error_scope":"","tr_id":"2017-04-26-1435__storm-fe.cr.cnaf.infn.it__grid002.ft.uam.es__1461431003__f9b18c10-0132-51df-b341-3031a0a9ea39","tr_timestamp_complete":1493217344240,"tr_timestamp_start":1493217290323,"user":"","user_dn":"","vo":"atlas"},"metadata":{"event_timestamp":1493217334563,"hostname":"monit-amqsource-9b83c9b3a5.cern.ch","kafka_timestamp":1493217345874,"original-destination":"/topic/transfer.fts_monitoring_complete","partition":"13","producer":"fts","timestamp":1493217344523,"topic":"fts_raw_complete","type":"complete","type_prefix":"raw","version":"006"}}

    The fts record consist of data and metadata parts where data part squashed
    into single string all requested parameters.

    :returns: a dictionary with fts Spark DataFrame
    """
    if  not date:
        # by default we read yesterdate data
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))

    hpath = '%s/%s' % (hdir, date)

    # create new spark DataFrame
    fts_df = sqlContext.read.json(hpath)
    fts_df.registerTempTable('fts_df')
    fts_df = fts_df.select(unpack_struct("data", fts_df)) # extract data part of JSON records
    fts_df.printSchema()
    tables = {'fts_df':fts_df}
    return tables

def split_dataset(df, dcol):
    "Split dataset name in DataFrame into primds,procds,tier components"
    ndf = df.withColumn("primds", split(col(dcol), "/").alias('primds').getItem(1))\
            .withColumn("procds", split(col(dcol), "/").alias('procds').getItem(2))\
            .withColumn("tier", split(col(dcol), "/").alias('tier').getItem(3))\
            .drop(dcol)
    return ndf

def unpack_struct(colname, df):
    "Unpack structure and extract specific column from dataframe"
    parent = filter(lambda field: field.name == colname, df.schema.fields).pop()
    fields = parent.dataType.fields \
            if isinstance(parent.dataType, StructType) else []
    return map(lambda x : col(colname+"."+x.name), fields)

def aso_tables(sqlContext, hdir='hdfs:///project/awg/cms', verbose=False):
    """
    Parse ASO records on HDFS via mapping ASO tables to Spark SQLContext.
    :returns: a dictionary with ASO Spark DataFrame.
    """
    adir = hdir+'/CMS_ASO/filetransfersdb/merged'

    # aso data
    pfiles = os.popen("hadoop fs -ls %s | grep part | awk '{print $8}'" % adir).read().splitlines()
    msg = "ASO snapshot found %d directories" % len(pfiles)
    print(msg)
    aso_df = unionAll([sqlContext.read.format('com.databricks.spark.csv')
                    .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                    .load(file_path, schema = schema_asodb()) \
                    for file_path in pfiles])

    # Register temporary tables to be able to use sqlContext.sql
    aso_df.registerTempTable('aso_df')

    tables = {'aso_df':aso_df}
    return tables
