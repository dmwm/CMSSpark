#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : spark_utils.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : Spark utilities
"""

# system modules
import logging
import os
import re
import time
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

from pyspark import SparkContext, StorageLevel
from pyspark.sql import DataFrame, SparkSession, SQLContext
from pyspark.sql.functions import split, col, date_format, from_unixtime, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType, StructType

# CMSSpark modules
from CMSSpark.schemas import schema_acquisition_eras, schema_datasets, schema_blocks
from CMSSpark.schemas import schema_cmspop_json
from CMSSpark.schemas import schema_files, schema_mod_configs, schema_out_configs
from CMSSpark.schemas import schema_jm, schema_asodb, schema_empty_aaa, schema_empty_eos
from CMSSpark.schemas import schema_phedex_summary, schema_data_tiers
from CMSSpark.schemas import schema_processing_eras, schema_dataset_access_types
from CMSSpark.schemas import schema_rel_versions, schema_file_lumis, schema_phedex


class SparkLogger(object):
    """Control Spark Logger"""

    def __init__(self, ctx):
        self.logger = ctx._jvm.org.apache.log4j
        self.rlogger = self.logger.LogManager.getRootLogger()

    def set_level(self, level):
        """Set Spark Logger level"""
        self.rlogger.setLevel(getattr(self.logger.Level, level))

    def lprint(self, stream, msg):
        """Print message via Spark Logger to given stream"""
        getattr(self.rlogger, stream)(msg)

    def info(self, msg):
        """Print message via Spark Logger to info stream"""
        self.lprint('info', msg)

    def error(self, msg):
        """Print message via Spark Logger to error stream"""
        self.lprint('error', msg)

    def warning(self, msg):
        """Print message via Spark Logger to warning stream"""
        self.lprint('warning', msg)


def apath(hdir, name):
    """Helper function to construct attribute path"""
    return os.path.join(hdir, name)


def files(path, verbose=0):
    """Return list of files for given HDFS path"""
    hpath = "hadoop fs -ls %s | awk '{print $8}'" % path
    if verbose:
        print("Lookup area: %s" % hpath)
    stream = os.popen(hpath)
    fnames = [f for f in stream.read().splitlines() if f.find('part-') != -1]
    return fnames


def glob_files(sc, url):
    """Return a list of files. It uses the jvm gateway.
       This function should be prefered to files when using glob expressions.
    """
    uri = sc._gateway.jvm.java.net.URI
    path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fsystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = fsystem.get(uri("hdfs:///"), sc._jsc.hadoopConfiguration())
    lst = fs.globStatus(path(url))
    return [f.getPath().toString() for f in lst]


def avro_files(path, verbose=0):
    """Return list of files for given HDFS path"""
    hpath = "hadoop fs -ls %s/*.avro | awk '{print $8}'" % path
    if verbose:
        print("### Avro files area: %s" % hpath)
    stream = os.popen(hpath)
    fnames = [f for f in stream.read().splitlines() if f.endswith('avro\n')]
    return fnames


def union_all(dfs, cols=None):
    """Unions snapshots in one dataframe

    :param dfs: dataframes to union
    :param cols: list of dataframe columns
    :returns: union of dataframes
    """

    if cols is None:
        return reduce(DataFrame.unionAll, dfs)
    else:
        return union_all(df.select(cols) for df in dfs)


def file_list(basedir, fromdate=None, todate=None):
    """Finds snapshots in given directory by interval dates

    :param basedir: directory where snapshots are held
    :param fromdate: date from which snapshots are filtered
    :param todate: date until which snapshots are filtered
    :returns: array of filtered snapshots paths
    :raises ValueError: if unparsable date format
    """
    sed = "sed '1d;s/  */ /g'"
    cut = "cut -d\  -f8"
    dirs = os.popen(f"hadoop fs -ls {basedir} | {sed} | {cut}").read().splitlines()
    # if files are not in hdfs --> dirs = os.listdir(basedir)

    # by default we'll use yesterday date on HDFS to avoid clashes
    date = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 60 * 60 * 24))
    if not fromdate:
        fromdate = date
    if not todate:
        todate = date

    o_fromdate = fromdate
    o_todate = todate
    try:
        fromdate = dt.strptime(fromdate, "%Y-%m-%d")
        todate = dt.strptime(todate, "%Y-%m-%d")
    except ValueError as err:
        print("### fromdate", fromdate)
        print("### todate", todate)
        print("### error", str(err))
        raise ValueError("Unparsable date parameters. Date should be specified in form: YYYY-mm-dd")

    pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

    dirdate_dic = {}
    from_match = 0
    to_match = 0
    for idir in dirs:
        if idir.find(o_fromdate) != -1:
            from_match = 1
        if idir.find(o_todate) != -1:
            to_match = 1
        matching = pattern.search(idir)
        if matching:
            dirdate_dic[idir] = dt.strptime(matching.group(1), "%Y-%m-%d")

    if not from_match:
        raise Exception("Unable to find fromdate=%s are on HDFS %s" % (o_fromdate, basedir))
    if not to_match:
        raise Exception("Unable to find todate=%s are on HDFS %s" % (o_todate, basedir))
    return [k for k, v in dirdate_dic.items() if fromdate <= v <= todate]


def print_rows(df, dfname, verbose, head=5):
    """Helper function to print rows from a given dataframe"""
    if verbose:
        print('First %s rows of %s' % (head, dfname))
        for i, row in enumerate(df.head(head)):
            print('%s. %s' % (i, row))


def spark_context(appname='cms', yarn=None, verbose=False, python_files=None):
    """Define spark context, it's main object which allow to communicate with spark"""
    if python_files is None:
        python_files = []
    if python_files:
        ctx = SparkContext(appName=appname, pyFiles=python_files)
    else:
        ctx = SparkContext(appName=appname)
    logger = SparkLogger(ctx)
    if not verbose:
        logger.set_level('ERROR')
    if yarn:
        logger.info("YARN client mode enabled")
    return ctx


def delete_hadoop_directory(path):
    os.popen("hadoop fs -rm -r \"" + path + "\"")


def unix2human(tstamp):
    """Convert unix time stamp into human readable format"""
    return time.strftime('%Y%m%d', time.gmtime(tstamp))


def phedex_summary_tables(sql_context, hdir='hdfs:///cms/phedex'):
    """Parse PhEDEx records on HDFS via mapping PhEDEx tables to Spark SQLContext.

    :returns: a dictionary with PhEDEx Spark DataFrame.
    """
    # look-up phedex summary area and construct a list of part-XXXXX files
    sed = "sed '1d;s/  */ /g'"
    cut = "cut -d\  -f8"
    dirs = os.popen(f"hadoop fs -ls {hdir} | {sed} | {cut}").read().splitlines()

    dfs = []
    for idir in dirs:
        pfiles = []
        for fname in files(idir):
            if 'part-' in fname:
                pfiles.append(fname)
        tstmp = time.strftime("%Y%m%d %H:%S", time.gmtime())
        msg = "Phedex snapshot %s %s: %d files" % (tstmp, idir, len(pfiles))
        print(msg)
        if not len(pfiles):
            print("Skip %s" % idir)
            continue
        pdf = union_all([sql_context.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(file_path, schema=schema_phedex_summary())
                         for file_path in pfiles])
        pdf.persist(StorageLevel.MEMORY_AND_DISK)
        dfs.append(pdf)

    # Register temporary tables to be able to use sqlContext.sql
    phedex_summary_df = union_all(dfs)
    phedex_summary_df.registerTempTable('phedex_summary_df')

    tables = {'phedex_summary_df': phedex_summary_df}
    return tables


def phedex_tables(sqlContext, hdir='hdfs:///project/awg/cms', verbose=False, fromdate=None, todate=None):
    """
    Parse PhEDEx records on HDFS via mapping PhEDEx tables to Spark SQLContext.
    :returns: a dictionary with PhEDEx Spark DataFrame.
    """
    phxdir = hdir + '/phedex/block-replicas-snapshots/csv/'

    # phedex data
    pfiles = file_list(phxdir, fromdate, todate)
    msg = "Phedex snapshot %s-%s found %d directories" \
          % (fromdate, todate, len(pfiles))
    print(msg)
    phedex_df = union_all([sqlContext.read.format('com.databricks.spark.csv')
                          .options(treatEmptyValuesAsNulls='true', nullValue='null')
                          .load(file_path, schema=schema_phedex())
                           for file_path in pfiles])

    # Register temporary tables to be able to use sqlContext.sql
    phedex_df.registerTempTable('phedex_df')

    tables = {'phedex_df': phedex_df}
    return tables


def dbs_tables(sqlContext, hdir='hdfs:///project/awg/cms', inst='GLOBAL', verbose=False, tables=None):
    """
    Parse DBS records on HDFS via mapping DBS tables to Spark SQLContext.
    :returns: a dictionary with DBS Spark DataFrame.
    """
    dbsdir = hdir + '/CMS_DBS3_PROD_%s/current' % inst
    paths = {'dpath': apath(dbsdir, 'DATASETS'),
             'tpath': apath(dbsdir, 'DATA_TIERS'),
             'bpath': apath(dbsdir, 'BLOCKS'),
             'fpath': apath(dbsdir, 'FILES'),
             'apath': apath(dbsdir, 'ACQUISITION_ERAS'),
             'ppath': apath(dbsdir, 'PROCESSING_ERAS'),
             'mcpath': apath(dbsdir, 'DATASET_OUTPUT_MOD_CONFIGS'),
             'ocpath': apath(dbsdir, 'OUTPUT_MODULE_CONFIGS'),
             'rvpath': apath(dbsdir, 'RELEASE_VERSIONS'),
             'flpath': apath(dbsdir, 'FILE_LUMIS'),
             'dapath': apath(dbsdir, 'DATASET_ACCESS_TYPES')}
    logging.info("Use the following data on HDFS")
    dict_dbs_tables = {}  # final dict of tables we'll load
    for key, val in paths.items():
        if tables:
            if key in tables:
                logging.info(val)
        else:
            logging.info(val)

    # define DBS tables
    if not tables or 'daf' in tables:
        daf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_dataset_access_types())
                         for path in files(paths['dapath'], verbose)])
        daf.registerTempTable('daf')
        dict_dbs_tables.update({'daf': daf})
    if not tables or 'ddf' in tables:
        ddf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_datasets())
                         for path in files(paths['dpath'], verbose)])
        ddf.registerTempTable('ddf')
        dict_dbs_tables.update({'ddf': ddf})
    if not tables or 'dtf' in tables:
        dtf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_data_tiers())
                         for path in files(paths['tpath'], verbose)])
        dtf.registerTempTable('dtf')
        dict_dbs_tables.update({'dtf': dtf})
    if not tables or 'bdf' in tables:
        bdf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_blocks())
                         for path in files(paths['bpath'], verbose)])
        bdf.registerTempTable('bdf')
        dict_dbs_tables.update({'bdf': bdf})
    if not tables or 'fdf' in tables:
        fdf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_files())
                         for path in files(paths['fpath'], verbose)])
        fdf.registerTempTable('fdf')
        dict_dbs_tables.update({'fdf': fdf})
    if not tables or 'aef' in tables:
        aef = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_acquisition_eras())
                         for path in files(paths['apath'], verbose)])
        aef.registerTempTable('aef')
        dict_dbs_tables.update({'aef': aef})
    if not tables or 'pef' in tables:
        pef = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_processing_eras())
                         for path in files(paths['ppath'], verbose)])
        pef.registerTempTable('pef')
        dict_dbs_tables.update({'pef': pef})
    if not tables or 'mcf' in tables:
        mcf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_mod_configs())
                         for path in files(paths['mcpath'], verbose)])
        mcf.registerTempTable('mcf')
        dict_dbs_tables.update({'mcf': mcf})
    if not tables or 'ocf' in tables:
        ocf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_out_configs())
                         for path in files(paths['ocpath'], verbose)])
        ocf.registerTempTable('ocf')
        dict_dbs_tables.update({'ocf': ocf})
    if not tables or 'rvf' in tables:
        rvf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_rel_versions())
                         for path in files(paths['rvpath'], verbose)])
        rvf.registerTempTable('rvf')
        dict_dbs_tables.update({'rvf': rvf})
    if not tables or 'flf' in tables:
        flf = union_all([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')
                        .load(path, schema=schema_file_lumis())
                         for path in files(paths['flpath'], verbose)])
        flf.registerTempTable('flf')
        dict_dbs_tables.update({'flf': flf})

    return dict_dbs_tables


def cmssw_tables(ctx, spark, hdir="hdfs:///project/monitoring/archive/cmssw_pop/raw/metric/", date=None,
                 verbose=None, ):
    """
       Parse cmssw popularity data from HDFS. 
    """
    # For backward compatibility
    if hdir == "hdfs:///project/awg/cms/cmssw-popularity/avro-snappy":
        hdir = "hdfs:///project/monitoring/archive/cmssw_pop/raw/metric/"
        logging.warning(
            "Deprecated: for backward compability {}".format(hdir) +
            " will be used instead of " +
            "hdfs:///project/awg/cms/cmssw-popularity/avro-snappy"
        )
    if date is None:
        date = time.strftime("%Y/%-m/%-d", time.gmtime(time.time() - 60 * 60 * 24))
        path = "%s/%s" % (hdir, date)
    elif len(str(date)) == 8:  # YYYYMMDD
        ddd = dt.strptime(str(date), "%Y%m%d")
        date = time.strftime("%Y/%-m/%-d", ddd.utctimetuple())
        path = "%s/%s" % (hdir, date)
    else:
        path = hdir
        if date:
            path = "%s/%s" % (hdir, date)
    df = (spark.read.option("basePath", hdir).json(path, schema=schema_cmspop_json()).select("data.*"))

    df = df.select(*[c.upper() for c in df.columns])
    df = df.withColumn("FILE_LFN", regexp_replace("FILE_LFN", "file:", ""))
    df.registerTempTable("cmssw_df")
    tables = {"cmssw_df": df}
    return tables


def jm_tables(ctx, sqlContext, hdir='hdfs:///project/awg/cms/jm-data-popularity/avro-snappy', date=None, verbose=None):
    """
    Parse JobMonitoring popularity HDFS records comes from
    https://gitlab.cern.ch/awg/awg-ETL-crons/blob/master/sqoop/jm-cms-data-pop.sh

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
    df = jdf.withColumn("WrapWC", jdf["WrapWC"].cast(DoubleType())) \
        .withColumn("WrapCPU", jdf["WrapCPU"].cast(DoubleType())) \
        .withColumn("ExeCPU", jdf["ExeCPU"].cast(DoubleType())) \
        .withColumn("NCores", jdf["NCores"].cast(IntegerType())) \
        .withColumn("NEvProc", jdf["NEvProc"].cast(IntegerType())) \
        .withColumn("NEvReq", jdf["NEvReq"].cast(IntegerType()))
    df.registerTempTable('jm_df')
    tables = {'jm_df': df}
    return tables


def avro_rdd(ctx, sqlContext, hdir, date=None, verbose=None):
    """
    Parse avro-snappy files on HDFS
    :returns: a Spark RDD object
    """

    if date is None:
        date = time.strftime("year=%Y/month=%-m/day=%-d", time.gmtime(time.time() - 60 * 60 * 24))
        path = '%s/%s' % (hdir, date)
    elif len(str(date)) == 8:  # YYYYMMDD
        ddd = dt.strptime(str(date), "%Y%m%d")
        date = time.strftime("year=%Y/month=%-m/day=%-d", ddd.utctimetuple())
        path = '%s/%s' % (hdir, date)
    else:
        path = hdir
        if date:
            path = '%s/%s' % (hdir, date)

    print("### hdir", path, type(path))
    if isinstance(path, list):
        afiles = path
    else:
        # get avro files from HDFS
        afiles = avro_files(path, verbose=verbose)
    print("### avro_files", afiles)

    # define newAPIHadoopFile parameters, java classes
    aformat = "org.apache.avro.mapreduce.AvroKeyInputFormat"
    akey = "org.apache.avro.mapred.AvroKey"
    awrite = "org.apache.hadoop.io.NullWritable"
    aconv = "org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter"

    # load data from HDFS
    if len(afiles) == 0:
        rdd = ctx.emptyRDD()
    else:
        rdd = ctx.union([ctx.newAPIHadoopFile(f, aformat, akey, awrite, aconv) for f in afiles])

    # the records are stored as [(dict, None), (dict, None)], therefore we take first element
    # and assign them to new rdd
    dict_avro_rdd = rdd.map(lambda x: x[0])
    records = dict_avro_rdd.take(1)  # take function will return list of records
    if verbose:
        print("### avro records", records, type(records))
    return dict_avro_rdd


def aaa_tables(sqlContext,
               hdir='hdfs:///project/monitoring/archive/xrootd/raw/gled',
               date=None, verbose=False):
    """
    Parse AAA HDFS records. This data set comes from XRootD servers around the
    world. Data is send by XRootD servers across CERN and US to dedicated
    clients, called GLED. These GLED clients collect the XRootD data and send
    it a messaging broker at CERN. From the messaging broker, data is consumer
    by us and integrated in the MONIT infrastructure.

    Example of AAA (xrootd) JSON record on HDFS
    {"data":{"activity":"r","app_info":"","client_domain":"cern.ch","client_host":"b608a4fe55","end_time":1491789715000,"file_lfn":"/eos/cms/store/hidata/PARun2016C/PAEGJet1/AOD/PromptReco-v1/000/286/471/00000/7483FE13-28BD-E611-A2BD-02163E01420E.root","file_size":189272229,"is_transfer":true,"operation_time":690,"read_average":0.0,"read_bytes":0,"read_bytes_at_close":189272229,"read_max":0,"read_min":0,"read_operations":0,"read_sigma":0.0,"read_single_average":0.0,"read_single_bytes":0,"read_single_max":0,"read_single_min":0,"read_single_operations":0,"read_single_sigma":0.0,"read_vector_average":0.0,"read_vector_bytes":0,"read_vector_count_average":0.0,"read_vector_count_max":0,"read_vector_count_min":0,"read_vector_count_sigma":0.0,"read_vector_max":0,"read_vector_min":0,"read_vector_operations":0,"read_vector_sigma":0.0,"remote_access":false,"server_domain":"cern.ch","server_host":"p05799459u51457","server_username":"","start_time":1491789025000,"throughput":274307.57826086954,"unique_id":"03404bbc-1d90-11e7-9717-47f48e80beef-2e48","user":"","user_dn":"","user_fqan":"","user_role":"","vo":"","write_average":0.0,"write_bytes":0,"write_bytes_at_close":0,"write_max":0,"write_min":0,"write_operations":0,"write_sigma":0.0},"metadata":{"event_timestamp":1491789715000,"hostname":"monit-amqsource-fafa51de8d.cern.ch","kafka_timestamp":1491789741627,"original-destination":"/topic/xrootd.cms.eos","partition":"10","producer":"xrootd","timestamp":1491789740015,"topic":"xrootd_raw_gled","type":"gled","type_prefix":"raw","version":"003"}}

    :returns: a dictionary with AAA Spark DataFrame
    """
    if not date:
        # by default we read yesterdate data
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))

    hpath = '%s/%s' % (hdir, date)
    try:
        rdd = union_all([sqlContext.read.json(path) for path in files(hpath, verbose)])
    except Exception:
        rdd = union_all([sqlContext.jsonFile(path) for path in files(hpath, verbose)])
    aaa_rdd = rdd.map(lambda r: r['data'])
    records = aaa_rdd.take(1)  # take function will return list of records
    if verbose:
        print("### aaa_rdd records", records, type(records))

    # create new spark DataFrame
    aaa_df = sqlContext.createDataFrame(aaa_rdd)
    aaa_df.registerTempTable('aaa_df')
    tables = {'aaa_df': aaa_df}
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
    if not date:
        # by default we read yesterdate data
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))

    hpath = '%s/%s' % (hdir, date)
    cols = ['data.src_experiment_site', 'data.user_dn', 'data.file_lfn']

    files_in_hpath = files(hpath, verbose)

    if len(files_in_hpath) == 0:
        aaa_df = sqlContext.createDataFrame([], schema=schema_empty_aaa())
    else:
        try:
            aaa_df = union_all([sqlContext.read.json(path) for path in files_in_hpath], cols)
        except Exception:
            aaa_df = union_all([sqlContext.jsonFile(path) for path in files_in_hpath], cols)

    aaa_df.registerTempTable('aaa_df')
    tables = {'aaa_df': aaa_df}
    return tables


def eos_tables(sqlContext,
               #        hdir='hdfs:///project/monitoring/archive/eos/logs/reports/cms', #before 2020
               hdir='hdfs:///project/monitoring/archive/eos-report/logs/cms',  # after 2020
               date=None, start_date=None, end_date=None, verbose=False):
    """
    Parse EOS HDFS records. This data set comes from EOS servers at CERN. Data
    is send directly by the EOS team, reading the EOS logs and sending them
    into the MONIT infrastructure.
    
    Use https://twiki.cern.ch/twiki/bin/view/ITAnalyticsWorkingGroup/EosFileAccessLogs as data dictionary. 

    Example of EOS JSON record on HDFS
    {"data":"\"log=9e7436fe-1d8e-11e7-ba07-a0369f1fbf0c&path=/store/mc/PhaseISpring17GS/MinBias_TuneCUETP8M1_13TeV-pythia8/GEN-SIM/90X_upgrade2017_realistic_v20-v1/50000/72C78841-2110-E711-867F-F832E4CC4D39.root&ruid=8959&rgid=1399&td=nobody.693038:472@fu-c2e05-24-03-daq2fus1v0--cms&host=p05798818q44165.cern.ch&lid=1048850&fid=553521212&fsid=18722&ots=1491788403&otms=918&cts=1491789688&ctms=225&rb=19186114&rb_min=104&rb_max=524288&rb_sigma=239596.05&wb=0&wb_min=0&wb_max=0&wb_sigma=0.00&sfwdb=7576183815&sbwdb=6313410471&sxlfwdb=7575971197&sxlbwdb=6313300667&nrc=72&nwc=0&nfwds=24&nbwds=10&nxlfwds=12&nxlbwds=4&rt=9130.44&wt=0.00&osize=3850577700&csize=3850577700&sec.prot=gsi&sec.name=cmsprd&sec.host=cms-ucsrv-c2f46-32-07.cern.ch&sec.vorg=&sec.grps=&sec.role=&sec.info=/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=amaltaro/CN=718748/CN=Alan Malta Rodrigues&sec.app=\"","metadata":{"host":"eoscms-srv-m1.cern.ch","kafka_timestamp":1491789692305,"partition":"14","path":"cms","producer":"eos","timestamp":1491789689562,"topic":"eos_logs","type":"reports","type_prefix":"logs"}}
    # in 2019 we have the following structure
    {"data":{"eos.csize":"49834","eos.ctms":"177","eos.cts":"1548009771","eos.fid":"1282182923","eos.fsid":"8953","eos.fstpath":"/data08/0001f4da/4c6c8f0b","eos.host":"lxfsrf65c02.cern.ch","eos.lid":"1048850","eos.log":"30a723b0-1ce3-11e9-b49d-a0369f1fba7c","eos.nbwds":"0","eos.nfwds":"0","eos.nrc":"0","eos.nwc":"3","eos.nxlbwds":"0","eos.nxlfwds":"0","eos.osize":"0","eos.otms":"109","eos.ots":"1548009771","eos.path":"/eos/cms/store/unified/www/joblogs/vlimant_ACDC0_task_SUS-RunIISummer16FSPremix-00090__v1_T_190118_174333_8486/139/SUS-RunIISummer16FSPremix-00090_0/f585f0eb-1e6b-4722-8266-5254e7975115-72-1-logArchive/job/WMTaskSpace/cmsRun1/lheevent/process/madevent/SubProcesses/P3_gq_urdlxgq/G168.08/run1_app.log","eos.rb":"0","eos.rb_max":"0","eos.rb_min":"0","eos.rb_sigma":"0.00","eos.rc_max":"0","eos.rc_min":"0","eos.rc_sigma":"0.00","eos.rc_sum":"0","eos.rgid":"1399","eos.rs_op":"0","eos.rsb_max":"0","eos.rsb_min":"0","eos.rsb_sigma":"0.00","eos.rsb_sum":"0","eos.rt":"0.00","eos.ruid":"103074","eos.rv_op":"0","eos.rvb_max":"0","eos.rvb_min":"0","eos.rvb_sigma":"0.00","eos.rvb_sum":"0","eos.rvt":"0.00","eos.sbwdb":"0","eos.sec.app":"fuse","eos.sec.host":"vocms0268.ipv6.cern.ch","eos.sec.name":"vlimant","eos.sec.prot":"krb5","eos.sfwdb":"0","eos.sxlbwdb":"0","eos.sxlfwdb":"0","eos.td":"daemon.23360:250@lxfsre50c03","eos.wb":"0","eos.wb_max":"0","eos.wb_min":"0","eos.wb_sigma":"0.00","eos.wt":"0.07","raw":"log=30a723b0-1ce3-11e9-b49d-a0369f1fba7c&path=/eos/cms/store/unified/www/joblogs/vlimant_ACDC0_task_SUS-RunIISummer16FSPremix-00090__v1_T_190118_174333_8486/139/SUS-RunIISummer16FSPremix-00090_0/f585f0eb-1e6b-4722-8266-5254e7975115-72-1-logArchive/job/WMTaskSpace/cmsRun1/lheevent/process/madevent/SubProcesses/P3_gq_urdlxgq/G168.08/run1_app.log&fstpath=/data08/0001f4da/4c6c8f0b&ruid=103074&rgid=1399&td=daemon.23360:250@lxfsre50c03&host=lxfsrf65c02.cern.ch&lid=1048850&fid=1282182923&fsid=8953&ots=1548009771&otms=109&cts=1548009771&ctms=177&nrc=0&nwc=3&rb=0&rb_min=0&rb_max=0&rb_sigma=0.00&rv_op=0&rvb_min=0&rvb_max=0&rvb_sum=0&rvb_sigma=0.00&rs_op=0&rsb_min=0&rsb_max=0&rsb_sum=0&rsb_sigma=0.00&rc_min=0&rc_max=0&rc_sum=0&rc_sigma=0.00&wb=0&wb_min=0&wb_max=0&wb_sigma=0.00&sfwdb=0&sbwdb=0&sxlfwdb=0&sxlbwdb=0&nfwds=0&nbwds=0&nxlfwds=0&nxlbwds=0&rt=0.00&rvt=0.00&wt=0.07&osize=0&csize=49834&sec.prot=krb5&sec.name=vlimant&sec.host=vocms0268.ipv6.cern.ch&sec.vorg=&sec.grps=&sec.role=&sec.info=&sec.app=fuse","real_timestamp":"1548009771000"},"metadata":{"_id":"e2a22c6d-77b3-dd33-d5d8-3357c1988e49","host":"eoscms-srv-m1.cern.ch","json":"true","kafka_timestamp":1548009773721,"partition":"19","path":"cms","producer":"eos","timestamp":1548009771000,"topic":"eos_logs","type":"reports","type_prefix":"logs"}}
    # in 2019 we have the following structure, version II after I reported eos prefix issue
    {"data":{"csize":"853699786","ctms":"939","cts":"1549043141","eos_host":"p06253937y92607.cern.ch","eos_path":"/eos/cms/store/cmst3/group/wmass/w-helicity-13TeV/trees/TREES_electrons_1l_V6_TINY/friends/tree_Friend_WJetsToLNu_NLO_part1.root","fid":"1126583180","fsid":"20546","fstpath":"/data07/0001b812/43264b8c","lid":"1048850","log":"287544b4-2649-11e9-abe8-a0369f1fba7c","nbwds":"145","nfwds":"195","nrc":"479","nwc":"0","nxlbwds":"144","nxlfwds":"143","osize":"853699786","otms":"479","ots":"1549043126","prot":"krb5","raw":"log=287544b4-2649-11e9-abe8-a0369f1fba7c&path=/eos/cms/store/cmst3/group/wmass/w-helicity-13TeV/trees/TREES_electrons_1l_V6_TINY/friends/tree_Friend_WJetsToLNu_NLO_part1.root&fstpath=/data07/0001b812/43264b8c&ruid=24421&rgid=1399&td=emanuele.81:510@b6644a93b5&host=p06253937y92607.cern.ch&lid=1048850&fid=1126583180&fsid=20546&ots=1549043126&otms=479&cts=1549043141&ctms=939&nrc=479&nwc=0&rb=41276444&rb_min=294&rb_max=326196&rb_sigma=101487.35&rv_op=0&rvb_min=0&rvb_max=0&rvb_sum=0&rvb_sigma=0.00&rs_op=0&rsb_min=0&rsb_max=0&rsb_sum=0&rsb_sigma=0.00&rc_min=0&rc_max=0&rc_sum=0&rc_sigma=0.00&wb=0&wb_min=0&wb_max=0&wb_sigma=0.00&sfwdb=3019121271&sbwdb=2233253538&sxlfwdb=3017497429&sxlbwdb=2233253338&nfwds=195&nbwds=145&nxlfwds=143&nxlbwds=144&rt=48.11&rvt=0.00&wt=0.00&osize=853699786&csize=853699786&sec.prot=krb5&sec.name=emanuele&sec.host=b6644a93b5.cern.ch&sec.vorg=&sec.grps=&sec.role=&sec.info=&sec.app=","rb":"41276444","rb_max":"326196","rb_min":"294","rb_sigma":"101487.35","rc_max":"0","rc_min":"0","rc_sigma":"0.00","rc_sum":"0","real_timestamp":"1549043126000","rgid":"1399","rs_op":"0","rsb_max":"0","rsb_min":"0","rsb_sigma":"0.00","rsb_sum":"0","rt":"48.11","ruid":"24421","rv_op":"0","rvb_max":"0","rvb_min":"0","rvb_sigma":"0.00","rvb_sum":"0","rvt":"0.00","sbwdb":"2233253538","sec_host":"b6644a93b5.cern.ch","sec_name":"emanuele","sfwdb":"3019121271","sxlbwdb":"2233253338","sxlfwdb":"3017497429","td":"emanuele.81:510@b6644a93b5","wb":"0","wb_max":"0","wb_min":"0","wb_sigma":"0.00","wt":"0.00"},"metadata":{"_id":"a3b00488-552e-35a2-42f0-c52112655e15","host":"eoscms-srv-b2.cern.ch","json":"true","kafka_timestamp":1549043144145,"partition":"10","path":"cms","producer":"eos","timestamp":1549043126000,"topic":"eos_logs","type":"reports","type_prefix":"logs"}}

    The EOS record consist of data and metadata parts where data part squashed
    into single string all requested parameters.

    :returns: a dictionary with eos Spark DataFrame
    """
    if not date:
        if start_date:
            if not end_date:
                end_date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))
            _sd = dt.strptime(start_date, "%Y/%m/%d")
            _ed = dt.strptime(end_date, "%Y/%m/%d")
            dates = ','.join([(_sd + timedelta(days=x)).strftime("%Y/%m/%d") for x in range(0, (_ed - _sd).days + 1)])
            date = '{{{}}}'.format(dates)
        else:
            # by default we read yesterdate data
            date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))

    hpath = '%s/%s/part*' % (hdir, date)
    cols = ['data', 'metadata.timestamp']
    # sqlContex can be either a SQLContext instance (for older spark/code) 
    # or a SparkSession for the newer(post spark 2.2) code. In most cases it works similar, 
    # but to get the SparkContext we need to threat it different:
    files_in_hpath = glob_files(
        sqlContext.sparkSession.sparkContext
        if isinstance(sqlContext, SQLContext)
        else sqlContext.sparkContext,
        hpath
    )

    if len(files_in_hpath) == 0:
        eos_df = sqlContext.createDataFrame([], schema=schema_empty_eos())
        eos_df.registerTempTable('eos_df')
        tables = {'eos_df': eos_df}
        return tables

    # in Spark 2.X and 2019 we have different record
    # Sampling ratio, if there is more than one file we can take the 10%,
    # but if there is only one file it is probable that it have less than 10 records 
    # (making the samplig size 0, which make the process fail)
    edf = sqlContext.read.option("basePath", hdir).option("samplingRatio", 0.1 if len(files_in_hpath) > 1 else 1).json(
        files_in_hpath)

    # select columns
    edf = edf.selectExpr(
        'metadata.timestamp',
        'data.rb_max',
        'data.td',
        'data.path',
        'data.`sec.app`',
        'data.rt',
        'data.wt',
        'data.rb',
        'data.wb',
        'data.cts',
        'data.csize',
        'data.`sec.name`',
        'data.`sec.info`')

    # backward compatibility: 
    # path -> file_lfn, sec.name -> user, sec.info -> user_dn, td -> session, sec.app -> application
    # add day based on timestamp
    eos_df = edf.withColumnRenamed("path", "file_lfn") \
        .withColumnRenamed("sec.name", "user") \
        .withColumnRenamed("sec.info", "user_dn") \
        .withColumnRenamed("sec.app", "application") \
        .withColumnRenamed("td", "session") \
        .withColumn('day', date_format(from_unixtime(edf.timestamp / 1000), 'yyyyMMdd'))

    # OLD ---before 2020
    #    f_data = 'data as raw' if str(edf.schema['data'].dataType) == 'StringType' else 'data.raw'
    #    edf = edf.selectExpr(f_data,'metadata.timestamp')

    # At this moment, json files can have one of two known schemas. In order to read several days we need to be able to work with both of them. 
    # eos_df = edf.select(data.getField("eos.path").alias("file_lfn"), data.getField("eos.sec.info").alias("user_dn"), data.getField("eos.sec.app").alias("application"), data.getField("eos.sec.host").alias("host"), edf.metadata.getField("timestamp").alias("timestamp"))
    # eos_df = edf.select(eos_path, data.getField("sec_info").alias("user_dn"), data.getField("sec_app").alias("application"), data.getField("eos_host").alias("host"), edf.metadata.getField("timestamp").alias("timestamp"))
    # We can use the raw field because it doesn't change on time  
    #   eos_df = edf\
    #        .withColumn('rb_max', regexp_extract(edf.raw,'&rb_max=([^&\']*)',1).cast('long'))\
    #        .withColumn('session', regexp_extract(edf.raw,'&td=([^&\']*)',1))\
    #        .withColumn('file_lfn', regexp_extract(edf.raw,'&path=([^&]*)',1))\
    #        .withColumn('application', regexp_extract(edf.raw,'&sec.app=([^&\']*)',1))\
    #        .withColumn('rt', regexp_extract(edf.raw,'&rt=([^&\']*)',1).cast('long'))\
    #        .withColumn('wt', regexp_extract(edf.raw,'&wt=([^&\']*)',1).cast('long'))\
    #        .withColumn('rb', regexp_extract(edf.raw,'&rb=([^&\']*)',1).cast('long'))\
    #        .withColumn('wb', regexp_extract(edf.raw,'&wb=([^&\']*)',1).cast('long'))\
    #        .withColumn('cts', regexp_extract(edf.raw,'&cts=([^&\']*)',1).cast('long'))\
    #        .withColumn('csize', regexp_extract(edf.raw,'&csize=([^&\']*)',1).cast('long'))\
    #        .withColumn('user', regexp_extract(edf.raw,'&sec.name=([^&\']*)',1))\
    #        .withColumn('user_dn', regexp_extract(edf.raw,'&sec.info=([^&\']*)',1))\
    #        .withColumn('day', date_format(from_unixtime(edf.timestamp/1000),'yyyyMMdd'))

    if verbose:
        eos_df.printSchema()
        records = eos_df.take(1)  # take function will return list of records
        print("### rdd records", records, type(records))

    if verbose:
        records = eos_df.take(1)  # take function will return list of records
        print("### eos_rdd records", records, type(records))

    # create new spark DataFrame
    eos_df.registerTempTable('eos_df')
    tables = {'eos_df': eos_df}
    return tables


def condor_tables(sqlContext,
                  hdir='hdfs:///project/monitoring/archive/condor/raw/metric',
                  date=None, verbose=False):
    """
    Parse HTCondor records

    Example of HTCondor recornd on HDFS
    {"data":{"AccountingGroup":"analysis.wverbeke","Badput":0.0,"CMSGroups":"[\"/cms\"]","CMSPrimaryDataTier":"MINIAODSIM","CMSPrimaryPrimaryDataset":"TTWJetsToLNu_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8","CMSPrimaryProcessedDataset":"RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext2-v1","CRAB_AsyncDest":"T2_BE_IIHE","CRAB_DataBlock":"/TTWJetsToLNu_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext2-v1/MINIAODSIM#291c85fa-aab1-11e6-846b-02163e0184a6","CRAB_ISB":"https://cmsweb.cern.ch/crabcache","CRAB_Id":30,"CRAB_JobArch":"slc6_amd64_gcc530","CRAB_JobSW":"CMSSW_9_2_4","CRAB_JobType":"analysis","CRAB_OutLFNDir":"/store/user/wverbeke/heavyNeutrino/TTWJetsToLNu_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/crab_Moriond2017_ext2-v1_ewkinoMCList-v7p1/171111_214448","CRAB_PrimaryDataset":"TTWJetsToLNu_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8","CRAB_Publish":false,"CRAB_PublishName":"crab_Moriond2017_ext2-v1_ewkinoMCList-v7p1-00000000000000000000000000000000","CRAB_Retry":0,"CRAB_SaveLogsFlag":true,"CRAB_SiteBlacklist":"[]","CRAB_SiteWhitelist":"[]","CRAB_SubmitterIpAddr":"193.58.172.33","CRAB_TaskEndTime":1513028688,"CRAB_TaskLifetimeDays":30,"CRAB_TaskWorker":"vocms052","CRAB_TransferOutputs":true,"CRAB_UserHN":"wverbeke","CRAB_Workflow":"171111_214448:wverbeke_crab_Moriond2017_ext2-v1_ewkinoMCList-v7p1","Campaign":"crab_wverbeke","ClusterId":20752288,"Cmd":"/data/srv/glidecondor/condor_local/spool/2259/0/cluster20752259.proc0.subproc0/gWMS-CMSRunAnalysis.sh","CommittedCoreHr":0.0,"CommittedSlotTime":0,"CommittedSuspensionTime":0,"CommittedTime":0,"CommittedWallClockHr":0.0,"CoreHr":0.0,"CoreSize":-1,"Country":"Unknown","CpuBadput":0.0,"CpuEff":0.0,"CpuTimeHr":0.0,"CumulativeRemoteSysCpu":0.0,"CumulativeRemoteUserCpu":0.0,"CumulativeSlotTime":0,"CumulativeSuspensionTime":0,"CurrentHosts":0,"DAGNodeName":"Job30","DAGParentNodeNames":"","DESIRED_Archs":"X86_64","DESIRED_CMSDataLocations":"T2_FR_IPHC,T2_CH_CERN_HLT,T1_ES_PIC,T2_DE_DESY,T2_BE_IIHE,T2_CH_CERN,T2_ES_IFCA","DESIRED_CMSDataset":"/TTWJetsToLNu_TuneCUETP8M1_13TeV-amcatnloFXFX-madspin-pythia8/RunIISummer16MiniAODv2-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6_ext2-v1/MINIAODSIM","DESIRED_Overflow_Region":"none,none,none","DESIRED_Sites":["T2_FR_IPHC","T2_CH_CERN_HLT","T1_ES_PIC","T2_DE_DESY","T2_BE_IIHE","T2_CH_CERN","T2_ES_IFCA"],"DataCollection":1510475761000,"DataCollectionDate":1510475761000,"DataLocations":["T2_FR_IPHC","T2_CH_CERN_HLT","T1_ES_PIC","T2_DE_DESY","T2_BE_IIHE","T2_CH_CERN","T2_ES_IFCA"],"DataLocationsCount":7,"DesiredSiteCount":7,"DiskUsage":5032,"DiskUsageGB":0.005032,"EncryptExecuteDirectory":false,"EnteredCurrentStatus":1510436775000,"EstimatedWallTimeMins":1250,"ExecutableSize":9,"ExitBySignal":false,"ExitStatus":0,"GLIDEIN_CMSSite":"Unknown","GlobalJobId":"crab3@vocms0122.cern.ch#20752288.0#1510436775","HasSingularity":false,"ImageSize":9,"JOB_CMSSite":"$$(GLIDEIN_CMSSite:Unknown)","JOB_Gatekeeper":"Unknown","JobBatchName":"RunJobs.dag+20752259","JobPrio":10,"JobStatus":1,"JobUniverse":5,"MaxHosts":1,"MaxWallTimeMins":1250,"MemoryMB":0.0,"MinHosts":1,"NumJobCompletions":0,"NumJobStarts":0,"NumRestarts":0,"NumSystemHolds":0,"OVERFLOW_CHECK":false,"Original_DESIRED_Sites":["UNKNOWN"],"OutputFiles":2,"Owner":"cms1315","PostJobPrio1":-1510436758,"PostJobPrio2":0,"PreJobPrio1":1,"ProcId":0,"QDate":1510436775000,"QueueHrs":10.951667712198363,"REQUIRED_OS":"rhel6","Rank":0,"RecordTime":1510475761000,"RemoteSysCpu":0,"RemoteUserCpu":0,"RemoteWallClockTime":0,"RequestCpus":1,"RequestDisk":1,"RequestMemory":2000,"ScheddName":"crab3@vocms0122.cern.ch","ShouldTransferFiles":"YES","Site":"Unknown","SpoolOnEvict":false,"Status":"Idle","TaskType":"Analysis","Tier":"Unknown","TotalSubmitProcs":1,"TotalSuspensions":0,"TransferInputSizeMB":4,"Type":"analysis","Universe":"Vanilla","User":"cms1315@cms","VO":"cms","WMAgent_TaskType":"UNKNOWN","WallClockHr":0.0,"WhenToTransferOutput":"ON_EXIT_OR_EVICT","Workflow":"wverbeke_crab_Moriond2017_ext2-v1_ewkinoMCList-v7p1","metadata":{"id":"crab3@vocms0122.cern.ch#20752288.0#1510436775","timestamp":1510476202,"uuid":"8aa4b4fe-c785-11e7-ad57-fa163e15539a"},"x509UserProxyEmail":"Willem.Verbeke@UGent.be","x509UserProxyFQAN":["/DC=org/DC=terena/DC=tcs/C=BE/O=Universiteit Gent/CN=Willem Verbeke wlverbek@UGent.be","/cms/Role=NULL/Capability=NULL"],"x509UserProxyFirstFQAN":"/cms/Role=NULL/Capability=NULL","x509UserProxyVOName":"cms","x509userproxysubject":"/DC=org/DC=terena/DC=tcs/C=BE/O=Universiteit Gent/CN=Willem Verbeke wlverbek@UGent.be"},"metadata":{"_id":"380721bc-a12c-9b43-b545-740c10d2d0f0","hostname":"monit-amqsource-fafa51de8d.cern.ch","kafka_timestamp":1510476204057,"partition":"1","producer":"condor","timestamp":1510476204022,"topic":"condor_raw_metric","type":"metric","type_prefix":"raw","version":"001"}}
    """
    if not date:
        # by default we read yesterdate data
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))

    hpath = '%s/%s' % (hdir, date)

    # create new spark DataFrame
    condor_df = sqlContext.read.json(hpath)
    condor_df.registerTempTable('condor_df')
    #    condor_df = condor_df.select(unpack_struct("data", condor_df)) # extract data part of JSON records
    condor_df.printSchema()
    tables = {'condor_df': condor_df}
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
    if not date:
        # by default we read yesterdate data
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))

    hpath = '%s/%s' % (hdir, date)

    # create new spark DataFrame
    fts_df = sqlContext.read.json(hpath)
    fts_df.registerTempTable('fts_df')
    fts_df = fts_df.select(unpack_struct("data", fts_df))  # extract data part of JSON records
    fts_df.printSchema()
    tables = {'fts_df': fts_df}
    return tables


def split_dataset(df, dcol):
    """Split dataset name in DataFrame into primds,procds,tier components"""
    ndf = df.withColumn("primds", split(col(dcol), "/").alias('primds').getItem(1)) \
        .withColumn("procds", split(col(dcol), "/").alias('procds').getItem(2)) \
        .withColumn("tier", split(col(dcol), "/").alias('tier').getItem(3)) \
        .drop(dcol)
    return ndf


def unpack_struct(colname, df):
    """Unpack structure and extract specific column from dataframe"""
    parent = list(filter(lambda field: field.name == colname, df.schema.fields))[0]
    fields = parent.dataType.fields if isinstance(parent.dataType, StructType) else []
    return list(map(lambda x: col(colname + "." + x.name), fields))


def aso_tables(sqlContext, hdir='hdfs:///project/awg/cms', verbose=False):
    """
    Parse ASO records on HDFS via mapping ASO tables to Spark SQLContext, data comes from
    https://gitlab.cern.ch/awg/awg-ETL-crons/blob/master/sqoop/cms-aso.sh

    :returns: a dictionary with ASO Spark DataFrame.
    """
    adir = hdir + '/CMS_ASO/filetransfersdb/merged'

    # aso data
    pfiles = os.popen("hadoop fs -ls %s | grep part | awk '{print $8}'" % adir).read().splitlines()
    msg = "ASO snapshot found %d directories" % len(pfiles)
    print(msg)
    aso_df = union_all([sqlContext.read.format('com.databricks.spark.csv')
                       .options(treatEmptyValuesAsNulls='true', nullValue='null')
                       .load(file_path, schema=schema_asodb())
                        for file_path in pfiles])

    # Register temporary tables to be able to use sqlContext.sql
    aso_df.registerTempTable('aso_df')

    tables = {'aso_df': aso_df}
    return tables


def get_candidate_files(start_date, end_date, spark, base, day_delta=1):
    """Returns a list of hdfs folders that can contain data for the given dates.
    """
    st_date = start_date - timedelta(days=day_delta)
    ed_date = end_date + timedelta(days=day_delta)
    days = (ed_date - st_date).days

    # pre_candidate_files = [
    #     "{base}/{day}{{,.tmp}}".format(
    #         base=base, day=(st_date + timedelta(days=i)).strftime("%Y/%m/%d")
    #     )
    #     for i in range(0, days)
    # ]

    sc = spark.sparkContext
    # The candidate files are the folders to the specific dates,
    # but if we are looking at recent days the compaction procedure could
    # have not run yet so we will considerate also the .tmp folders.
    candidate_files = [
        f"{base}/{(st_date + timedelta(days=i)).strftime('%Y/%m/%d')}"
        for i in range(0, days)
    ]
    fsystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    uri = sc._gateway.jvm.java.net.URI
    path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = fsystem.get(uri("hdfs:///"), sc._jsc.hadoopConfiguration())
    candidate_files = [url for url in candidate_files if fs.globStatus(path(url))]
    return candidate_files


def get_spark_session(app_name):
    """Get or create the spark context and session.
    """
    sc = SparkContext(appName=app_name)
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()
