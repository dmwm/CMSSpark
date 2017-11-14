#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark script to parse DBS and Condor records on HDFS.
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
from CMSSpark.spark_utils import spark_context, condor_tables, split_dataset
from CMSSpark.utils import elapsed_time

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process DBS+AAA metadata"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'condor_datasets.csv'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select CMSSW data for specific date (YYYYMMDD)')
        msg = 'DBS instance on HDFS: global (default), phys01, phys02, phys03'
        self.parser.add_argument("--inst", action="store",
            dest="inst", default="global", help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def condor_date(date):
    "Convert given date into AAA date format"
    if  not date:
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    return '%s/%s/%s' % (year, month, day)

def condor_date_unix(date):
    "Convert AAA date into UNIX timestamp"
    return time.mktime(time.strptime(date, '%Y/%m/%d'))

def run(date, fout, yarn=None, verbose=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    tables.update(phedex_tables(sqlContext, verbose=verbose))
    phedex_df = tables['phedex_df']
    daf = tables['daf'] # dataset access table
    ddf = tables['ddf'] # dataset table
    bdf = tables['bdf'] # block table
    fdf = tables['fdf'] # file table
    aef = tables['aef'] # acquisition era
    pef = tables['pef'] # processing era table
    mcf = tables['mcf'] # output mod config table
    ocf = tables['ocf'] # output module table
    rvf = tables['rvf'] # release version table

    # read Condor rdd
    date = condor_date(date)
    tables.update(condor_tables(sqlContext, date=date, verbose=verbose))
    condor_df = tables['condor_df'] # aaa table

    # aggregate phedex info into dataframe
    phedex_cols = ['node_name', 'dataset_name', 'dataset_is_open', 'block_bytes', 'replica_time_create']
    newpdf = phedex_df.select(phedex_cols).groupBy(['node_name', 'dataset_name', 'dataset_is_open'])\
            .agg({'block_bytes':'sum', 'replica_time_create':'max'})\
            .withColumnRenamed('sum(block_bytes)', 'pbr_size')\
            .withColumnRenamed('max(replica_time_create)', 'max_replica_time')
    newpdf.registerTempTable('newpdf')

    # aggregate dbs info into dataframe
    cols = ['d_dataset_id', 'd_dataset','d_creation_date','d_is_dataset_valid','f_event_count','f_file_size','dataset_access_type','acquisition_era_name','processing_version']
    stmt = 'SELECT %s FROM ddf JOIN fdf on ddf.d_dataset_id = fdf.f_dataset_id JOIN daf ON ddf.d_dataset_access_type_id = daf.dataset_access_type_id JOIN aef ON ddf.d_acquisition_era_id = aef.acquisition_era_id JOIN pef ON ddf.d_processing_era_id = pef.processing_era_id' % ','.join(cols)
    print(stmt)
    joins = sqlContext.sql(stmt)

    # construct conditions
    cond = 'dataset_access_type = "VALID" AND d_is_dataset_valid = 1'
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
    agg_dbs_df = sqlContext.sql(stmt)
    agg_dbs_df.registerTempTable('agg_dbs_df')

    # join dbs and phedex tables
    cols = ['d_dataset','evts','size','date','dataset_access_type','acquisition_era_name','r_release_version','node_name','pbr_size','dataset_is_open','max_replica_time']
    stmt = 'SELECT %s FROM agg_dbs_df JOIN newpdf ON agg_dbs_df.d_dataset = newpdf.dataset_name' % ','.join(cols)
    dbs_phedex_df = sqlContext.sql(stmt)
    dbs_phedex_df.registerTempTable('dbs_phedex_df')

    # merge dbs+phedex and Condor data
#    cols = ['data.DESIRED_CMSDataset', 'data.KEvents', 'data.CMSSWWallHrs']
#    stmt = 'SELECT %s FROM condor_df WHERE condor_df.data.ExitCode=0 AND condor_df.data.KEvents > 0' % ','.join(cols)
    cols = cols + ['data.KEvents', 'data.CMSSWKLumis', 'data.CMSSWWallHrs', 'data.Campaign', 'data.Workflow', 'data.CpuEff', 'data.CoreHr', 'data.QueueHrs', 'data.CRAB_UserHN', 'data.Type']
    stmt = 'SELECT %s FROM condor_df JOIN dbs_phedex_df ON dbs_phedex_df.d_dataset = condor_df.data.DESIRED_CMSDataset WHERE condor_df.data.ExitCode=0 AND condor_df.data.KEvents > 0' % ','.join(cols)
    finel_df = sqlContext.sql(stmt)
    print_rows(finel_df, stmt, verbose)

    # keep table around
    finel_df.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        ndf = split_dataset(finel_df, 'd_dataset')
        ndf.write.format("com.databricks.spark.csv")\
                .option("header", "true").save(fout)

    ctx.stop()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    inst = opts.inst
    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    run(opts.date, opts.fout, opts.yarn, opts.verbose, inst)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
