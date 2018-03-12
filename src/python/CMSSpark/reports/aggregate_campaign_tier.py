#!/usr/bin/env python
"""
Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
import os
import re
import sys
import time
import json
import argparse
from types import NoneType

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import elapsed_time

LIMIT = 100

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process DBS+PhEDEx metadata"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
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
        msg = 'DBS instance on HDFS: global (default), phys01, phys02, phys03'
        self.parser.add_argument("--inst", action="store",
            dest="inst", default="global", help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select CMSSW data for specific date (YYYYMMDD)')

def quiet_logs(sc):
    """
    Sets logger's level to ERROR so INFO logs would not show up.
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def run(fout, date, yarn=None, verbose=None, patterns=None, antipatterns=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)

    quiet_logs(ctx)

    sqlContext = HiveContext(ctx)
    
    fromdate = '%s-%s-%s' % (date[:4], date[4:6], date[6:])
    todate = fromdate

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    tables.update(phedex_tables(sqlContext, verbose=verbose, fromdate=fromdate, todate=todate))
    phedex = tables['phedex_df']
    
    daf = tables['daf']
    ddf = tables['ddf']
    fdf = tables['fdf']

    # DBS
    dbs_fdf_cols = ['f_dataset_id', 'f_file_size']
    dbs_ddf_cols = ['d_dataset_id', 'd_dataset', 'd_dataset_access_type_id']
    dbs_daf_cols = ['dataset_access_type_id', 'dataset_access_type']

    fdf_df = fdf.select(dbs_fdf_cols)
    ddf_df = ddf.select(dbs_ddf_cols)
    daf_df = daf.select(dbs_daf_cols)
    
    # dataset, dbs_size, dataset_access_type_id
    dbs_df = fdf_df.join(ddf_df, fdf_df.f_dataset_id == ddf_df.d_dataset_id)\
                   .drop('f_dataset_id')\
                   .drop('d_dataset_id')\
                   .withColumnRenamed('d_dataset', 'dataset')\
                   .withColumnRenamed('f_file_size', 'size')\
                   .withColumnRenamed('d_dataset_access_type_id', 'dataset_access_type_id')

    # dataset, size, dataset_access_type
    dbs_df = dbs_df.join(daf_df, dbs_df.dataset_access_type_id == daf_df.dataset_access_type_id)\
                   .drop(dbs_df.dataset_access_type_id)\
                   .drop(daf_df.dataset_access_type_id)

    # dataset, dbs_size
    dbs_df = dbs_df.where(dbs_df.dataset_access_type == 'VALID')\
                   .groupBy('dataset')\
                   .agg({'size':'sum'})\
                   .withColumnRenamed('sum(size)', 'dbs_size')

    # PhEDEx

    size_on_disk_udf = udf(lambda site, size: 0 if site.endswith(('_MSS', '_Buffer', '_Export')) else size)

    # dataset, size, site
    phedex_cols = ['dataset_name', 'block_bytes', 'node_name']
    phedex_df = phedex.select(phedex_cols)\
                      .withColumnRenamed('dataset_name', 'dataset')\
                      .withColumnRenamed('block_bytes', 'size')\
                      .withColumnRenamed('node_name', 'site')
    
    # dataset, phedex_size, size_on_disk
    phedex_df = phedex_df.withColumn('size_on_disk', size_on_disk_udf(phedex_df.site, phedex_df.size))\
                         .groupBy('dataset')\
                         .agg({'size':'sum', 'size_on_disk': 'sum'})\
                         .withColumnRenamed('sum(size)', 'phedex_size')\
                         .withColumnRenamed('sum(size_on_disk)', 'size_on_disk')

    # dataset, dbs_size, phedex_size, size_on_disk
    result = phedex_df.join(dbs_df, phedex_df.dataset == dbs_df.dataset)\
                      .drop(dbs_df.dataset)

    extract_campaign_udf = udf(lambda dataset: dataset.split('/')[2])
    extract_tier_udf = udf(lambda dataset: dataset.split('/')[3])

    # campaign, tier, dbs_size, phedex_size, size_on_disk
    result = result.withColumn('campaign', extract_campaign_udf(result.dataset))\
                   .withColumn('tier', extract_tier_udf(result.dataset))\
                   .drop('dataset')\
                   .groupBy(['campaign', 'tier'])\
                   .agg({'dbs_size':'sum', 'phedex_size': 'sum', 'size_on_disk': 'sum'})\
                   .withColumnRenamed('sum(dbs_size)', 'dbs_size')\
                   .withColumnRenamed('sum(phedex_size)', 'phedex_size')\
                   .withColumnRenamed('sum(size_on_disk)', 'size_on_disk')

    # campaign, tier, dbs_size, phedex_size, size_on_disk
    result = result.withColumn('sum_size', result.dbs_size + result.phedex_size)
    result = result.orderBy(result.sum_size, ascending=False)\
                   .drop('sum_size')\
                   .limit(LIMIT)
    
    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        result.write.format("com.databricks.spark.csv")\
                    .option("header", "true").save(fout)

    ctx.stop()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    fout = opts.fout
    date = opts.date
    verbose = opts.verbose
    yarn = opts.yarn
    inst = opts.inst
    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    patterns = opts.patterns.split(',') if opts.patterns else []
    antipatterns = opts.antipatterns.split(',') if opts.antipatterns else []
    run(fout, date, yarn, verbose, patterns, antipatterns, inst)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s' % elapsed_time(time0))

    with open("spark_exec_time_campaign_tier.txt", "w") as text_file:
        text_file.write(elapsed_time(time0))

if __name__ == '__main__':
    main()
