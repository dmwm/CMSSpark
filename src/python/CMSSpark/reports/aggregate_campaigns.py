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
from pyspark.sql.functions import struct, array, udf, countDistinct
from pyspark.sql.types import IntegerType, LongType, StringType, StructType, StructField

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

def extract_campaign(dataset):
    return dataset.split('/')[2]

def quiet_logs(sc):
    """
    Sets logger's level to ERROR so INFO logs would not show up.
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def get_mss(row):
    sorted_values = sorted([x for x in row if x != None], reverse=True)
    return sorted_values[0]

def get_second_mss(row):
    sorted_values = sorted([x for x in row if x != None], reverse=True)
    return sorted_values[1] if len(sorted_values) > 1 else None
    
def get_mss_name(row, sites_columns):
    list_of_values = [x for x in row]
    tuples = zip(list_of_values, sites_columns)
    tuples = sorted([x for x in tuples if x[0] != None], key=lambda x: x[0], reverse=True)
    return tuples[0][1]

def get_second_mss_name(row, sites_columns):
    list_of_values = [x for x in row]
    tuples = zip(list_of_values, sites_columns)
    tuples = sorted([x for x in tuples if x[0] != None], key=lambda x: x[0], reverse=True)
    return tuples[1][1] if len(tuples) > 1 else None

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

    # read Phedex and DBS tables
    tables = {}

    tables.update(phedex_tables(sqlContext, verbose=verbose, fromdate=fromdate, todate=todate))
    phedex = tables['phedex_df']

    instances = ['GLOBAL', 'PHYS01', 'PHYS02', 'PHYS03']
    for instance in instances:
        dbs_dict = dbs_tables(sqlContext, inst=instance, verbose=verbose)
        for key, val in dbs_dict.items():
            new_key = '%s_%s' % (key, instance)
            tables[new_key] = val
    
    daf = reduce(lambda a,b: a.unionAll(b), [tables['daf_%s' % x] for x in instances])
    ddf = reduce(lambda a,b: a.unionAll(b), [tables['ddf_%s' % x] for x in instances])
    fdf = reduce(lambda a,b: a.unionAll(b), [tables['fdf_%s' % x] for x in instances])

    dbs_fdf_cols = ['f_dataset_id', 'f_file_size']
    dbs_ddf_cols = ['d_dataset_id', 'd_dataset', 'd_dataset_access_type_id']
    dbs_daf_cols = ['dataset_access_type_id', 'dataset_access_type']

    fdf_df = fdf.select(dbs_fdf_cols)
    ddf_df = ddf.select(dbs_ddf_cols)
    daf_df = daf.select(dbs_daf_cols)

    # Aggregate by campaign and find total PhEDEx and DBS size of each campaign

    extract_campaign_udf = udf(lambda dataset: dataset.split('/')[2])

    # d_dataset_id, d_dataset, dataset_access_type
    dbs_df = ddf_df.join(daf_df, ddf_df.d_dataset_access_type_id == daf_df.dataset_access_type_id)\
                   .drop(ddf_df.d_dataset_access_type_id)\
                   .drop(daf_df.dataset_access_type_id)\
                   .withColumnRenamed('d_dataset_access_type_id', 'dataset_access_type_id')

    # dataset, size
    dbs_df = dbs_df.where(dbs_df.dataset_access_type == 'VALID')\
                   .join(fdf_df, dbs_df.d_dataset_id == fdf_df.f_dataset_id)\
                   .withColumnRenamed('d_dataset', 'dataset')\
                   .withColumnRenamed('f_file_size', 'size')\
                   .drop(dbs_df.d_dataset_id)\
                   .drop(fdf_df.f_dataset_id)\
                   .drop(dbs_df.dataset_access_type)

    # campaign, dbs_size
    dbs_df = dbs_df.withColumn('campaign', extract_campaign_udf(dbs_df.dataset))\
                   .groupBy(['campaign'])\
                   .agg({'size':'sum'})\
                   .withColumnRenamed('sum(size)', 'dbs_size')\
                   .drop('dataset')

    # campaign, phedex_size
    phedex_cols = ['dataset_name', 'block_bytes']
    phedex_df = phedex.select(phedex_cols)
    phedex_df = phedex_df.withColumn('campaign', extract_campaign_udf(phedex_df.dataset_name))\
                .groupBy(['campaign'])\
                .agg({'block_bytes':'sum'})\
                .withColumnRenamed('sum(block_bytes)', 'phedex_size')

    # campaign, dbs_size, phedex_size
    dbs_phedex_df = dbs_df.join(phedex_df, dbs_df.campaign == phedex_df.campaign)\
                          .drop(dbs_df.campaign)

    # Select campaign - site pairs and their sizes (from PhEDEx)

    # campaign, site, size
    phedex_cols = ['dataset_name', 'node_name', 'block_bytes']
    campaign_site_df = phedex.select(phedex_cols)
    campaign_site_df = campaign_site_df.withColumn('campaign', extract_campaign_udf(campaign_site_df.dataset_name))\
                .groupBy(['campaign', 'node_name'])\
                .agg({'block_bytes':'sum'})\
                .withColumnRenamed('sum(block_bytes)', 'size')\
                .withColumnRenamed('node_name', 'site')

    # Aggregate data for site - campaign count table

    # site, count
    site_campaign_count_df = campaign_site_df.groupBy(['site'])\
                                             .agg(countDistinct('campaign'))\
                                             .withColumnRenamed('count(campaign)', 'campaign_count')\
                                             .orderBy('campaign_count', ascending=False)\
                                             .limit(LIMIT)

    # Find two most significant sites for each campaign

    columns_before_pivot = campaign_site_df.columns

    result = campaign_site_df.groupBy(['campaign'])\
                             .pivot('site')\
                             .sum('size')\
                             .na.fill(0)
    
    columns_after_pivot = result.columns
    sites_columns = [x for x in columns_after_pivot if x not in columns_before_pivot]

    number_of_sites_udf = udf(lambda row: len([x for x in row if x != 0]), IntegerType())
    mss_udf = udf(get_mss, LongType())
    second_mss_udf = udf(get_second_mss, LongType())
    mss_name_udf = udf(lambda row: get_mss_name(row, sites_columns), StringType())
    second_mss_name_udf = udf(lambda row: get_second_mss_name(row, sites_columns), StringType())

    result = result.withColumn('sites', number_of_sites_udf(struct([result[x] for x in sites_columns])))\
                   .withColumn('mss', mss_udf(struct([result[x] for x in sites_columns])))\
                   .withColumn('mss_name', mss_name_udf(struct([result[x] for x in sites_columns])))\
                   .withColumn('second_mss', second_mss_udf(struct([result[x] for x in sites_columns])))\
                   .withColumn('second_mss_name', second_mss_name_udf(struct([result[x] for x in sites_columns])))

    # campaign, phedex_size, dbs_size, mss, mss_name, second_mss, second_mss_name, sites
    result = result.join(dbs_phedex_df, result.campaign == dbs_phedex_df.campaign)\
                   .drop(result.campaign)

    sorted_by_phedex = result.orderBy(result.phedex_size, ascending=False).limit(LIMIT)
    sorted_by_dbs = result.orderBy(result.dbs_size, ascending=False).limit(LIMIT)
    
    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        sorted_by_phedex.write.format("com.databricks.spark.csv")\
                              .option("header", "true").save('%s/phedex' % fout)
        
        sorted_by_dbs.write.format("com.databricks.spark.csv")\
                           .option("header", "true").save('%s/dbs' % fout)

        site_campaign_count_df.write.format("com.databricks.spark.csv")\
                              .option("header", "true").save('%s/site_campaign_count' % fout)

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

    with open("spark_exec_time_campaigns.txt", "w") as text_file:
        text_file.write(elapsed_time(time0))

if __name__ == '__main__':
    main()
