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
from CMSSpark.utils import info_save, bytes_to_readable
from CMSSpark.conf import OptionParser

LEFTOVERS_TIME_DATA_FILE = 'spark_exec_time_leftovers.txt'

def get_options():
    opts = OptionParser('leftovers')

    opts.parser.add_argument("--inst", action="store",
        dest="inst", default="global")

    return opts.parser.parse_args()

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_destination_dir():
    return '%s/../../../bash/report_leftovers' % get_script_dir()

def quiet_logs(sc):
    """
    Sets logger's level to ERROR so INFO logs would not show up.
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def run(fout, date, yarn=None, verbose=None, inst='GLOBAL'):
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

    instances = [inst]
    if inst == 'all':
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

    # d_dataset_id, d_dataset, dataset_access_type
    dbs_df = ddf_df.join(daf_df, ddf_df.d_dataset_access_type_id == daf_df.dataset_access_type_id)\
                   .drop(ddf_df.d_dataset_access_type_id)\
                   .drop(daf_df.dataset_access_type_id)

    # dataset, dbs_size
    dbs_df = dbs_df.where(dbs_df.dataset_access_type == 'VALID')\
                   .join(fdf_df, dbs_df.d_dataset_id == fdf_df.f_dataset_id)\
                   .withColumnRenamed('d_dataset', 'dataset')\
                   .withColumnRenamed('f_file_size', 'dbs_size')\
                   .drop(dbs_df.d_dataset_id)\
                   .drop(fdf_df.f_dataset_id)\
                   .drop(dbs_df.dataset_access_type)

    # dataset, dbs_size
    dbs_df = dbs_df.groupBy(['dataset'])\
                   .agg({'dbs_size':'sum'})\
                   .withColumnRenamed('sum(dbs_size)', 'dbs_size')

    # dataset, site, phedex_size
    phedex_cols = ['dataset_name', 'block_bytes', 'node_name']
    phedex_df = phedex.select(phedex_cols)
    phedex_df = phedex_df.withColumnRenamed('block_bytes', 'phedex_size')\
                         .withColumnRenamed('dataset_name', 'dataset')\
                         .withColumnRenamed('node_name', 'site')

    # dataset, sites, phedex_size
    phedex_df = phedex_df.groupBy(['dataset'])\
                   .agg({'phedex_size':'sum', 'site': 'collect_set'})\
                   .withColumnRenamed('sum(phedex_size)', 'phedex_size')\
                   .withColumnRenamed('collect_set(site)', 'sites')

    # Subtract to get leftovers

    extract_campaign_udf = udf(lambda dataset: dataset.split('/')[2])

    # dataset
    leftover_datasets_df = phedex_df.select('dataset').subtract(dbs_df.select('dataset'))

    # dataset, campaign, sites, phedex_size
    leftovers_df = leftover_datasets_df.select('dataset').join(phedex_df, 'dataset')#.join(dbs_df, 'dataset')
    leftovers_df = leftovers_df.withColumn('campaign', extract_campaign_udf(leftovers_df.dataset))\
                               .select(['dataset', 'campaign', 'sites', 'phedex_size'])

    # Subtract to get leftovers that don't even exist in DBS (orphans)

    ddf_datasets_df = ddf_df.withColumnRenamed('d_dataset', 'dataset').select('dataset')
    leftover_datasets_df = phedex_df.select('dataset').subtract(ddf_datasets_df)

    # dataset, campaign, sites, phedex_size
    leftovers_orphans_df = leftover_datasets_df.select('dataset').join(phedex_df, 'dataset')#.join(dbs_df, 'dataset')
    leftovers_orphans_df = leftovers_orphans_df.withColumn('campaign', extract_campaign_udf(leftovers_orphans_df.dataset))\
                                               .select(['dataset', 'campaign', 'sites', 'phedex_size'])

    # Sum total size of leftovers
    all_leftovers_size = leftovers_df.select('phedex_size').groupBy().sum().rdd.map(lambda x: x[0]).collect()[0]
    orphan_leftovers_size = leftovers_orphans_df.select('phedex_size').groupBy().sum().rdd.map(lambda x: x[0]).collect()[0]

    print 'All leftovers PhEDEx size: %s' % bytes_to_readable(all_leftovers_size)
    print 'Orphan leftovers PhEDEx size: %s' % bytes_to_readable(orphan_leftovers_size)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        leftovers_df.write.format("com.databricks.spark.csv")\
                          .option("header", "true").save('%s/all' % fout)

        leftovers_orphans_df.write.format("com.databricks.spark.csv")\
                            .option("header", "true").save('%s/orphans' % fout)

    ctx.stop()

@info_save('%s/%s' % (get_destination_dir(), LEFTOVERS_TIME_DATA_FILE))
def main():
    "Main function"
    opts = get_options()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    fout = opts.fout
    date = opts.date
    verbose = opts.verbose
    yarn = opts.yarn
    inst = opts.inst
    if inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    elif inst != 'all':
        raise Exception('Unsupported DBS instance "%s"' % inst)
    
    run(fout, date, yarn, verbose, inst)

if __name__ == '__main__':
    main()
