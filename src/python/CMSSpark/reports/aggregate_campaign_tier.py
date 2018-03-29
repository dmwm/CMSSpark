#!/usr/bin/env python
"""
Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# System modules
import os

# Pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType, DoubleType

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import info_save
from CMSSpark.conf import OptionParser

CAMPAIGN_TIER_TIME_DATA_FILE = 'spark_exec_time_campaign_tier.txt'

def get_options():
    opts = OptionParser('campaign tier')

    opts.parser.add_argument("--inst", action="store",
        dest="inst", default="global")

    opts.parser.add_argument("--limit", type=int,
        dest="limit", default=100)

    return opts.parser.parse_args()

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_destination_dir():
    return '%s/../../../bash/report_campaigns' % get_script_dir()

def quiet_logs(sc):
    """
    Sets logger's level to ERROR so INFO logs would not show up.
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def aaa(a):
    print a
    print type(a)
    return a

def run(fout, date, yarn=None, verbose=None, inst='GLOBAL', limit=100):
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
    # result = result.withColumn('campaign', extract_campaign_udf(result.dataset))\
    #                .withColumn('tier', extract_tier_udf(result.dataset))\
    #                .drop('dataset')\
    #                .groupBy(['campaign', 'tier'])\
    #                .agg({'dbs_size':'sum', 'phedex_size': 'sum', 'size_on_disk': 'sum'})\
    #                .withColumnRenamed('sum(dbs_size)', 'dbs_size')\
    #                .withColumnRenamed('sum(phedex_size)', 'phedex_size')\
    #                .withColumnRenamed('sum(size_on_disk)', 'size_on_disk')

    # campaign, tier, dbs_size, phedex_size, size_on_disk
    # result = result.withColumn('sum_size', result.dbs_size + result.phedex_size)
    # result = result.orderBy(result.sum_size, ascending=False)\
    #                .drop('sum_size')\
    #                .limit(limit)

    # , 'phedex_size': 'sum'
    result = result.withColumn('campaign', extract_campaign_udf(result.dataset))\
                   .withColumn('tier', extract_tier_udf(result.dataset))\
                   .groupBy('campaign')\
                   .agg({'tier': 'collect_list', 'phedex_size': 'collect_list', 'dbs_size': 'collect_list', 'size_on_disk': 'sum'})\
                   .withColumnRenamed('collect_list(tier)', 'tiers_list')\
                   .withColumnRenamed('collect_list(phedex_size)', 'phedex_sizes_list')\
                   .withColumnRenamed('collect_list(dbs_size)', 'dbs_sizes_list')\
                   .withColumnRenamed('sum(size_on_disk)', 'size_on_disk')

    zip_ = udf(lambda tier, dbs, phedex: list(zip(tier, dbs, phedex)), 
    ArrayType(StructType([StructField("_1", StringType()), StructField("_2", DoubleType()), StructField("_3", LongType())])))

    result = result.withColumn('tiers', zip_(result.tiers_list, result.dbs_sizes_list, result.phedex_sizes_list))\
                   .drop('tiers_list')\
                   .drop('phedex_sizes_list')\
                   .drop('dbs_sizes_list')
    
    result = result.limit(limit)
    
    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        result.write.format("com.databricks.spark.csv")\
                    .option("header", "true").save(fout)

    ctx.stop()

@info_save('%s/%s' % (get_destination_dir(), CAMPAIGN_TIER_TIME_DATA_FILE))
def main():
    "Main function"
    opts = get_options()
    print("Input arguments: %s" % opts)
    
    fout = opts.fout
    date = opts.date
    verbose = opts.verbose
    yarn = opts.yarn
    inst = opts.inst
    limit = opts.limit
    
    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    
    run(fout, date, yarn, verbose, inst, limit)

if __name__ == '__main__':
    main()
