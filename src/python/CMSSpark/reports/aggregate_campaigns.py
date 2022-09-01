#!/usr/bin/env python
"""
Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# System modules
import os

# Pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import struct, array, udf, countDistinct
from pyspark.sql.types import IntegerType, LongType, StringType, StructType, StructField

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import info_save
from CMSSpark.conf import OptionParser

CAMPAIGNS_TIME_DATA_FILE = 'spark_exec_time_campaigns.txt'

def get_options():
    opts = OptionParser('campaigns')

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

    # read Phedex and DBS tables
    tables = {}

    tables.update(phedex_tables(sqlContext, verbose=verbose, fromdate=fromdate, todate=todate))
    phedex = tables['phedex_df']

    instances = ['GLOBAL'] # , 'PHYS01', 'PHYS02', 'PHYS03'
    for instance in instances:
        dbs_dict = dbs_tables(sqlContext, inst=instance, verbose=verbose)
        for key, val in dbs_dict.items():
            new_key = '%s_%s' % (key, instance)
            tables[new_key] = val
    
    daf = reduce(lambda a,b: a.union_all(b), [tables['daf_%s' % x] for x in instances])
    ddf = reduce(lambda a,b: a.union_all(b), [tables['ddf_%s' % x] for x in instances])
    fdf = reduce(lambda a,b: a.union_all(b), [tables['fdf_%s' % x] for x in instances])

    dbs_fdf_cols = ['f_dataset_id', 'f_file_size']
    dbs_ddf_cols = ['d_dataset_id', 'd_dataset', 'd_dataset_access_type_id']
    dbs_daf_cols = ['dataset_access_type_id', 'dataset_access_type']

    fdf_df = fdf.select(dbs_fdf_cols)
    ddf_df = ddf.select(dbs_ddf_cols)
    daf_df = daf.select(dbs_daf_cols)

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

    # dataset_name, block_bytes, node_name
    phedex_cols = ['dataset_name', 'block_bytes', 'node_name']
    phedex_all_df = phedex.select(phedex_cols)
    
    aggregate(sqlContext, fout, phedex_all_df, dbs_df, limit)
    aggregate(sqlContext, fout, phedex_all_df, dbs_df, limit, disk_only=True)

    ctx.stop()

def aggregate(sqlContext, fout, phedex_all_df, dbs_df, limit, disk_only=False):
    extract_campaign_udf = udf(lambda dataset: dataset.split('/')[2])

    if disk_only == True:
        is_tape = lambda site: site.endswith('_MSS') | site.endswith('_Buffer') | site.endswith('_Export')
        phedex_filtered_df = phedex_all_df.where(is_tape(phedex_all_df.node_name) == False)
    else:
        phedex_filtered_df = phedex_all_df

    # dataset, phedex_site
    phedex_df = phedex_filtered_df.withColumnRenamed('block_bytes', 'phedex_size')\
                                  .withColumnRenamed('dataset_name', 'dataset')\
                                  .drop('node_name')
    
    # dataset, phedex_size
    phedex_df = phedex_df.groupBy(['dataset'])\
                   .agg({'phedex_size':'sum'})\
                   .withColumnRenamed('sum(phedex_size)', 'phedex_size')

    # dataset, dbs_size, phedex_size
    dbs_phedex_df = dbs_df.join(phedex_df, 'dataset')
    
    dbs_phedex_df = dbs_phedex_df.withColumn('campaign', extract_campaign_udf(dbs_phedex_df.dataset))

    # dataset, campaign, dbs_size, phedex_size
    dbs_phedex_df = dbs_phedex_df.groupBy(['campaign'])\
                                 .agg({'dbs_size':'sum', 'phedex_size': 'sum'})\
                                 .withColumnRenamed('sum(dbs_size)', 'dbs_size')\
                                 .withColumnRenamed('sum(phedex_size)', 'phedex_size')

    # campaign, site, size
    campaign_site_df = phedex_filtered_df.withColumn('campaign', extract_campaign_udf(phedex_filtered_df.dataset_name))\
                                         .groupBy(['campaign', 'node_name'])\
                                         .agg({'block_bytes':'sum'})\
                                         .withColumnRenamed('sum(block_bytes)', 'size')\
                                         .withColumnRenamed('node_name', 'site')

    # site, campaign_count
    site_campaign_count = campaign_site_df.groupBy(['site'])\
                                          .agg(countDistinct('campaign'))\
                                          .withColumnRenamed('count(campaign)', 'campaign_count')\
                                          .orderBy('campaign_count', ascending=False)

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

    result = result.join(dbs_phedex_df, 'campaign')

    sorted_by_phedex = result.orderBy(result.phedex_size, ascending=False).limit(limit)
    sorted_by_dbs = result.orderBy(result.dbs_size, ascending=False).limit(limit)
    site_campaign_count = site_campaign_count.limit(limit)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        suffix = 'disk_only' if disk_only else 'full'

        sorted_by_phedex.write.format("com.databricks.spark.csv")\
                              .option("header", "true").save('%s/phedex_%s' % (fout, suffix))
        
        sorted_by_dbs.write.format("com.databricks.spark.csv")\
                           .option("header", "true").save('%s/dbs_%s' % (fout, suffix))

        site_campaign_count.write.format("com.databricks.spark.csv")\
                                 .option("header", "true").save('%s/site_campaign_count_%s' % (fout, suffix))

@info_save('%s/%s' % (get_destination_dir(), CAMPAIGNS_TIME_DATA_FILE))
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
