#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : rucio_all_detailed_datasets.py
Author      : Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : This Spark job creates detailed datasets(in each RSEs) results by aggregating Rucio&DBS tables and
                save result to HDFS directory as a source to MongoDB of go web service
"""

# system modules
from datetime import datetime

import click
import pandas as pd
from pyspark.sql.functions import (
    array_distinct, col, collect_set, concat_ws, countDistinct, first, flatten, from_unixtime, greatest, lit, lower,
    when,
    count as _count,
    hex as _hex,
    max as _max,
    split as _split,
    sum as _sum,
)

from pyspark.sql.types import (
    LongType,
    DecimalType
)

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session

pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.max_colwidth', None)

# global variables
TODAY = datetime.today().strftime('%Y-%m-%d')
# Rucio
HDFS_RUCIO_RSES = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/RSES/part*.avro'
HDFS_RUCIO_REPLICAS = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/REPLICAS/part*.avro'
HDFS_RUCIO_DIDS = f'/project/awg/cms/rucio/{TODAY}/dids/part*.avro'
HDFS_RUCIO_LOCKS = f'/project/awg/cms/rucio/{TODAY}/locks/part*.avro'
# DBS
HDFS_DBS_DATASETS = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/DATASETS/part*.avro'
HDFS_DBS_BLOCKS = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/BLOCKS/part*.avro'
HDFS_DBS_FILES = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/FILES/part*.avro'
PROD_ACCOUNTS = ['transfer_ops', 'wma_prod', 'wmcore_output', 'wmcore_transferor', 'crab_tape_recall', 'sync']
SYNC_PREFIX = 'sync'


def get_df_rses(spark):
    """Create rucio RSES table dataframe with some rse tag calculations
    """
    return spark.read.format("avro").load(HDFS_RUCIO_RSES) \
        .filter(col('DELETED_AT').isNull()) \
        .withColumn('rse_id', lower(_hex(col('ID')))) \
        .withColumn('rse_tier', _split(col('RSE'), '_').getItem(0)) \
        .withColumn('rse_country', _split(col('RSE'), '_').getItem(1)) \
        .withColumn('rse_kind',
                    when((col("rse").endswith('Temp') | col("rse").endswith('temp') | col("rse").endswith('TEMP')),
                         'temp')
                    .when((col("rse").endswith('Test') | col("rse").endswith('test') | col("rse").endswith('TEST')),
                          'test')
                    .otherwise('prod')
                    ) \
        .select(['rse_id', 'RSE', 'RSE_TYPE', 'rse_tier', 'rse_country', 'rse_kind'])


def get_df_replicas(spark):
    """Create rucio Replicas table dataframe
    """
    return spark.read.format('avro').load(HDFS_RUCIO_REPLICAS) \
        .withColumn('rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumn('f_size_replicas', col('BYTES').cast(LongType())) \
        .withColumn('rep_lock_cnt', col('LOCK_CNT').cast(LongType())) \
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('ACCESSED_AT', 'rep_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'rep_created_at') \
        .filter(col('SCOPE') == 'cms') \
        .select(['f_name', 'rse_id', 'f_size_replicas', 'rep_accessed_at', 'rep_created_at', 'rep_lock_cnt'])


def get_df_dids_files(spark):
    """Create rucio DIDS table dataframe with just for files
    """
    return spark.read.format('avro').load(HDFS_RUCIO_DIDS) \
        .filter(col('DELETED_AT').isNull()) \
        .filter(col('HIDDEN') == '0') \
        .filter(col('SCOPE') == 'cms') \
        .filter(col('DID_TYPE') == 'F') \
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('ACCESSED_AT', 'dids_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'dids_created_at') \
        .withColumn('f_size_dids', col('BYTES').cast(LongType())) \
        .select(['f_name', 'f_size_dids', 'dids_accessed_at', 'dids_created_at'])


def get_df_dids_datasets(spark):
    """Create rucio DIDS table dataframe with just for datasets
    """
    return spark.read.format('avro').load(HDFS_RUCIO_DIDS) \
        .filter(col('DELETED_AT').isNull()) \
        .filter(col('HIDDEN') == '0') \
        .filter(col('SCOPE') == 'cms') \
        .filter(col('DID_TYPE') == 'C') \
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('ACCESSED_AT', 'dids_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'dids_created_at') \
        .withColumn('f_size_dids', col('BYTES').cast(LongType())) \
        .select(['f_name', 'f_size_dids', 'dids_accessed_at', 'dids_created_at'])


def get_df_locks(spark):
    """Create rucio Locks table dataframe
    """
    return spark.read.format('avro').load(HDFS_RUCIO_LOCKS) \
        .filter(col('SCOPE') == 'cms') \
        .withColumn('rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumn('account',
                    when(col('ACCOUNT').startswith(SYNC_PREFIX), lit(SYNC_PREFIX)).otherwise(col('ACCOUNT'))) \
        .withColumn('has_prod_lock', when(col('account').isin(PROD_ACCOUNTS), lit(1)).otherwise(lit(0))) \
        .withColumn('prod_account', when(col('account').isin(PROD_ACCOUNTS), col('account'))) \
        .select(['rse_id', 'NAME', 'account', 'prod_account', 'has_prod_lock']) \
        .groupby(['rse_id', 'NAME']).agg(
        collect_set('prod_account').alias('prod_accts'),
        _sum(col('has_prod_lock')).alias('prod_lock_cnt')) \
        .withColumn('has_lock', when(col('prod_lock_cnt') > 0, lit(1)).otherwise(lit(0))) \
        .withColumn('has_oth_lock', when(col('prod_lock_cnt') == 0, lit(1)).otherwise(lit(0))) \
        .select(['rse_id', 'NAME', 'prod_accts', 'has_lock', 'has_oth_lock'])


def get_df_dbs_files(spark):
    """Create DBS Files table dataframe
    """
    return spark.read.format('avro').load(HDFS_DBS_FILES) \
        .filter(col('IS_FILE_VALID') == '1') \
        .withColumnRenamed('LOGICAL_FILE_NAME', 'FILE_NAME') \
        .select(['FILE_NAME', 'DATASET_ID', 'BLOCK_ID', 'FILE_SIZE'])


def get_df_dbs_blocks(spark):
    """Create DBS Blocks table dataframe
    """
    return spark.read.format('avro').load(HDFS_DBS_BLOCKS) \
        .select(['BLOCK_NAME', 'BLOCK_ID', 'DATASET_ID', 'FILE_COUNT'])


def get_df_dbs_datasets(spark):
    """Create DBS Datasets table dataframe
    """
    return spark.read.format('avro').load(HDFS_DBS_DATASETS) \
        .filter(col('IS_DATASET_VALID') == '1') \
        .select(['DATASET_ID', 'DATASET'])


def get_df_dbs_f_d_map(spark):
    """Create dataframe for DBS dataset:file map
    """
    dbs_files = get_df_dbs_files(spark).withColumnRenamed('DATASET_ID', 'F_DATASET_ID')
    dbs_datasets = get_df_dbs_datasets(spark)
    return dbs_files.join(dbs_datasets, dbs_files.F_DATASET_ID == dbs_datasets.DATASET_ID, how='inner') \
        .select(['DATASET_ID', 'DATASET', 'FILE_NAME'])


def get_df_ds_file_cnt(spark):
    """Calculate total file count
    """
    return get_df_dbs_f_d_map(spark).groupby(['DATASET']).agg(countDistinct(col('FILE_NAME')).alias('TOT_FILE_CNT')) \
        .select(['DATASET', 'TOT_FILE_CNT'])


def get_df_files_enr(spark):
    """Enriched files with REPLICAS, DIDS and LOCKS"""
    df_replicas = get_df_replicas(spark)
    df_dids_files = get_df_dids_files(spark)
    df_locks = get_df_locks(spark).withColumnRenamed('rse_id', 'locks_rse_id')
    df_rses = get_df_rses(spark)
    df_rep_enr_dids = df_replicas.join(df_dids_files, ['f_name'], how='left') \
        .withColumn('f_size',
                    when(col('f_size_replicas').isNotNull(), col('f_size_replicas'))
                    .when(col('f_size_dids').isNotNull(), col('f_size_dids'))
                    ) \
        .withColumn('accessed_at',
                    greatest(col('dids_accessed_at'), col('rep_accessed_at'))
                    ) \
        .withColumn('created_at',
                    greatest(col('dids_created_at'), col('rep_created_at'))
                    ) \
        .select(['f_name', 'rse_id', 'accessed_at', 'f_size', 'created_at', 'rep_lock_cnt'])
    # ['rse_id', 'NAME', 'prod_accts', 'oth_acct_cnt']
    cond_replicas_locks = [df_rep_enr_dids.f_name == df_locks.NAME, df_rep_enr_dids.rse_id == df_locks.locks_rse_id]
    df_rep_enr_with_locks = df_rep_enr_dids.join(df_locks, cond_replicas_locks, how='left')

    return df_rep_enr_with_locks.join(df_rses.select(['rse_id', 'RSE']), ['rse_id'], how='left') \
        .select(['f_name', 'RSE', 'f_size', 'accessed_at', 'created_at', 'rep_lock_cnt', 'prod_accts', 'has_lock',
                 'has_oth_lock'])


def get_df_main_raw(spark):
    """Files with dataset name"""
    df_files_enr = get_df_files_enr(spark)
    df_dbs_f_d_map = get_df_dbs_f_d_map(spark)
    return df_files_enr \
        .join(df_dbs_f_d_map, df_files_enr.f_name == df_dbs_f_d_map.FILE_NAME, how='left') \
        .fillna("UnknownDatasetNameOfFiles_MonitoringTag", subset=['DATASET']) \
        .fillna("0", subset=['DATASET_ID']) \
        .withColumnRenamed('DATASET_ID', 'd_id').withColumnRenamed('DATASET', 'd_name') \
        .select(['d_id', 'd_name', 'RSE', 'f_name', 'f_size', 'accessed_at', 'created_at', 'rep_lock_cnt', 'prod_accts',
                 'has_lock', 'has_oth_lock'])


def get_ds_in_rses(spark):
    df_main_raw = get_df_main_raw(spark)
    df_ds_file_cnt = get_df_ds_file_cnt(spark)
    df = df_main_raw.groupby(['RSE', 'd_name']) \
        .agg(_count(lit(1)).alias('Fcnt'),
             _max(col('accessed_at')).alias('LastAccMs'),
             _sum(col('f_size')).alias('SizeBytes'),
             _sum(col('has_lock')).alias('ProdLckCnt'),
             _sum(col('has_oth_lock')).alias('OthLckCnt'),
             _sum(when(col('accessed_at').isNull(), 0).otherwise(1)).alias('AccFcnt'),
             first(col('d_id')).alias('d_id'),
             concat_ws(', ', array_distinct(flatten(collect_set(col('prod_accts'))))).alias('ProdAccts'), ) \
        .withColumn('LastAcc', from_unixtime(col("LastAccMs") / 1000, "yyyy-MM-dd"))

    df = df.join(df_ds_file_cnt, df.d_name == df_ds_file_cnt.DATASET, how='left').drop('DATASET') \
        .withColumn('Fpct', (100 * col('Fcnt') / col('TOT_FILE_CNT')).cast(DecimalType(6, 2))) \
        .withColumn('Id', col('d_id').cast(LongType())) \
        .withColumnRenamed('d_name', 'Dataset') \
        .select(['RSE', 'Dataset', 'Id', 'SizeBytes', 'LastAcc', 'LastAccMs', 'Fcnt', 'AccFcnt', 'ProdLckCnt',
                 'OthLckCnt', 'ProdAccts', 'Fpct'])

    # Enrich with RSE attributes
    df_rses = get_df_rses(spark)
    return df.join(df_rses, ['RSE'], how='left') \
        .withColumnRenamed('RSE_TYPE', 'Type') \
        .withColumnRenamed('rse_tier', 'Tier') \
        .withColumnRenamed('rse_country', 'C') \
        .withColumnRenamed('rse_kind', 'RseKind') \
        .fillna(0, subset=['LastAccMs']) \
        .select(['Type', 'Dataset', 'RSE', 'Tier', 'C', 'RseKind', 'SizeBytes', 'LastAcc', 'LastAccMs', 'Fpct', 'Fcnt',
                 'AccFcnt', 'ProdLckCnt', 'OthLckCnt', 'ProdAccts', ])


@click.command()
@click.option('--hdfs_out_dir', default=None, type=str, required=True,
              help='I.e. /tmp/${KERBEROS_USER}/rucio_ds_mongo/$(date +%Y-%m-%d) ')
def main(hdfs_out_dir):
    """Main function that run Spark dataframe creations and save results to HDFS directory as JSON lines
    """

    # HDFS output file format. If you change, please modify bin/cron4rucio_ds_mongo.sh accordingly.
    write_format = 'json'
    write_mode = 'overwrite'

    spark = get_spark_session(app_name='cms-monitoring-rucio-detailed-datasets-for-mongo')
    # Set TZ as UTC. Also set in the spark-submit confs.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df_ds_in_rses = get_ds_in_rses(spark)
    df_ds_in_rses.write.save(path=hdfs_out_dir, format=write_format, mode=write_mode)


if __name__ == '__main__':
    main()
