#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : rucio_ds_summary.py
Author      : Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : This Spark job creates datasets summary results by aggregating Rucio&DBS tables and
                save result to HDFS directory as a source to MongoDB of go web service

It does not include RSE details part, each row represents a dataset and its aggregated results in RSEs.
"""

# system modules
import json
import logging
import os
import sys
import time
from datetime import datetime

import click
from pyspark.sql.functions import (
    col, countDistinct, first, greatest, lit, lower, when,
    avg as _avg,
    count as _count,
    hex as _hex,
    max as _max,
    min as _min,
    split as _split,
    sum as _sum,
)
from pyspark.sql.types import LongType

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session

# CMSMonitoring modules
try:
    from CMSMonitoring.StompAMQ7 import StompAMQ7
except ImportError:
    print("ERROR: Could not import StompAMQ")
    sys.exit(1)

# global variables
TODAY = datetime.today().strftime('%Y-%m-%d')

# Rucio
HDFS_RUCIO_RSES = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/RSES/part*.avro'
HDFS_RUCIO_REPLICAS = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/REPLICAS/part*.avro'
HDFS_RUCIO_DIDS = f'/project/awg/cms/rucio/{TODAY}/dids/part*.avro'
# DBS
HDFS_DBS_DATASETS = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/DATASETS/part*.avro'
HDFS_DBS_FILES = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/FILES/part*.avro'
HDFS_DBS_DATA_TIERS = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/DATA_TIERS/part*.avro'
HDFS_DBS_PHYSICS_GROUPS = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/PHYSICS_GROUPS/part*.avro'
HDFS_DBS_ACQUISITION_ERAS = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/ACQUISITION_ERAS/part*.avro'
HDFS_DBS_DATASET_ACCESS_TYPES = f'/tmp/cmsmonit/rucio_daily_stats-{TODAY}/DATASET_ACCESS_TYPES/part*.avro'

# To fill null columns of string type. Reason:
#   {"find": "terms", "field": "data.data_tier_name"} kind of ES queries do not return Null values.
STR_TYPE_COLUMNS = ['RseType', 'IsDatasetValid', 'TierName', 'PhysicsGroupName', 'AcquisitionEraName',
                    'DatasetAccessType']

# Null string type column values will be replaced with
NULL_STR_TYPE_COLUMN_VALUE = 'UNKNOWN'


def get_df_rses(spark):
    """Get pandas dataframe of RSES
    """
    df_rses = spark.read.format("avro").load(HDFS_RUCIO_RSES) \
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
    return df_rses


def get_df_replicas(spark):
    """Create main replicas dataframe by selecting only Disk or Tape RSEs in Rucio REPLICAS table
    """
    # List of all RSE id list
    # rse_id_list = df_pd_rses['replica_rse_id'].to_list()
    # .filter(col('rse_id').isin(rse_id_list)) \
    return spark.read.format('avro').load(HDFS_RUCIO_REPLICAS) \
        .withColumn('rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumn('f_size_replicas', col('BYTES').cast(LongType())) \
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('ACCESSED_AT', 'rep_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'rep_created_at') \
        .filter(col('SCOPE') == 'cms') \
        .select(['f_name', 'rse_id', 'f_size_replicas', 'rep_accessed_at', 'rep_created_at'])


def get_df_dids_files(spark):
    """Create spark dataframe for DIDS table by selecting only Files in Rucio DIDS table.

    Filters:
        - DELETED_AT not null
        - HIDDEN = 0
        - SCOPE = cms
        - DID_TYPE = F

    Columns selected:
        - f_name: file name
        - f_size_dids: represents size of a file in DIDS table
        - dids_accessed_at: file last access time
        - dids_created_at: file creation time
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


def get_df_dbs_f_d(spark):
    """Create a dataframe for FILE-DATASET membership/ownership map

    Columns selected: f_name, dataset
    """
    dbs_files = spark.read.format('avro').load(HDFS_DBS_FILES) \
        .withColumnRenamed('LOGICAL_FILE_NAME', 'f_name') \
        .withColumnRenamed('DATASET_ID', 'f_dataset_id') \
        .select(['f_name', 'f_dataset_id'])
    dbs_datasets = spark.read.format('avro').load(HDFS_DBS_DATASETS) \
        .withColumnRenamed('DATASET_ID', 'd_dataset_id') \
        .withColumnRenamed('DATASET', 'd_dataset') \
        .select(['d_dataset_id', 'd_dataset'])
    df_dbs_f_d = dbs_files.join(dbs_datasets, dbs_files.f_dataset_id == dbs_datasets.d_dataset_id, how='left') \
        .withColumnRenamed('f_dataset_id', 'dataset_id') \
        .withColumnRenamed('d_dataset', 'dataset') \
        .select(['dataset_id', 'f_name', 'dataset'])
    return df_dbs_f_d


def get_df_ds_general_info(spark):
    """Calculate real size and total file counts of dataset: RealSize, TotalFileCnt
    """
    dbs_files = spark.read.format('avro').load(HDFS_DBS_FILES).select(['DATASET_ID', 'FILE_SIZE', 'LOGICAL_FILE_NAME'])
    dbs_datasets = spark.read.format('avro').load(HDFS_DBS_DATASETS).select(['DATASET_ID'])
    dbs_data_tiers = spark.read.format('avro').load(HDFS_DBS_DATA_TIERS)
    dbs_physics_group = spark.read.format('avro').load(HDFS_DBS_PHYSICS_GROUPS)
    dbs_acquisition_era = spark.read.format('avro').load(HDFS_DBS_ACQUISITION_ERAS)
    dbs_dataset_access_type = spark.read.format('avro').load(HDFS_DBS_DATASET_ACCESS_TYPES)
    dbs_datasets_file_info = dbs_datasets.join(dbs_files, ['DATASET_ID'], how='left') \
        .groupby('DATASET_ID') \
        .agg(_sum('FILE_SIZE').alias('RealSize'),
             countDistinct(col('LOGICAL_FILE_NAME')).alias('TotalFileCnt')
             ) \
        .select(['DATASET_ID', 'RealSize', 'TotalFileCnt'])

    dbs_datasets = spark.read.format('avro').load(HDFS_DBS_DATASETS)

    return dbs_datasets \
        .join(dbs_data_tiers, ['DATA_TIER_ID'], how='left') \
        .join(dbs_physics_group, ['PHYSICS_GROUP_ID'], how='left') \
        .join(dbs_acquisition_era, ['ACQUISITION_ERA_ID'], how='left') \
        .join(dbs_dataset_access_type, ['DATASET_ACCESS_TYPE_ID'], how='left') \
        .join(dbs_datasets_file_info, ['DATASET_ID'], how='left') \
        .withColumnRenamed('DATASET_ID', 'Id') \
        .withColumnRenamed('IS_DATASET_VALID', 'IsDatasetValid') \
        .withColumnRenamed('DATA_TIER_NAME', 'TierName') \
        .withColumnRenamed('PHYSICS_GROUP_NAME', 'PhysicsGroupName') \
        .withColumnRenamed('ACQUISITION_ERA_NAME', 'AcquisitionEraName') \
        .withColumnRenamed('DATASET_ACCESS_TYPE', 'DatasetAccessType') \
        .select(['Id', 'RealSize', 'TotalFileCnt', 'IsDatasetValid', 'TierName', 'PhysicsGroupName',
                 'AcquisitionEraName', 'DatasetAccessType'])


def get_df_replicas_j_dids(df_replicas, df_dids_files):
    """Left join of df_replicas and df_dids_files to fill the RSE_ID, f_size and accessed_at, created_at for all files.

    Be aware that there are 2 columns for each f_size, accessed_at, created_at
    They will be combined in get_df_file_rse_ts_size

    Columns:
        comes from DID:       file, dids_accessed_at, dids_created_at, f_size_dids,
        comes from REPLICAS:  file, rse_id, f_size_replicas, rep_accessed_at, rep_created_at
   """
    return df_replicas.join(df_dids_files, ['f_name'], how='left')


def get_df_file_rse_ts_size(df_replicas_j_dids):
    """Combines columns to get filled and correct values from join of DIDS and REPLICAS

    Firstly, REPLICAS size value will be used. If there are files with no size values, DIDS size values will be used:
    see 'when' function order. For accessed_at and created_at, their max values will be got.

    Columns: file, rse_id, accessed_at, f_size, created_at

    df_file_rse_ts_size: files and their rse_id, size and access time are completed
    """

    # f_size is not NULL, already verified.
    # df_file_rse_ts_size.filter(col('f_size').isNull()).limit(5).toPandas()
    return df_replicas_j_dids \
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
        .select(['f_name', 'rse_id', 'accessed_at', 'f_size', 'created_at'])


def get_df_dataset_file_rse_ts_size(df_file_rse_ts_size, df_dbs_f_d):
    """ Left join df_file_rse_ts_size and df_dbs_f_d to get dataset names of files.

    In short: adds 'dataset' names to 'df_file_rse_ts_size' dataframe by joining DBS tables

    Columns: block(from df_contents_f_to_b), file, rse_id, accessed_at, f_size
    """
    df_dataset_file_rse_ts_size = df_file_rse_ts_size \
        .join(df_dbs_f_d, ['f_name'], how='left') \
        .fillna("UnknownDatasetNameOfFiles_MonitoringTag", subset=['dataset']) \
        .select(['dataset_id', 'dataset', 'f_name', 'rse_id', 'accessed_at', 'created_at', 'f_size'])

    # f_c = df_dataset_file_rse_ts_size.select('f_name').distinct().count()
    # f_w_no_dataset_c = df_dataset_file_rse_ts_size.filter(col('dataset').isNull()).select('f_name').distinct().count()
    # print('Distinct file count:', f_c)
    # print('Files with no dataset name count:', f_w_no_dataset_c)
    # print('% of null dataset name in all files:', round(f_w_no_dataset_c / f_c, 3) * 100)

    return df_dataset_file_rse_ts_size


def get_df_enr_with_rse_info(df_dataset_file_rse_ts_size, df_rses):
    """Add RSE type, name, kind, tier, country by joining RSE ID"""
    return df_dataset_file_rse_ts_size.join(df_rses, ['rse_id'], how='left') \
        .select(['dataset_id', 'dataset', 'f_name', 'rse_id', 'accessed_at', 'created_at', 'f_size',
                 'RSE', 'RSE_TYPE', 'rse_tier', 'rse_country', 'rse_kind'])


# --------------------------------------------------------------------------------
# Main dataset functions
# --------------------------------------------------------------------------------

def get_df_sub_rse_details(df_enr_with_rse_info):
    """Get dataframe of datasets that are not read since N months for sub details htmls

    Group by 'dataset' and 'rse_id' of get_df_dataset_file_rse_ts_size

    Filters:
        - If a dataset contains EVEN a single file with null accessed_at, filter out

    Access time filter logic:
        - If 'last_access_time_of_dataset_in_all_rses' is less than 'n_months_filter', ...
          ... set 'is_not_read_since_{n_months_filter}_months' column as True

    Columns:
        - 'dataset_size_in_rse_gb'
                Total size of a Dataset in an RSE.
                Produced by summing up datasets' all files in that RSE.
        - 'last_access_time_of_dataset_in_rse'
                Last access time of a Dataset in an RSE.
                Produced by getting max `accessed_at`(represents single file's access time) of a dataset in an RSE.
        - '#files_with_null_access_time_of_dataset_in_rse'
                Number of files count, which have NULL `accessed_at` values, of a Dataset in an RSE.
                This is important to know to filter out if there is any NULL `accessed_at` value of a Dataset.
        - '#files_of_dataset_in_rse'
                Number of files count of a Dataset in an RSE
        - '#distinct_files_of_dataset_in_rse'
                Number of unique files count of dataset in an RSE

    df_main_datasets_and_rses: RSE name, dataset and their size and access time calculations
    """
    # Get RSE ID:NAME map
    # rses_id_name_map = dict(df_pd_rses[['replica_rse_id', 'rse']].values)
    # rses_id_type_map = dict(df_pd_rses[['replica_rse_id', 'rse_type']].values)
    # rses_id_tier_map = dict(df_pd_rses[['replica_rse_id', 'rse_tier']].values)
    # rses_id_country_map = dict(df_pd_rses[['replica_rse_id', 'rse_country']].values)
    # rses_id_kind_map = dict(df_pd_rses[['replica_rse_id', 'rse_kind']].values)
    # .replace(rses_id_name_map, subset=['rse_id']) \
    # , 'rse_tier', 'rse_country', 'rse_kind',
    return df_enr_with_rse_info \
        .groupby(['rse_id', 'dataset']) \
        .agg(_sum(col('f_size')).alias('SizeInRseBytes'),
             _max(col('accessed_at')).alias('LastAccessInRse'),
             _max(col('created_at')).alias('LastCreateInRse'),
             _count(lit(1)).alias('FileCnt'),
             _sum(when(col('accessed_at').isNull(), 0).otherwise(1)).alias('AccessedFileCnt'),
             first(col('dataset_id')).alias('dataset_id'),
             first(col('RSE_TYPE')).alias('RseType'),
             first(col('RSE')).alias('RSE'),
             first(col('rse_tier')).alias('rse_tier'),
             first(col('rse_country')).alias('rse_country'),
             first(col('rse_kind')).alias('rse_kind'),
             ) \
        .withColumnRenamed('dataset', 'Dataset') \
        .select(['dataset_id', 'RseType', 'RSE', 'Dataset', 'SizeInRseBytes',
                 'LastAccessInRse', 'LastCreateInRse', 'FileCnt', 'AccessedFileCnt', ])


def get_df_main_datasets(df_sub_rse_details, df_ds_general_info):
    """Get dataframe of datasets not read since N months for main htmls.

    Get last access of dataframe in all RSE(s)
    """
    # Order of the select is important
    df = df_sub_rse_details \
        .groupby(['RseType', 'Dataset']) \
        .agg(_max(col('SizeInRseBytes')).cast(LongType()).alias('Max'),
             _min(col('SizeInRseBytes')).cast(LongType()).alias('Min'),
             _avg(col('SizeInRseBytes')).cast(LongType()).alias('Avg'),
             _sum(col('SizeInRseBytes')).cast(LongType()).alias('Sum'),
             _max(col('LastAccessInRse')).cast(LongType()).alias('LastAccess'),
             _max(col('LastCreateInRse')).cast(LongType()).alias('LastCreate'),
             first(col('dataset_id')).cast(LongType()).alias('Id'),
             ) \
        .select(['Id', 'RseType', 'Dataset', 'LastAccess', 'LastCreate', 'Max', 'Min', 'Avg', 'Sum'])

    df = df.join(df_ds_general_info, ['Id'], how='left') \
        .select(['Id', 'RseType', 'Dataset', 'LastAccess', 'LastCreate', 'Max', 'Min', 'Avg', 'Sum',
                 'RealSize', 'TotalFileCnt', 'IsDatasetValid', 'TierName', 'PhysicsGroupName', 'AcquisitionEraName',
                 'DatasetAccessType'])
    df = df.fillna(value=NULL_STR_TYPE_COLUMN_VALUE, subset=STR_TYPE_COLUMNS)
    return df


# =====================================================================================================================
#                     Send data with STOMP AMQ
# =====================================================================================================================
def credentials(f_name):
    if os.path.exists(f_name):
        return json.load(open(f_name))
    return {}


def drop_nulls_in_dict(d):  # d: dict
    """Drops the dict key if the value is None

    ES mapping does not allow None values and drops the document completely.
    """
    return {k: v for k, v in d.items() if v is not None}  # dict


def to_chunks(data, samples=1000):
    length = len(data)
    for i in range(0, length, samples):
        yield data[i:i + samples]


def send_to_amq(data, confs, batch_size):
    """Sends list of dictionary in chunks"""
    wait_seconds = 0.001
    if confs:
        username = confs.get('username', '')
        password = confs.get('password', '')
        producer = confs.get('producer')
        doc_type = confs.get('type', None)
        topic = confs.get('topic')
        host = confs.get('host')
        port = int(confs.get('port'))
        cert = confs.get('cert', None)
        ckey = confs.get('ckey', None)
        # Slow: stomp_amq.send_as_tx(chunk, docType=doc_type)
        #
        for chunk in to_chunks(data, batch_size):
            stomp_amq = StompAMQ7(username=username, password=password, producer=producer, topic=topic,
                                  key=ckey, cert=cert, validation_schema=None, host_and_ports=[(host, port)],
                                  loglevel=logging.WARNING)
            messages = []
            for msg in chunk:
                notif, _, _ = stomp_amq.make_notification(payload=msg, doc_type=doc_type, producer=producer)
                messages.append(notif)
            if messages:
                stomp_amq.send(messages)
                time.sleep(wait_seconds)
        time.sleep(1)
        print("Message sending is finished")


@click.command()
@click.option("--creds", required=True, help="secrets/cms-rucio-dailystats/creds.json")
@click.option("--amq_batch_size", type=click.INT, required=False, help="AMQ transaction batch size",
              default=100)
@click.option("--test", is_flag=True, default=False, required=False,
              help="It will send only 10 documents to ElasticSearch. "
                   "[!Attention!] Please provide test/training AMQ topic.")
def main(creds, amq_batch_size, test):
    """Main function that run Spark dataframe creations and save results to HDFS directory as JSON lines
    """
    spark = get_spark_session(app_name='cms-monitoring-rucio-datasets-for-mongo')
    # Set TZ as UTC. Also set in the spark-submit confs.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # The reason that we have lots of functions that returns PySpark dataframes is mainly for code readability.
    df_rses = get_df_rses(spark)
    df_dbs_f_d = get_df_dbs_f_d(spark)
    df_ds_general_info = get_df_ds_general_info(spark)
    df_replicas = get_df_replicas(spark)
    df_dids_files = get_df_dids_files(spark)
    df_replicas_j_dids = get_df_replicas_j_dids(df_replicas, df_dids_files)
    df_file_rse_ts_size = get_df_file_rse_ts_size(df_replicas_j_dids)
    df_dataset_file_rse_ts_size = get_df_dataset_file_rse_ts_size(df_file_rse_ts_size, df_dbs_f_d)
    df_enr_with_rse_info = get_df_enr_with_rse_info(df_dataset_file_rse_ts_size, df_rses)
    df_sub_rse_details = get_df_sub_rse_details(df_enr_with_rse_info)
    df = get_df_main_datasets(df_sub_rse_details, df_ds_general_info)

    creds_json = credentials(f_name=creds)
    print('Schema:')
    df.printSchema()
    total_size = 0
    if test:
        _topic = creds_json["topic"].lower()
        if ("train" not in _topic) and ("test" not in _topic):
            print(f"Test failed. Topic \"{_topic}\" is not training or test topic.")
            sys.exit(1)

        for part in df.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
            part_size = len(part)
            print(f"Length of partition: {part_size}")
            send_to_amq(data=part[:10], confs=creds_json, batch_size=amq_batch_size)
            print(f"Test successfully finished and sent 10 documents to {_topic} AMQ topic.")
            sys.exit(0)
    else:
        # Iterate over list of dicts returned from spark
        for part in df.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
            part_size = len(part)
            print(f"Length of partition: {part_size}")
            send_to_amq(data=part, confs=creds_json, batch_size=amq_batch_size)
            total_size += part_size
        print(f"Total document size: {total_size}")


if __name__ == '__main__':
    main()
