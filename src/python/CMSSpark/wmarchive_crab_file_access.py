#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : wmarchive_crab_file_access.py
Author      : Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : Generates last/first access of datasets using WMArchive data belonging CRAB jobs

How:
  - Only filter is that meta_data.jobtype should start with CRAB for the aim of just to get user jobs' accesses to files
  - data.meta_data.ts is used as file access time
  - files' datasets are extracted from the join with DBS tables and additional DBS dataset info is placed in each row
  - Main calculation is to get LastAccess, FirstAccess, AccessCnt of datasets which are only accessed by user jobs
"""

# system modules
import logging
import sys
import time
from datetime import date, datetime, timezone
from collections import defaultdict
import click
from pyspark.sql.functions import (
    col as _col, countDistinct, explode, posexplode, first, greatest, lit, lower, upper, when,
    avg as _avg,
    count as _count,
    hex as _hex,
    split as _split,
    max as _max,
    min as _min,
    sum as _sum,
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session, get_candidate_files
from es_opensearch.es import EsInterface

# global variables
TODAY = datetime.today().strftime('%Y-%m-%d')
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]

# WMArchive
HDFS_WMA_WMARCHIVE = "/project/monitoring/archive/wmarchive/raw/metric"
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
STR_TYPE_COLUMNS = ['IsDatasetValid', 'TierName', 'PhysicsGroupName', 'AcquisitionEraName', 'DatasetAccessType']

# Null string type column values will be replaced with
NULL_STR_TYPE_COLUMN_VALUE = 'UNKNOWN'


def info_logs():
    """Print info some important variables for Spark job"""
    print("[INFO] Used HDFS folders")
    print(f"[INFO] HDFS_WMA_WMARCHIVE :{HDFS_WMA_WMARCHIVE}")
    print(f"[INFO] HDFS_RUCIO_RSES :{HDFS_RUCIO_RSES}")
    print(f"[INFO] HDFS_RUCIO_REPLICAS :{HDFS_RUCIO_REPLICAS}")
    print(f"[INFO] HDFS_RUCIO_DIDS :{HDFS_RUCIO_DIDS}")
    print(f"[INFO] HDFS_DBS_DATASETS :{HDFS_DBS_DATASETS}")
    print(f"[INFO] HDFS_DBS_FILES :{HDFS_DBS_FILES}")
    print(f"[INFO] HDFS_DBS_DATA_TIERS :{HDFS_DBS_DATA_TIERS}")
    print(f"[INFO] HDFS_DBS_PHYSICS_GROUPS :{HDFS_DBS_PHYSICS_GROUPS}")
    print(f"[INFO] HDFS_DBS_ACQUISITION_ERAS :{HDFS_DBS_ACQUISITION_ERAS}")
    print(f"[INFO] HDFS_DBS_DATASET_ACCESS_TYPES :{HDFS_DBS_DATASET_ACCESS_TYPES}")


# =====================================================================================================================
#                         DBS General Intermediate Data Preparation
# =====================================================================================================================

def get_df_ds_general_info(spark):
    """Calculate real size and total file counts of dataset: RealSize, TotalFileCnt

    Explanations:
        - LastCreation date of a DATASET is deliberatelycalculated by getting max of FILE's LAST_MODIFICATION_DATE
        because CREATION_DATE fields are null in most of the files.
    """
    dbs_files = spark.read.format('avro').load(HDFS_DBS_FILES) \
        .select(['DATASET_ID', 'FILE_SIZE', 'LOGICAL_FILE_NAME', 'LAST_MODIFICATION_DATE'])
    dbs_datasets = spark.read.format('avro').load(HDFS_DBS_DATASETS).select(['DATASET_ID'])
    dbs_data_tiers = spark.read.format('avro').load(HDFS_DBS_DATA_TIERS)
    dbs_physics_group = spark.read.format('avro').load(HDFS_DBS_PHYSICS_GROUPS)
    dbs_acquisition_era = spark.read.format('avro').load(HDFS_DBS_ACQUISITION_ERAS)
    dbs_dataset_access_type = spark.read.format('avro').load(HDFS_DBS_DATASET_ACCESS_TYPES)
    dbs_datasets_file_info = dbs_datasets.join(dbs_files, ['DATASET_ID'], how='left') \
        .groupby('DATASET_ID') \
        .agg(_sum('FILE_SIZE').alias('RealSize'),
             countDistinct(_col('LOGICAL_FILE_NAME')).alias('TotalFileCnt'),
             _max('LAST_MODIFICATION_DATE').alias('LastCreation'),
             ) \
        .select(['DATASET_ID', 'RealSize', 'TotalFileCnt', 'LastCreation'])
    dbs_datasets = spark.read.format('avro').load(HDFS_DBS_DATASETS)
    return dbs_datasets \
        .join(dbs_data_tiers, ['DATA_TIER_ID'], how='left') \
        .join(dbs_physics_group, ['PHYSICS_GROUP_ID'], how='left') \
        .join(dbs_acquisition_era, ['ACQUISITION_ERA_ID'], how='left') \
        .join(dbs_dataset_access_type, ['DATASET_ACCESS_TYPE_ID'], how='left') \
        .join(dbs_datasets_file_info, ['DATASET_ID'], how='left') \
        .withColumnRenamed('DATASET_ID', 'DatasetId') \
        .withColumnRenamed('DATASET', 'Dataset') \
        .withColumnRenamed('IS_DATASET_VALID', 'IsDatasetValid') \
        .withColumnRenamed('DATA_TIER_NAME', 'TierName') \
        .withColumnRenamed('PHYSICS_GROUP_NAME', 'PhysicsGroupName') \
        .withColumnRenamed('ACQUISITION_ERA_NAME', 'AcquisitionEraName') \
        .withColumnRenamed('DATASET_ACCESS_TYPE', 'DatasetAccessType') \
        .select(['DatasetId', 'Dataset', 'RealSize', 'TotalFileCnt', 'LastCreation', 'IsDatasetValid', 'TierName',
                 'PhysicsGroupName', 'AcquisitionEraName', 'DatasetAccessType'])


def get_tier_ids(spark, tiers):
    """Returns dataset tier ids of given tier names"""
    df_data_tiers = spark.read.format('avro').load(HDFS_DBS_DATA_TIERS).toPandas()
    # Get tier ids of given filtered data tier names
    return df_data_tiers[df_data_tiers['DATA_TIER_NAME'].isin(tiers)]['DATA_TIER_ID'].to_list()


def get_all_parents_of_datasets(spark):
    """Returns datasets and their all parents in a list (grand parents, grand grand parents, etc.)

    Examples:
        Firstly gather all:
            Row(id=1, pids=[2]), Row(id=2, pids=[3, 4]),
        Then flatten again
            Row(id=1, pid=2), Row(id=1, pid=3), Row(id=1, pid=4),
            Row(id=2, pid=3), Row(id=2, pid=4),
    Operation is not costly
    """
    schema_in = StructType([
        StructField("this_dataset_id", LongType(), True),
        StructField("parent_dataset_id", LongType(), True)])
    dbs_dataset_parents = spark.read.csv('/tmp/cmssqoop/dbs_global/2022-10-04/DATASET_PARENTS/*.gz', header=False,
                                         schema=schema_in)
    dct = defaultdict(set)
    for ds_id, parent_id in dbs_dataset_parents.toPandas().to_records(index=False).tolist():
        if dct[parent_id]:
            # let's say: ds_id:3, parent_id:2 has also a parent as {1}, we should add {1, 2} to ds_id 3's values
            t = dct[parent_id].copy()
            t.add(parent_id)
            dct[ds_id].update(t)
        else:
            # Firs addition to empty set object
            dct[ds_id].add(parent_id)

    schema = StructType([
        StructField('DatasetId', LongType(), True),
        StructField('ParentDatasetId', ArrayType(LongType()), True)
    ])
    # convert set to list for pyspark
    dct = {k: list(v) for k, v in dct.items() if v is not None}
    df = spark.createDataFrame(data=dct.items(), schema=schema)
    df = df.select(df.DatasetId, explode(df.ParentDatasetId).alias('ParentDatasetId'))
    return df


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


# =====================================================================================================================
#                         WMArchive Data Preparation
# =====================================================================================================================
def get_wm_agg_schema():
    """Final schema for LFNArray files"""
    return StructType(
        [
            StructField('wmaid', StringType(), nullable=False),
            StructField('access_ts', LongType(), nullable=False),
            # StructField('jobtype', StringType(), nullable=False),
            StructField('jobstate', StringType(), nullable=True),
            StructField('file', StringType(), nullable=False),
        ]
    )


def get_wma_raw_read_schema():
    """Raw schema while reading WMArchive data

    WMArchive schema had changed in years, we need to use a custom schema while reading it to not get errors like
    duplicate column error. i.e.: `guid`
    """
    return StructType([
        StructField('data', StructType([
            StructField('meta_data', StructType([
                StructField('ts', LongType(), nullable=True),
                StructField('jobtype', StringType(), nullable=True),
                StructField('jobstate', StringType(), nullable=True)
            ])),
            StructField('LFNArray', ArrayType(StringType()), nullable=True),
            StructField('wmaid', StringType(), nullable=True),
            StructField('wmats', LongType(), nullable=True),
        ])),
        StructField('metadata', StructType([
            StructField('timestamp', LongType(), nullable=True)
        ])),
    ])


def udf_wma_lfn_extract(row):
    """
    Borrowed from wmarchive.py

    Helper function to extract useful data from WMArchive records.
    Returns list of files from LFNArray with its metadata
    """
    # Spark reads only data.meta_data, data.LFNArray, data.wmaid and data.timestamp
    if row['LFNArray']:
        # prepare metadata
        result = {'wmaid': row.wmaid, 'access_ts': row.ts * 1000, 'jobstate': row.jobstate}
        if result:
            # In case of any duplicate files in LFNArray, use set

            # if file is not empty, for each file create a new record like below
            #    ==== [{'wmaid':x, 'access_ts':x, 'jobtype':x, 'jobstate':x, 'file':x}, ...] ====
            return [{**result, **{'file': file}} for file in set(row['LFNArray']) if file]
    else:
        return []


def get_df_main_wma(spark, start_date, end_date):
    """Read WMArchive HDFS data, join with DBS for file/dataset map and make calculations

    Explanations:
        - WmaLastAccess: Last time that WMA CRAB3 jobtype had LFNArray which contains a file of the dataset
        - WmaFirstAccess: First time that WMA CRAB3 jobtype had LFNArray which contains a file of the dataset
        - WmaAccessCnt: Unique count of wmaid of WMA CRAB3 jobs had LFNArray which contains a file of the dataset
    """
    print(f"WMArchive data will be processed between {str(start_date)} - {str(end_date)}")

    # Get WMArchive data only for CRAB3 jobs
    df_raw = spark.read.schema(get_wma_raw_read_schema()).option("basePath", HDFS_WMA_WMARCHIVE) \
        .json(get_candidate_files(start_date, end_date, spark, base=HDFS_WMA_WMARCHIVE, day_delta=1)) \
        .select(["data.meta_data.*", "data.LFNArray", "data.wmaid", "metadata.timestamp"]) \
        .filter(f"""data.wmats >= {start_date.timestamp()} AND data.wmats < {end_date.timestamp()}""") \
        .filter(upper(_col('data.meta_data.jobtype')).startswith('CRAB'))

    df_rdd = df_raw.rdd.flatMap(lambda r: udf_wma_lfn_extract(r))
    df_wma_files = spark.createDataFrame(df_rdd, schema=get_wm_agg_schema()).where(_col("file").isNotNull())

    # Get file:dataset map from DBS
    df_dbs_f_d = get_df_dbs_f_d(spark)

    # Join wma LFNArray file data with DBS to find dataset names of wma files (CRAB jobs that requested)
    df_wma_and_dbs = df_wma_files.join(df_dbs_f_d, df_dbs_f_d.f_name == df_wma_files.file, how='left') \
        .fillna("UnknownDatasetNameOfFiles_MonitoringTag", subset=['dataset']) \
        .select(['dataset_id', 'dataset', 'f_name', 'wmaid', 'access_ts'])

    # Calculate last and first access of files using LFNArray files
    df = df_wma_and_dbs.groupby(['dataset_id']) \
        .agg(_max(_col('access_ts')).cast(LongType()).alias('WmaLastAccess'),
             _min(_col('access_ts')).cast(LongType()).alias('WmaFirstAccess'),
             first(_col('dataset_id')).cast(LongType()).alias('Id'),
             countDistinct(_col('wmaid')).cast(LongType()).alias('WmaAccessCnt'),
             ) \
        .withColumnRenamed('dataset_id', 'DatasetId') \
        .withColumn('WmaJobtype', lit('CRAB')) \
        .select(['DatasetId', 'WmaJobtype', 'WmaLastAccess', 'WmaFirstAccess', 'WmaAccessCnt'])

    print('WMA Schema:')
    df.printSchema()
    return df


# =====================================================================================================================
#                     Rucio Data Preparation
# =====================================================================================================================

def get_df_rses(spark):
    """Get pandas dataframe of RSES
    """
    df_rses = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_RSES) \
        .filter(_col('DELETED_AT').isNull()) \
        .withColumn('rse_id', lower(_hex(_col('ID')))) \
        .withColumn('rse_tier', _split(_col('RSE'), '_').getItem(0)) \
        .withColumn('rse_country', _split(_col('RSE'), '_').getItem(1)) \
        .withColumn('rse_kind',
                    when((_col("rse").endswith('Temp') | _col("rse").endswith('temp') | _col("rse").endswith('TEMP')),
                         'temp')
                    .when((_col("rse").endswith('Test') | _col("rse").endswith('test') | _col("rse").endswith('TEST')),
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
        .withColumn('rse_id', lower(_hex(_col('RSE_ID')))) \
        .withColumn('f_size_replicas', _col('BYTES').cast(LongType())) \
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('ACCESSED_AT', 'rep_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'rep_created_at') \
        .filter(_col('SCOPE') == 'cms') \
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
        .filter(_col('DELETED_AT').isNull()) \
        .filter(_col('HIDDEN') == '0') \
        .filter(_col('SCOPE') == 'cms') \
        .filter(_col('DID_TYPE') == 'F') \
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('ACCESSED_AT', 'dids_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'dids_created_at') \
        .withColumn('f_size_dids', _col('BYTES').cast(LongType())) \
        .select(['f_name', 'f_size_dids', 'dids_accessed_at', 'dids_created_at'])


def get_enr_with_rse_info(df_replicas, df_dids_files, df_dbs_f_d, df_rses):
    """Combines columns to get filled and correct values from join of DIDS and REPLICAS

    Firstly, REPLICAS size value will be used. If there are files with no size values, DIDS size values will be used:
    see 'when' function order. For accessed_at and created_at, their max values will be got.

    Columns: file, rse_id, accessed_at, f_size, created_at

    df_file_rse_ts_size: files and their rse_id, size and access time are completed
    """

    # Left join of df_replicas and df_dids_files to fill the RSE_ID, f_size and accessed_at, created_at for all files
    # Be aware that there are 2 columns for each f_size, accessed_at, created_at which will be combined below
    df_replicas_j_dids = df_replicas.join(df_dids_files, ['f_name'], how='left')

    # f_size is not NULL, already verified.
    # df_file_rse_ts_size.filter(col('f_size').isNull()).limit(5).toPandas()
    df_file_rse_ts_size = df_replicas_j_dids \
        .withColumn('f_size',
                    when(_col('f_size_replicas').isNotNull(), _col('f_size_replicas'))
                    .when(_col('f_size_dids').isNotNull(), _col('f_size_dids'))
                    ) \
        .withColumn('accessed_at',
                    greatest(_col('dids_accessed_at'), _col('rep_accessed_at'))
                    ) \
        .withColumn('created_at',
                    greatest(_col('dids_created_at'), _col('rep_created_at'))
                    ) \
        .select(['f_name', 'rse_id', 'accessed_at', 'f_size', 'created_at'])

    # Left join df_file_rse_ts_size and df_dbs_f_d to get dataset names of files.
    # In short: adds 'dataset' names to 'df_file_rse_ts_size' dataframe by joining DBS tables
    df_dataset_file_rse_ts_size = df_file_rse_ts_size \
        .join(df_dbs_f_d, ['f_name'], how='left') \
        .fillna("UnknownDatasetNameOfFiles_MonitoringTag", subset=['dataset']) \
        .select(['dataset_id', 'dataset', 'f_name', 'rse_id', 'accessed_at', 'created_at', 'f_size'])

    # Add RSE type, name, kind, tier, country by joining RSE ID
    df_enr_with_rse_info = df_dataset_file_rse_ts_size.join(df_rses, ['rse_id'], how='left') \
        .select(['dataset_id', 'dataset', 'f_name', 'rse_id', 'accessed_at', 'created_at', 'f_size',
                 'RSE', 'RSE_TYPE', 'rse_tier', 'rse_country', 'rse_kind'])
    return df_enr_with_rse_info


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
        .agg(_sum(_col('f_size')).alias('SizeInRseBytes'),
             _max(_col('accessed_at')).alias('LastAccessInRse'),
             _count(lit(1)).alias('FileCnt'),
             _sum(when(_col('accessed_at').isNull(), 0).otherwise(1)).alias('AccessedFileCnt'),
             first(_col('dataset_id')).alias('dataset_id'),
             first(_col('RSE_TYPE')).alias('RseType'),
             first(_col('RSE')).alias('RSE'),
             first(_col('rse_tier')).alias('rse_tier'),
             first(_col('rse_country')).alias('rse_country'),
             first(_col('rse_kind')).alias('RseKind'),
             ) \
        .withColumnRenamed('dataset', 'Dataset') \
        .select(['dataset_id', 'RseType', 'RseKind', 'RSE', 'Dataset', 'SizeInRseBytes',
                 'LastAccessInRse', 'FileCnt', 'AccessedFileCnt', ])


def get_df_main_rucio(spark):
    """Get dataframe of datasets not read since N months for main htmls.

    Get last access of dataframe in all RSE(s)
    """
    # Raw dfs
    df_rses = get_df_rses(spark)
    df_dbs_f_d = get_df_dbs_f_d(spark)
    df_replicas = get_df_replicas(spark)
    df_dids_files = get_df_dids_files(spark)
    # Aggregated dfs
    df_enr_with_rse_info = get_enr_with_rse_info(df_replicas, df_dids_files, df_dbs_f_d, df_rses)
    df_sub_rse_details = get_df_sub_rse_details(df_enr_with_rse_info)

    # Filter only DISK and prod RSES(not test and temp rses)
    df_sub_rse_details = df_sub_rse_details \
        .filter(_col('RseType') == 'DISK') \
        .filter(_col('RseKind') == 'prod')

    # Order of the select is important
    df = df_sub_rse_details \
        .groupby(['RseType', 'Dataset']) \
        .agg(_max(_col('SizeInRseBytes')).cast(LongType()).alias('Max'),
             _min(_col('SizeInRseBytes')).cast(LongType()).alias('Min'),
             _avg(_col('SizeInRseBytes')).cast(LongType()).alias('Avg'),
             _sum(_col('SizeInRseBytes')).cast(LongType()).alias('Sum'),
             _max(_col('LastAccessInRse')).cast(LongType()).alias('RucioLastAccess'),
             first(_col('dataset_id')).cast(LongType()).alias('DatasetId'),
             ) \
        .select(['DatasetId', 'RucioLastAccess', 'Max', 'Min', 'Avg', 'Sum'])

    print('Rucio Schema:')
    df.printSchema()

    # ['DatasetId', 'LastAccess', 'Max', 'Min', 'Avg', 'Sum']
    return df


def get_parent_dataset_calculations(spark, df_rucio_and_wma):
    """Calculate parent datasets' children's access count

    To better understand the logic, please hava a look https://gist.github.com/mrceyhun/b2b13dab8bc401d7f6b0e6035866d154
    """
    # WmaAccessCnt0 will be used to calculate higher hierarchy of datasets(grand parents, etc.) iteratively
    df_rucio_and_wma = df_rucio_and_wma.select(['DatasetId', 'WmaAccessCnt']) \
        .withColumnRenamed('WmaAccessCnt', 'WmaAccessCnt0')

    df_dataset_parents = get_all_parents_of_datasets(spark=spark)
    df = df_dataset_parents.join(df_rucio_and_wma, ['DatasetId'], 'left').fillna(0, ['WmaAccessCnt0'])

    # hierarchy rank is at most 3, NANOAOD, MINIAOD, AOD, RAW
    for i in range(5):
        df_tmp = df.groupby('ParentDatasetId').agg(_sum("WmaAccessCnt" + str(i)).alias('WmaAccessCnt' + str(i + 1))) \
            .withColumnRenamed('ParentDatasetId', 'DatasetId')
        df = df.join(df_tmp, ['DatasetId'], 'left').fillna(0, ['WmaAccessCnt' + str(i + 1)])
    df = df.withColumn('WmaTotalAccessCnt', sum(df[c] for c in df.columns if c.startswith('WmaAccessCnt')))
    return df.select(['DatasetId', 'WmaTotalAccessCnt'])


# =====================================================================================================================
#                     Join WMArchive and Rucio
# =====================================================================================================================
def get_df_main(spark, df_rucio, df_wma):
    """Joins WMArchive results and Rucio results, and then adds DBS tags to datasets"""

    # To reduce the Spark memory heap, we used DatasetId as common column instead of dataset name
    df_rucio_and_wma = df_rucio.join(df_wma, ['DatasetId'], how='outer')
    df_parent_children_acc_cnt = get_parent_dataset_calculations(spark, df_rucio_and_wma)
    df = df_rucio_and_wma.join(df_parent_children_acc_cnt, ['DatasetId'], how='left')

    # Important logic,
    #   - LastAccess will be filled firstly with WmaLastAccess and then with RucioLastAccess
    # We can check easily if data is coming from Wma by using: _exists_: data.WmaJobtype
    # We're using this logic since we're interested in mainly user(CRAB) jobs which came from WMArchive
    df = df.withColumn('LastAccess',
                       when(_col('WmaLastAccess').isNotNull(), _col('WmaLastAccess'))
                       .otherwise(_col('RucioLastAccess')))
    # Add dataset additional info like tier, size, campaign etc.
    df_ds_general_info = get_df_ds_general_info(spark)
    df = df.join(df_ds_general_info, ['DatasetId'], how='left') \
        .withColumnRenamed('DatasetId', 'Id') \
        .select(['Id', 'Dataset', 'LastAccess', 'LastCreation', 'DatasetAccessType', 'RealSize', 'TotalFileCnt',
                 'IsDatasetValid', 'TierName', 'PhysicsGroupName', 'AcquisitionEraName',
                 'RucioLastAccess', 'Max', 'Min', 'Avg', 'Sum',
                 'WmaJobtype', 'WmaLastAccess', 'WmaFirstAccess', 'WmaAccessCnt', 'WmaTotalAccessCnt'])

    df = df.fillna(value=NULL_STR_TYPE_COLUMN_VALUE, subset=STR_TYPE_COLUMNS)
    print('Main Schema:')
    df.printSchema()
    return df


# =====================================================================================================================
#                     Send data to ES
# =====================================================================================================================
def drop_nulls_in_dict(d):  # d: dict
    """Drops the dict key if the value is None

    ES mapping does not allow None values and drops the document completely.
    """
    return {k: v for k, v in d.items() if v is not None}  # dict


@click.command()
@click.option("--start_date", default=None, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--end_date", default=None, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--es_conf", required=True, help="ES credentials json file, see es_opensearch: CMSSpark.src/python")
@click.option("--index", required=True, help="Es index name", default="test-dima")
@click.option("--batch_size", type=click.INT, required=False, help="ES producer chunk size", default=1000)
@click.option("--test", is_flag=True, default=False, required=False,
              help="It will send only 10 documents to ElasticSearch. "
                   "[!Attention!] Please provide test/training AMQ topic.")
def main(start_date, end_date, index, es_conf, batch_size, test):
    info_logs()
    spark = get_spark_session(app_name='wmarchive_crab_file_access')

    # if any of them null, iterate all data till today 00:00
    if not (start_date and end_date):
        start_date = datetime(2000, 1, 1)
        today = date.today()
        end_date = datetime(today.year, today.month, today.day)
    start_date = start_date.replace(tzinfo=timezone.utc)
    end_date = end_date.replace(tzinfo=timezone.utc)

    df_wma = get_df_main_wma(spark, start_date, end_date)
    df_rucio = get_df_main_rucio(spark)
    df = get_df_main(spark, df_rucio, df_wma)
    # spark.sparkContext.addPyFile('/eos/user/c/cuzunogl/CMSSpark/src/python/es_opensearch.zip')
    # spark.sparkContext.addFile(es_conf)

    total_size = 0
    for part in df.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
        part_size = len(part)
        print(f"Length of partition: {part_size}")
        es = EsInterface(es_conf, logging_level=logging.WARNING)
        es.post_bulk(index=index, data=part, metadata={"timestamp": round(time.time() * 1000)},
                     is_daily_index=True, batch_size=batch_size)
        total_size += part_size
    print(f"Total document size: {total_size}")


if __name__ == '__main__':
    main()

# TODO: temporary mapping creaytion
# - ES producer is not ready, there are python "elasticsearch" module version differences in Spark nodes,
#   so we cannot create mapping via python rest call for now.
# - ES OpenSearch index templates is not working as expected with current mappings.
# - Because of these issues, for this unfinished task, we can create mapping via curl for now:

# curl -X PUT --negotiate -u : https://es-cms1.cern.ch/es/_index_template/test-dima -H 'Content-Type: application/json' -d'
# {
#   "index_patterns": [
#     "test-dima-*"
#   ],
#   "template": {
#     "aliases": {
#       "test-dima": {}
#     },
#     "settings": {
#       "number_of_shards": 2,
#       "number_of_replicas": 1
#     },
#     "mappings": {
#       "properties": {
#         "LastCreation": {
#           "type": "date",
#           "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_second"
#         },
#         "LastAccess": {
#           "type": "date",
#           "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
#         },
#         "WmaLastAccess": {
#           "type": "date",
#           "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
#         },
#         "WmaFirstAccess": {
#           "type": "date",
#           "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
#         },
#         "metadata": {
#           "properties": {
#             "timestamp": {
#               "type": "date",
#               "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
#             }
#           }
#         }
#       }
#     }
#   }
# }
# '
