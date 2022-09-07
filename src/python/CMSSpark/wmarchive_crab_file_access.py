#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : wmarchive_crab_file_access.py
Author      : Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : Generates last/first access of datasets using WMArchive data belonging CRAB jobs

How:
  - Only filter is that meta_data.jobtype should start with CRAB for the aim of just to get user jobs' accesses to files
  - data.meta_data.ts is used as file access time
  - files' datasets are extracted from join with DBS tables and additional DBS dataset info is placed in each row
  - Main calculation is to get LastAccess and FirstAccess of datasets which are only accessed by user jobs
"""

# system modules
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timezone

import click
from pyspark.sql.functions import (
    col as _col,
    countDistinct,
    first,
    max as _max,
    min as _min,
    sum as _sum,
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session, get_candidate_files

# CMSMonitoring modules
try:
    from CMSMonitoring.StompAMQ7 import StompAMQ7
except ImportError:
    print("ERROR: Could not import StompAMQ")
    sys.exit(1)

# global variables
_DEFAULT_HDFS_FOLDER = "/project/monitoring/archive/wmarchive/raw/metric"
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]

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


def info_logs():
    """Print info some important variables for Spark job"""
    print("[INFO] Used HDFS folders")
    print(f"[INFO] _DEFAULT_HDFS_FOLDER :{_DEFAULT_HDFS_FOLDER}")
    print(f"[INFO] HDFS_RUCIO_RSES :{HDFS_RUCIO_RSES}")
    print(f"[INFO] HDFS_RUCIO_REPLICAS :{HDFS_RUCIO_REPLICAS}")
    print(f"[INFO] HDFS_RUCIO_DIDS :{HDFS_RUCIO_DIDS}")
    print(f"[INFO] HDFS_DBS_DATASETS :{HDFS_DBS_DATASETS}")
    print(f"[INFO] HDFS_DBS_FILES :{HDFS_DBS_FILES}")
    print(f"[INFO] HDFS_DBS_DATA_TIERS :{HDFS_DBS_DATA_TIERS}")
    print(f"[INFO] HDFS_DBS_PHYSICS_GROUPS :{HDFS_DBS_PHYSICS_GROUPS}")
    print(f"[INFO] HDFS_DBS_ACQUISITION_ERAS :{HDFS_DBS_ACQUISITION_ERAS}")
    print(f"[INFO] HDFS_DBS_DATASET_ACCESS_TYPES :{HDFS_DBS_DATASET_ACCESS_TYPES}")


def get_schema():
    """Final schema for LFNArray files"""
    return StructType(
        [
            StructField('wmaid', StringType(), nullable=False),
            StructField('access_ts', LongType(), nullable=False),
            StructField('jobtype', StringType(), nullable=False),
            StructField('jobstate', StringType(), nullable=True),
            StructField('file', StringType(), nullable=False),
        ]
    )


def get_raw_schema():
    """Raw schema for reading WMArchive data

    WMArchive schema had changed in years, we need to use a custom schema while reading it to not get errors like
    duplicate columns error
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


def udf_lfn_extract(row):
    """
    Borrowed from wmarchive.py

    Helper function to extract useful data from WMArchive records.
    Returns list of files from LFNArray with its metadata
    """
    # Spark reads only data.meta_data, data.LFNArray, data.wmaid and data.timestamp
    if row['jobtype'] and row['LFNArray']:
        # prepare metadata
        result = {'wmaid': row.wmaid, 'access_ts': row.ts * 1000, 'jobtype': row.jobtype, 'jobstate': row.jobstate}
        if result:
            # if file is not empty, for each file create a new record
            return [{**result, **{'file': file}} for file in row['LFNArray'] if file]
    else:
        return []


def get_df_ds_general_info(spark):
    """Calculate real size and total file counts of dataset: RealSize, TotalFileCnt
    """
    dbs_files = spark.read.format('avro').load(HDFS_DBS_FILES).select(['DATASET_ID', 'FILE_SIZE',
                                                                       'LOGICAL_FILE_NAME', 'CREATION_DATE'])
    dbs_datasets = spark.read.format('avro').load(HDFS_DBS_DATASETS).select(['DATASET_ID'])
    dbs_data_tiers = spark.read.format('avro').load(HDFS_DBS_DATA_TIERS)
    dbs_physics_group = spark.read.format('avro').load(HDFS_DBS_PHYSICS_GROUPS)
    dbs_acquisition_era = spark.read.format('avro').load(HDFS_DBS_ACQUISITION_ERAS)
    dbs_dataset_access_type = spark.read.format('avro').load(HDFS_DBS_DATASET_ACCESS_TYPES)
    dbs_datasets_file_info = dbs_datasets.join(dbs_files, ['DATASET_ID'], how='left') \
        .groupby('DATASET_ID') \
        .agg(_sum('FILE_SIZE').alias('RealSize'),
             countDistinct(_col('LOGICAL_FILE_NAME')).alias('TotalFileCnt'),
             _max('CREATION_DATE').alias('LastCreationTs'),
             ) \
        .select(['DATASET_ID', 'RealSize', 'TotalFileCnt', 'LastCreationTs'])

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
        .select(['Id', 'RealSize', 'TotalFileCnt', 'LastCreationTs', 'IsDatasetValid', 'TierName', 'PhysicsGroupName',
                 'AcquisitionEraName', 'DatasetAccessType'])


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
#                     Send data with STOMP AMQ
# =====================================================================================================================
def credentials(f_name):
    """Reads AMQ credentials JSON file and returns its python dict object.

    See required keys in `send_to_amq` function
    """
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
        stomp_amq = StompAMQ7(username=username, password=password, producer=producer, topic=topic,
                              key=ckey, cert=cert, validation_schema=None, host_and_ports=[(host, port)],
                              loglevel=logging.WARNING)
        # Slow: stomp_amq.send_as_tx(chunk, docType=doc_type)
        #
        for chunk in to_chunks(data, batch_size):
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
@click.option("--start_date", default=None, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--end_date", default=None, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--creds", required=True, help="amq credentials json file")
@click.option("--amq_batch_size", type=click.INT, required=False, help="AMQ producer chunk size", default=1000)
@click.option("--test", is_flag=True, default=False, required=False,
              help="It will send only 10 documents to ElasticSearch. "
                   "[!Attention!] Please provide test/training AMQ topic.")
def main(start_date, end_date, creds, amq_batch_size, test):
    info_logs()
    spark = get_spark_session(app_name='wmarchive_crab_file_access')

    # if any of them null, iterate all data till today 00:00
    if not (start_date and end_date):
        start_date = datetime(2000, 1, 1)
        today = date.today()
        end_date = datetime(today.year, today.month, today.day)
    start_date = start_date.replace(tzinfo=timezone.utc)
    end_date = end_date.replace(tzinfo=timezone.utc)

    print(f"WMArchive data will be processed between {str(start_date)} - {str(end_date)}")

    # Get WMArchive data
    df_raw = spark.read.schema(get_raw_schema()).option("basePath", _DEFAULT_HDFS_FOLDER) \
        .json(get_candidate_files(start_date, end_date, spark, base=_DEFAULT_HDFS_FOLDER, day_delta=1)) \
        .select(["data.meta_data.*", "data.LFNArray", "data.wmaid", "metadata.timestamp"]) \
        .filter(f"""data.wmats >= {start_date.timestamp()} AND data.wmats < {end_date.timestamp()}""")

    df_rdd = df_raw.rdd.flatMap(lambda r: udf_lfn_extract(r))
    df_wma_files = spark.createDataFrame(df_rdd, schema=get_schema()).where(_col("file").isNotNull())

    # Get file:dataset map from DBS
    df_dbs_f_d = get_df_dbs_f_d(spark)

    # Join wma LFNArray file data with DBS to find dataset names of files
    df_wma_and_dbs = df_wma_files.join(df_dbs_f_d, df_dbs_f_d.f_name == df_wma_files.file, how='left') \
        .fillna("UnknownDatasetNameOfFiles_MonitoringTag", subset=['dataset']) \
        .select(['access_ts', 'dataset_id', 'dataset'])

    # Calculate last and first access of files using LFNArray files
    df = df_wma_and_dbs.groupby(['dataset']) \
        .agg(_max(_col('access_ts')).cast(LongType()).alias('LastAccessTs'),
             _min(_col('access_ts')).cast(LongType()).alias('FirstAccessTs'),
             first(_col('dataset_id')).cast(LongType()).alias('Id'),
             ) \
        .withColumnRenamed('dataset', 'Dataset') \
        .select(['Id', 'Dataset', 'LastAccessTs', 'FirstAccessTs'])

    # Add dataset additional info like tier, size, campaign etc.
    df_ds_general_info = get_df_ds_general_info(spark)

    # main dataframe
    df = df.join(df_ds_general_info, ['Id'], how='left') \
        .select(['Id', 'Dataset', 'LastAccessTs', 'FirstAccessTs',
                 'RealSize', 'TotalFileCnt', 'IsDatasetValid', 'TierName', 'PhysicsGroupName', 'AcquisitionEraName',
                 'DatasetAccessType'])

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
