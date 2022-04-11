#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
#
# Sends aggregated DBS and Rucio table dump results to MONIT(ElasticSearch) via AMQ
#
# Requirements:
#    - stomp.py version 7.0.0 will be provided with spark-submit as zip file
#    - Specific branch of dmwm/CMSMonitoring will be provided with spark-submit as zip file
#    - creds json file should be provided which contains AMQ and MONIT credentials and configurations
#
# Ref: https://github.com/vkuznet/log-clustering/blob/master/workflow/workflow.py
# Ref: https://github.com/dmwm/CMSSpark/blob/master/src/python/CMSSpark/cern_monit.py

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import click
from pyspark import SparkContext, SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, lower, when,
    count as _count,
    hex as _hex,
    max as _max,
    sum as _sum,
)
from pyspark.sql.types import (
    LongType,
)

try:
    from CMSMonitoring.StompAMQ7 import StompAMQ7
except ImportError:
    print("ERROR: Could not import StompAMQ")
    sys.exit(1)

BASE_HDFS_DIR = f"/tmp/cmssqoop/rucio_daily_stats-{datetime.utcnow().strftime('%Y-%m-%d')}"
HDFS_RUCIO_CONTENTS = BASE_HDFS_DIR + '/CONTENTS/part*.avro'
HDFS_RUCIO_REPLICAS = BASE_HDFS_DIR + '/REPLICAS/part*.avro'
HDFS_RUCIO_RSES = BASE_HDFS_DIR + '/RSES/part*.avro'
HDFS_DBS_FILES = BASE_HDFS_DIR + '/FILES/part*.avro'
HDFS_DBS_DATASETS = BASE_HDFS_DIR + '/DATASETS/part*.avro'
HDFS_DBS_DT = BASE_HDFS_DIR + '/DATA_TIERS/part*.avro'
HDFS_DBS_PG = BASE_HDFS_DIR + '/PHYSICS_GROUPS/part*.avro'
HDFS_DBS_AE = BASE_HDFS_DIR + '/ACQUISITION_ERAS/part*.avro'
HDFS_DBS_DAT = BASE_HDFS_DIR + '/DATASET_ACCESS_TYPES/part*.avro'

# Specifies that individually for Rucio and DBS if they have the information of all files of that dataset.
#   all: it(Rucio or DBS) has all files of that dataset
#   partial: it(Rucio or DBS) has some files of that dataset
#   none: it(Rucio or DBS) does not have any files of that dataset
IS_ALL_DATASET_FILES_EXISTS = {'a': 'all', 'p': 'partial', 'n': 'none'}

# Null string type column values will be replaced with
NULL_STR_TYPE_COLUMN_VALUE = 'UNKNOWN'

# Fill null columns of string type. Reason:
#   {"find": "terms", "field": "data.data_tier_name"} kind of ES queries do not return Null values.
STR_TYPE_COLUMNS = ['dataset', 'prep_id', 'data_tier_name', 'physics_group_name', 'acquisition_era_name',
                    'dataset_access_type', 'rse_type']


def get_spark_session(pyfiles=None):
    """Get or create the spark context and session."""
    sc = SparkContext(appName="cms-monitoring-rucio-daily-stats")
    if pyfiles:
        for f in pyfiles:
            sc.addPyFile(SparkFiles.get(f))
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def create_main_df(spark):
    # Get RSES id, name and type from RSES table dump
    df_rses = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_RSES) \
        .withColumn('replica_rse_id', lower(_hex(col('ID')))) \
        .withColumnRenamed('RSE', 'rse') \
        .withColumnRenamed('RSE_TYPE', 'rse_type') \
        .select(['replica_rse_id', 'rse', 'rse_type'])
    #
    # UTC timestamp of start hour of spark job
    ts_current_hour = int(datetime.utcnow().replace(minute=0, second=0, microsecond=0,
                                                    tzinfo=timezone.utc).timestamp() * 1000)
    #
    # Rucio Dataset(D) refers to dbs block, so we used DBS terminology from the beginning
    df_contents_f_to_b = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .filter(col("DID_TYPE") == "D") \
        .filter(col("CHILD_TYPE") == "F") \
        .withColumnRenamed("NAME", "block") \
        .withColumnRenamed("CHILD_NAME", "file") \
        .select(["scope", "block", "file"])
    #
    # Rucio Dataset(D) refers to dbs block; Rucio Container(C) refers to dbs dataset.
    # We used DBS terminology from the beginning
    df_contents_b_to_d = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .filter(col("DID_TYPE") == "C") \
        .filter(col("CHILD_TYPE") == "D") \
        .withColumnRenamed("NAME", "dataset") \
        .withColumnRenamed("CHILD_NAME", "block") \
        .select(["scope", "dataset", "block"])
    #
    # Get file to dataset map
    df_contents_f_d = df_contents_f_to_b.join(df_contents_b_to_d, ["scope", "block"], how="left") \
        .withColumnRenamed('dataset', 'contents_dataset') \
        .filter(col('file').isNotNull()) \
        .filter(col('contents_dataset').isNotNull()) \
        .select(["contents_dataset", "file"])
    #
    dbs_files = spark.read.format('avro').load(HDFS_DBS_FILES) \
        .withColumnRenamed('LOGICAL_FILE_NAME', 'file') \
        .withColumnRenamed('DATASET_ID', 'dbs_file_ds_id') \
        .withColumnRenamed('FILE_SIZE', 'dbs_file_size') \
        .select(['file', 'dbs_file_ds_id', 'dbs_file_size'])
    #
    dbs_datasets = spark.read.format('avro').load(HDFS_DBS_DATASETS)
    #
    df_dbs_f_d = dbs_files.join(dbs_datasets.select(['DATASET_ID', 'DATASET']),
                                dbs_files.dbs_file_ds_id == dbs_datasets.DATASET_ID, how='left') \
        .withColumnRenamed('dbs_file_ds_id', 'dbs_dataset_id') \
        .withColumnRenamed('DATASET', 'dbs_dataset') \
        .select(['file', 'dbs_dataset']) \
        .filter(col('file').isNotNull()) \
        .filter(col('dbs_dataset').isNotNull())
    #
    df_replicas = spark.read.format('avro').load(HDFS_RUCIO_REPLICAS) \
        .withColumn('replica_rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumn('replica_file_size', col('BYTES').cast(LongType())) \
        .withColumnRenamed('NAME', 'file') \
        .withColumnRenamed('ACCESSED_AT', 'replica_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'replica_created_at') \
        .select(['scope', 'file', 'replica_rse_id', 'replica_file_size', 'replica_accessed_at', 'replica_created_at'])
    #
    # Create enriched file df which adds dbs file size to replicas files. Left join select only replicas files
    df_files_enr = df_replicas \
        .join(dbs_files.select(['file', 'dbs_file_size']), ['file'], how='left') \
        .select(['file', 'scope', 'replica_rse_id', 'replica_file_size', 'replica_accessed_at', 'replica_created_at',
                 'dbs_file_size'])
    #
    # Full outer join of Rucio and DBS to get all dataset-file maps
    df_dataset_file_map_enr = df_contents_f_d.join(df_dbs_f_d, ['file'], how='full')
    # df_dataset_file_enr.filter(col("contents_dataset").isNull() & col("dbs_dataset").isNull()).count()
    # 1245 files do not have dataset name.
    #
    df_dataset_file_map_enr = df_dataset_file_map_enr \
        .withColumn("dataset",
                    when(col("contents_dataset").isNotNull(), col("contents_dataset"))
                    .when(col("dbs_dataset").isNotNull(), col("dbs_dataset"))
                    ) \
        .withColumn("is_ds_from_dbs", when(col("dbs_dataset").isNotNull(), 1).otherwise(0)) \
        .withColumn("is_ds_from_rucio", when(col("contents_dataset").isNotNull(), 1).otherwise(0)) \
        .select(['dataset', 'file', 'is_ds_from_dbs', 'is_ds_from_rucio'])
    # df_dataset_file_map_enr.select('file').distinct().count()
    # df_dataset_file_map_enr.count()
    #
    df_files_w_ds = df_files_enr \
        .join(df_dataset_file_map_enr, ['file'], how='left')
    # df_files_w_ds.filter(col("dataset").isNull()).count()
    #
    # df_files_w_ds.groupBy('file', 'replica_rse_id', 'dataset').count().filter("count > 1").head()
    # NO data, we're good. It means 1 file is only 1 dataset-rse couple
    #
    df_files_w_ds = df_files_w_ds.filter(col('dataset').isNotNull())
    df_main = df_files_w_ds \
        .groupby(['replica_rse_id', 'dataset']) \
        .agg(_sum(col('replica_file_size')).alias('d_rucio_size'),
             _sum(col('dbs_file_size')).alias('d_dbs_size'),
             _max(col('replica_accessed_at')).alias('d_last_accessed_at'),
             _max(col('replica_created_at')).alias('d_last_created_at'),
             _sum(col('is_ds_from_dbs')).alias('n_files_in_dbs'),
             _sum(col('is_ds_from_rucio')).alias('n_files_in_rucio'),
             _count(lit(1)).alias('n_files'),
             _sum(
                 when(col('replica_accessed_at').isNull(), 0).otherwise(1)
             ).alias('n_accessed_files')
             ) \
        .withColumn('all_f_in_dbs',
                    when(col('n_files_in_dbs') == 0, IS_ALL_DATASET_FILES_EXISTS['n'])
                    .when(col('n_files_in_dbs') == col('n_files'), IS_ALL_DATASET_FILES_EXISTS['a'])
                    .when(col('n_files_in_dbs') > 0, IS_ALL_DATASET_FILES_EXISTS['p'])
                    ) \
        .withColumn('all_f_in_rucio',
                    when(col('n_files_in_rucio') == 0, IS_ALL_DATASET_FILES_EXISTS['n'])
                    .when(col('n_files_in_rucio') == col('n_files'), IS_ALL_DATASET_FILES_EXISTS['a'])
                    .when(col('n_files_in_rucio') > 0, IS_ALL_DATASET_FILES_EXISTS['p'])
                    ) \
        .select(['dataset',
                 'replica_rse_id',
                 'd_rucio_size',
                 'd_dbs_size',
                 'd_last_accessed_at',
                 'd_last_created_at',
                 'n_files',
                 'n_accessed_files',
                 'n_files_in_dbs',
                 'n_files_in_rucio',
                 'all_f_in_dbs',
                 'all_f_in_rucio',
                 ])
    # Enrich dbs dataset with names from id properties of other tables
    dbs_data_tiers = spark.read.format('avro').load(HDFS_DBS_DT)
    dbs_physics_group = spark.read.format('avro').load(HDFS_DBS_PG)
    dbs_acquisition_era = spark.read.format('avro').load(HDFS_DBS_AE)
    dbs_dataset_access_type = spark.read.format('avro').load(HDFS_DBS_DAT)
    #
    dbs_datasets_enr = dbs_datasets \
        .join(dbs_data_tiers, ['data_tier_id'], how='left') \
        .join(dbs_physics_group, ['physics_group_id'], how='left') \
        .join(dbs_acquisition_era, ['acquisition_era_id'], how='left') \
        .join(dbs_dataset_access_type, ['dataset_access_type_id'], how='left') \
        .select(['dataset', 'dataset_id', 'is_dataset_valid', 'primary_ds_id', 'processed_ds_id', 'prep_id',
                 'data_tier_id', 'data_tier_name',
                 'physics_group_id', 'physics_group_name',
                 'acquisition_era_id', 'acquisition_era_name',
                 'dataset_access_type_id', 'dataset_access_type'])
    #
    # Add DBS dataset enrichment's to main df
    df_main = df_main.join(dbs_datasets_enr, ['dataset'], how='left')
    df_main = df_main.join(df_rses, ['replica_rse_id'], how='left').drop('replica_rse_id')
    #
    # UTC timestamp of mid of the day
    df_main = df_main.withColumn('tstamp_hour', lit(ts_current_hour))
    # Fill null values of string type columns. Null values is hard to handle in ES queries.
    df_main = df_main.fillna(value=NULL_STR_TYPE_COLUMN_VALUE, subset=STR_TYPE_COLUMNS)
    return df_main


# ============================================================================
#                     Send data with STOMP AMQ
# ============================================================================
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
@click.option("--creds", required=True, help="secrets/cms-rucio-dailystats/creds.json")
@click.option("--stomp_zip", required=True, help="stomp.zip that includes specific version of stomp.py")
@click.option("--cmsmonit_zip", required=True, help="CMSMonit.zip that includes specific version of CMSMonit")
@click.option("--amq_batch_size", type=click.INT, required=False, help="AMQ transaction batch size")
def main(creds=None, stomp_zip=None, cmsmonit_zip=None, amq_batch_size=100):
    spark = get_spark_session(pyfiles=[stomp_zip, cmsmonit_zip])
    creds_json = credentials(f_name=creds)
    df = create_main_df(spark)
    print('Schema:')
    df.printSchema()
    total_size = 0
    # Iterate over list of dicts returned from spark
    for part in df.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
        part_size = len(part)
        print(f"Length of partition: {part_size}")
        send_to_amq(data=part, confs=creds_json, batch_size=amq_batch_size)
        total_size += part_size
    print(f"Total document size: {total_size}")


if __name__ == "__main__":
    main()
