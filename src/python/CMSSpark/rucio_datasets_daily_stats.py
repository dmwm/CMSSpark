#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : rucio_datasets_daily_stats.py
Author      : Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : Sends aggregated DBS and Rucio table dump results to MONIT(ElasticSearch) via AMQ

Requirements:
   - stomp.py version 7.0.0 will be provided with spark-submit as zip file
   - Specific branch of dmwm/CMSMonitoring will be provided with spark-submit as zip file
   - creds json file should be provided which contains AMQ and MONIT credentials and configurations

Assumptions:
   - No Scope filter, assumed all of them is of cms scope

References:
  - https://github.com/vkuznet/log-clustering/blob/master/workflow/workflow.py
  - https://github.com/dmwm/CMSSpark/blob/master/src/python/CMSSpark/cern_monit.py
"""

# system modules
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import click
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, lower, when,
    count as _count,
    first as _first,
    hex as _hex,
    max as _max,
    split as _split,
    sum as _sum,
)
from pyspark.sql.types import (
    LongType,
)

# CMSMonitoring modules
try:
    from CMSMonitoring.StompAMQ7 import StompAMQ7
except ImportError:
    print("ERROR: Could not import StompAMQ")
    sys.exit(1)

# global variables

# used Oracle tables
TABLES = ['REPLICAS', 'CONTENTS', 'RSES', 'FILES', 'DATASETS', 'DATA_TIERS', 'PHYSICS_GROUPS',
          'ACQUISITION_ERAS', 'DATASET_ACCESS_TYPES']

# Used to set string value for "is dataset from rucio or dbs"
BOOL_STR = {True: 'True', False: 'False'}

# Specifies that individually for Rucio and DBS if they have the information of all files of that dataset.
#   all: it(Rucio or DBS) has all files of that dataset
#   partial: it(Rucio or DBS) has some files of that dataset
#   none: it(Rucio or DBS) does not have any files of that dataset
IS_ALL_DATASET_FILES_EXISTS = {'a': 'all', 'p': 'partial', 'n': 'none'}

# Used to set dataset is locked or dynamic
IS_DATASET_LOCKED = {True: 'locked', False: 'dynamic'}

# Null string type column values will be replaced with
NULL_STR_TYPE_COLUMN_VALUE = 'UNKNOWN'

# To fill null columns of string type. Reason:
#   {"find": "terms", "field": "data.data_tier_name"} kind of ES queries do not return Null values.
STR_TYPE_COLUMNS = ['all_f_in_dbs', 'all_f_in_rucio', 'rucio_has_ds_name', 'dbs_has_ds_name', 'rucio_is_d_locked',
                    'dbs_is_d_locked', 'joint_is_d_locked', 'dataset_id', 'is_dataset_valid', 'primary_ds_id',
                    'processed_ds_id', 'prep_id', 'data_tier_id', 'data_tier_name', 'physics_group_id',
                    'physics_group_name', 'acquisition_era_id', 'acquisition_era_name', 'dataset_access_type_id',
                    'dataset_access_type', 'rse', 'rse_type', 'rse_tier', 'rse_country', 'rse_kind', ]


def get_spark_session():
    """Get or create the spark context and session."""
    sc = SparkContext(appName="cms-monitoring-rucio-daily-stats")
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def write_stats_to_eos(base_eos_dir, stats_dict):
    """Writes some Spark job statistics about datasets of DBS and Rucio to EOS file in html format"""
    html_tr_template = '''<tr><td>{KEY}</td><td>{VALUE}</td></tr>'''
    html = '<table style="color: #34568b; font-size: 14px;"><tbody>'
    for k, v in stats_dict.items():
        html += html_tr_template.format(KEY=k, VALUE=v)
        print(f"[INFO] {k}: {v}")  # for logging to k8s
    html += '</tbody></table>'

    # It'll be used in Grafana info panel: ${__to:date:YYYY-MM-DD}.html
    f_with_ext = datetime.today().strftime('%Y-%m-%d') + ".html"
    html_file = os.path.join(base_eos_dir, f_with_ext)
    try:
        if not os.path.exists(base_eos_dir):
            os.makedirs(base_eos_dir)
        with open(html_file, 'w+') as f:
            f.write(html)
    except OSError as e:
        print("[ERROR] Could not write statistics to EOS, error:", str(e))


def create_main_df(spark, hdfs_paths, base_eos_dir):
    # UTC timestamp of start hour of spark job
    ts_current_hour = int(datetime.utcnow().replace(minute=0, second=0, microsecond=0,
                                                    tzinfo=timezone.utc).timestamp() * 1000)
    # -----------------------------------------------------------------------------------------------------------------
    #                -- ==================  Prepare main Spark dataframes  ===========================

    # Get RSES id, name, type, tier, country, kind from RSES table dump
    df_rses = spark.read.format("com.databricks.spark.avro").load(hdfs_paths['RSES']) \
        .filter(col('DELETED_AT').isNull()) \
        .withColumn('replica_rse_id', lower(_hex(col('ID')))) \
        .withColumnRenamed('RSE', 'rse') \
        .withColumnRenamed('RSE_TYPE', 'rse_type') \
        .withColumn('rse_tier', _split(col('rse'), '_').getItem(0)) \
        .withColumn('rse_country', _split(col('rse'), '_').getItem(1)) \
        .withColumn('rse_kind',
                    when(col("rse").endswith('Temp'), 'temp')
                    .when(col("rse").endswith('Test'), 'test')
                    .otherwise('prod')
                    ) \
        .select(['replica_rse_id', 'rse', 'rse_type', 'rse_tier', 'rse_country', 'rse_kind'])

    # Rucio Dataset(D) refers to dbs block, so we used DBS terminology from the beginning
    df_contents_f_to_b = spark.read.format("com.databricks.spark.avro").load(hdfs_paths['CONTENTS']) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "D") \
        .filter(col("CHILD_TYPE") == "F") \
        .withColumnRenamed("NAME", "block") \
        .withColumnRenamed("CHILD_NAME", "file") \
        .select(["block", "file"])

    # Rucio Dataset(D) refers to dbs block; Rucio Container(C) refers to dbs dataset.
    # We used DBS terminology from the beginning
    df_contents_b_to_d = spark.read.format("com.databricks.spark.avro").load(hdfs_paths['CONTENTS']) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "C") \
        .filter(col("CHILD_TYPE") == "D") \
        .withColumnRenamed("NAME", "dataset") \
        .withColumnRenamed("CHILD_NAME", "block") \
        .select(["dataset", "block"])

    # Get file to dataset map
    df_contents_ds_files = df_contents_f_to_b.join(df_contents_b_to_d, ["block"], how="left") \
        .filter(col('file').isNotNull()) \
        .filter(col('dataset').isNotNull()) \
        .withColumnRenamed('dataset', 'contents_dataset') \
        .withColumn('is_d_name_from_rucio', lit(BOOL_STR[True])) \
        .select(["contents_dataset", "file", "is_d_name_from_rucio"])

    dbs_files = spark.read.format('avro').load(hdfs_paths['FILES']) \
        .withColumnRenamed('LOGICAL_FILE_NAME', 'file') \
        .withColumnRenamed('DATASET_ID', 'dbs_file_ds_id') \
        .withColumnRenamed('FILE_SIZE', 'dbs_file_size') \
        .select(['file', 'dbs_file_ds_id', 'dbs_file_size'])

    dbs_datasets = spark.read.format('avro').load(hdfs_paths['DATASETS'])

    df_dbs_ds_files = dbs_files.join(dbs_datasets.select(['DATASET_ID', 'DATASET']),
                                     dbs_files.dbs_file_ds_id == dbs_datasets.DATASET_ID, how='left') \
        .filter(col('file').isNotNull()) \
        .filter(col('DATASET').isNotNull()) \
        .withColumnRenamed('dbs_file_ds_id', 'dbs_dataset_id') \
        .withColumnRenamed('DATASET', 'dbs_dataset') \
        .withColumn('is_d_name_from_dbs', lit(BOOL_STR[True])) \
        .select(['file', 'dbs_dataset', 'is_d_name_from_dbs'])

    # Prepare replicas
    df_replicas = spark.read.format('avro').load(hdfs_paths['REPLICAS']) \
        .filter(col("SCOPE") == "cms") \
        .withColumn('replica_rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumn('replica_file_size', col('BYTES').cast(LongType())) \
        .withColumnRenamed('NAME', 'file') \
        .withColumnRenamed('ACCESSED_AT', 'replica_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'replica_created_at') \
        .withColumnRenamed('LOCK_CNT', 'lock_cnt') \
        .withColumnRenamed('STATE', 'state') \
        .select(['file', 'replica_rse_id', 'replica_file_size',
                 'replica_accessed_at', 'replica_created_at', 'lock_cnt'])

    # Create enriched file df which adds dbs file size to replicas files. Left join select only replicas files
    df_files_enriched_with_dbs = df_replicas \
        .join(dbs_files.select(['file', 'dbs_file_size']), ['file'], how='left') \
        .withColumn('joint_file_size',
                    when(col('replica_file_size').isNotNull(), col('replica_file_size'))
                    .when(col('dbs_file_size').isNotNull(), col('dbs_file_size'))
                    ) \
        .select(['file', 'replica_rse_id', 'replica_accessed_at', 'replica_created_at', 'lock_cnt',
                 'replica_file_size', 'dbs_file_size', 'joint_file_size'])

    # -----------------------------------------------------------------------------------------------------------------
    #            -- ==================  only Rucio: Replicas and Contents  ======================= --

    df_only_from_rucio = df_replicas \
        .join(df_contents_ds_files, ['file'], how='left') \
        .select(['contents_dataset', 'file', 'replica_rse_id', 'replica_file_size',
                 'replica_accessed_at', 'replica_created_at', 'is_d_name_from_rucio', 'lock_cnt'])

    # Use them in outer join
    # _max(col('replica_accessed_at')).alias('rucio_last_accessed_at'),
    # _max(col('replica_created_at')).alias('rucio_last_created_at'),

    df_only_from_rucio = df_only_from_rucio \
        .groupby(['replica_rse_id', 'contents_dataset']) \
        .agg(_sum(col('replica_file_size')).alias('rucio_size'),
             _count(lit(1)).alias('rucio_n_files'),
             _sum(
                 when(col('replica_accessed_at').isNull(), 0).otherwise(1)
             ).alias('rucio_n_accessed_files'),
             _first(col("is_d_name_from_rucio")).alias("is_d_name_from_rucio"),
             _sum(col('lock_cnt')).alias('rucio_locked_files')
             ) \
        .withColumn('rucio_is_d_locked',
                    when(col('rucio_locked_files') > 0, IS_DATASET_LOCKED[True]).otherwise(IS_DATASET_LOCKED[False])
                    ) \
        .select(['contents_dataset', 'replica_rse_id', 'rucio_size', 'rucio_n_files', 'rucio_n_accessed_files',
                 'is_d_name_from_rucio', 'rucio_locked_files', 'rucio_is_d_locked', ])

    # -----------------------------------------------------------------------------------------------------------------
    #             -- =================  only DBS: Replicas, Files, Datasets  ====================== --

    # Of course only files from Replicas processed, select only dbs related fields
    df_only_from_dbs = df_files_enriched_with_dbs \
        .select(['file', 'replica_rse_id', 'dbs_file_size', 'replica_accessed_at', 'lock_cnt']) \
        .join(df_dbs_ds_files, ['file'], how='left') \
        .filter(col('dbs_dataset').isNotNull()) \
        .select(['file', 'dbs_dataset', 'replica_rse_id', 'dbs_file_size', 'replica_accessed_at',
                 'is_d_name_from_dbs', 'lock_cnt'])

    df_only_from_dbs = df_only_from_dbs \
        .groupby(['replica_rse_id', 'dbs_dataset']) \
        .agg(_sum(col('dbs_file_size')).alias('dbs_size'),
             _count(lit(1)).alias('dbs_n_files'),
             _sum(
                 when(col('replica_accessed_at').isNull(), 0).otherwise(1)
             ).alias('dbs_n_accessed_files'),
             _first(col("is_d_name_from_dbs")).alias("is_d_name_from_dbs"),
             _sum(col('lock_cnt')).alias('dbs_locked_files')
             ) \
        .withColumn('dbs_is_d_locked',
                    when(col('dbs_locked_files') > 0, IS_DATASET_LOCKED[True])
                    .otherwise(IS_DATASET_LOCKED[False])
                    ) \
        .select(['dbs_dataset', 'replica_rse_id', 'dbs_size', 'dbs_n_files', 'dbs_n_accessed_files',
                 'is_d_name_from_dbs', 'dbs_locked_files', 'dbs_is_d_locked'])

    # Full outer join of Rucio and DBS to get all dataset-file maps
    df_dataset_file_map_enr = df_contents_ds_files.join(df_dbs_ds_files, ['file'], how='full')

    # -----------------------------------------------------------------------------------------------------------------
    #               -- ======  check files do not have dataset name  ============ --

    # Check Replicas files do not have dataset name in Contents, DBS or both
    x = df_replicas.join(df_dataset_file_map_enr, ['file'], how='left') \
        .select(['contents_dataset', 'dbs_dataset', 'file'])

    y_contents = x.filter(col('contents_dataset').isNull())
    z_dbs = x.filter(col('dbs_dataset').isNull())
    t_both = x.filter(col('contents_dataset').isNull() & col('dbs_dataset').isNull())
    stats_dict = {
        "Replicas files do not have dataset name in Contents": y_contents.select('file').distinct().count(),
        "Replicas files do not have dataset name in DBS": z_dbs.select('file').distinct().count(),
        "Replicas files do not have dataset name neither in Contents nor DBS": t_both.select('file').distinct().count()
    }
    write_stats_to_eos(base_eos_dir, stats_dict)
    del x, y_contents, z_dbs, t_both

    # -----------------------------------------------------------------------------------------------------------------
    #              -- ======  joint Rucio and DBS: Replicas, Contents, Files, Datasets  ============ --

    # Main aim is to get all datasets of files
    df_dataset_file_map_enr = df_dataset_file_map_enr \
        .withColumn("dataset",
                    when(col("contents_dataset").isNotNull(), col("contents_dataset"))
                    .when(col("dbs_dataset").isNotNull(), col("dbs_dataset"))
                    ) \
        .withColumn("is_ds_from_rucio", when(col("is_d_name_from_rucio").isNotNull(), 1).otherwise(0)) \
        .withColumn("is_ds_from_dbs", when(col("is_d_name_from_dbs").isNotNull(), 1).otherwise(0)) \
        .select(['dataset', 'file', 'is_ds_from_dbs', 'is_ds_from_rucio'])

    df_joint_ds_files = df_files_enriched_with_dbs \
        .select(['file', 'replica_rse_id', 'replica_accessed_at', 'replica_created_at',
                 'joint_file_size', 'lock_cnt']) \
        .join(df_dataset_file_map_enr, ['file'], how='left') \
        .filter(col('dataset').isNotNull()) \
        .select(['dataset', 'file', 'is_ds_from_dbs', 'is_ds_from_rucio',
                 'replica_rse_id', 'replica_accessed_at', 'replica_created_at', 'joint_file_size', 'lock_cnt'])

    df_joint_main = df_joint_ds_files \
        .groupby(['replica_rse_id', 'dataset']) \
        .agg(_sum(col('joint_file_size')).alias('joint_size'),
             _max(col('replica_accessed_at')).alias('joint_last_accessed_at'),
             _max(col('replica_created_at')).alias('joint_last_created_at'),
             _sum(col('is_ds_from_dbs')).alias('joint_dbs_n_files'),
             _sum(col('is_ds_from_rucio')).alias('joint_rucio_n_files'),
             _count(lit(1)).alias('joint_n_files'),
             _sum(
                 when(col('replica_accessed_at').isNull(), 0).otherwise(1)
             ).alias('joint_n_accessed_files'),
             _sum(col('lock_cnt')).alias('joint_locked_files')
             ) \
        .withColumn('all_f_in_dbs',
                    when((col('joint_dbs_n_files') == 0) & (col('joint_dbs_n_files').isNull()),
                         IS_ALL_DATASET_FILES_EXISTS['n'])
                    .when(col('joint_dbs_n_files') == col('joint_n_files'), IS_ALL_DATASET_FILES_EXISTS['a'])
                    .when(col('joint_dbs_n_files') > 0, IS_ALL_DATASET_FILES_EXISTS['p'])
                    ) \
        .withColumn('all_f_in_rucio',
                    when((col('joint_rucio_n_files') == 0) & (col('joint_rucio_n_files').isNull()),
                         IS_ALL_DATASET_FILES_EXISTS['n'])
                    .when(col('joint_rucio_n_files') == col('joint_n_files'), IS_ALL_DATASET_FILES_EXISTS['a'])
                    .when(col('joint_rucio_n_files') > 0, IS_ALL_DATASET_FILES_EXISTS['p'])
                    ) \
        .withColumn('joint_is_d_locked',
                    when(col('joint_locked_files') > 0, IS_DATASET_LOCKED[True])
                    .otherwise(IS_DATASET_LOCKED[False])
                    ) \
        .withColumnRenamed("replica_rse_id", "rse_id") \
        .select(['dataset',
                 'rse_id',
                 'joint_size',
                 'joint_last_accessed_at',
                 'joint_last_created_at',
                 'joint_dbs_n_files',
                 'joint_rucio_n_files',
                 'joint_n_files',
                 'joint_n_accessed_files',
                 'all_f_in_dbs',
                 'all_f_in_rucio',
                 'joint_locked_files',
                 'joint_is_d_locked'
                 ])
    # -----------------------------------------------------------------------------------------------------------------
    #          -- ============  Dataset enrichment with Dataset tags  ============ --

    # Enrich dbs dataset with names from id properties of other tables
    dbs_data_tiers = spark.read.format('avro').load(hdfs_paths['DATA_TIERS'])
    dbs_physics_group = spark.read.format('avro').load(hdfs_paths['PHYSICS_GROUPS'])
    dbs_acquisition_era = spark.read.format('avro').load(hdfs_paths['ACQUISITION_ERAS'])
    dbs_dataset_access_type = spark.read.format('avro').load(hdfs_paths['DATASET_ACCESS_TYPES'])

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

    # -----------------------------------------------------------------------------------------------------------------
    #                       -- ============  Main: join all  ============ --

    cond_with_only_rucio = [df_joint_main.dataset == df_only_from_rucio.contents_dataset,
                            df_joint_main.rse_id == df_only_from_rucio.replica_rse_id]

    cond_with_only_dbs = [df_joint_main.dataset == df_only_from_dbs.dbs_dataset,
                          df_joint_main.rse_id == df_only_from_dbs.replica_rse_id]

    # Left joins: since df_join_main has outer join, should have all datasets of both Rucio and DBS
    df_main = df_joint_main.join(df_only_from_rucio, cond_with_only_rucio, how='left').drop('replica_rse_id')
    df_main = df_main.join(df_only_from_dbs, cond_with_only_dbs, how='left').drop('replica_rse_id')

    df_main = df_main \
        .withColumn('rucio_has_ds_name',
                    when(col('is_d_name_from_rucio').isNotNull(), col('is_d_name_from_rucio'))
                    .otherwise(BOOL_STR[False])) \
        .withColumn('dbs_has_ds_name',
                    when(col('is_d_name_from_dbs').isNotNull(), col('is_d_name_from_dbs'))
                    .otherwise(BOOL_STR[False]))

    # Remove unneeded columns by selecting specific ones
    df_main = df_main.select(['dataset', 'rse_id', 'joint_size', 'joint_last_accessed_at',
                              'joint_last_created_at', 'joint_dbs_n_files', 'joint_rucio_n_files', 'joint_n_files',
                              'joint_n_accessed_files', 'all_f_in_dbs', 'all_f_in_rucio',
                              'rucio_size', 'rucio_n_files', 'rucio_n_accessed_files', 'rucio_has_ds_name',
                              'dbs_size', 'dbs_n_files', 'dbs_n_accessed_files', 'dbs_has_ds_name',
                              'rucio_locked_files', 'rucio_is_d_locked', 'dbs_locked_files', 'dbs_is_d_locked',
                              'joint_locked_files', 'joint_is_d_locked'])

    # Add DBS dataset enrichment's to main df
    df_main = df_main.join(dbs_datasets_enr, ['dataset'], how='left')

    # Add RSES name, type, tier, country, kind to dataset
    df_main = df_main \
        .join(df_rses, df_main.rse_id == df_rses.replica_rse_id, how='left') \
        .drop('rse_id', 'replica_rse_id')

    # UTC timestamp of start hour of the spark job
    df_main = df_main.withColumn('tstamp_hour', lit(ts_current_hour))

    # Fill null values of string type columns. Null values is hard to handle in ES queries.
    df_main = df_main.fillna(value=NULL_STR_TYPE_COLUMN_VALUE, subset=STR_TYPE_COLUMNS)
    return df_main


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
@click.option("--base_hdfs_dir", required=True, help="Base HDFS path that includes Sqoop dumps")
@click.option("--base_eos_dir", required=True, help="Base EOS path to write Spark job statistics",
              default="/eos/user/c/cmsmonit/www/rucio_daily_ds_stats", )
@click.option("--amq_batch_size", type=click.INT, required=False, help="AMQ transaction batch size",
              default=100)
def main(creds, base_hdfs_dir, base_eos_dir, amq_batch_size):
    tables_hdfs_paths = {}
    for table in TABLES:
        tables_hdfs_paths[table] = f"{base_hdfs_dir}/{table}/part*.avro"

    spark = get_spark_session()
    creds_json = credentials(f_name=creds)
    df = create_main_df(spark=spark, hdfs_paths=tables_hdfs_paths, base_eos_dir=base_eos_dir)
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
