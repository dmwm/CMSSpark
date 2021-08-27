#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
"""Get last access timeS of datasets by joining Rucio's REPLICAS, DIDS and CONTENTS tables"""
import pickle
import sys
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count as _count,
    countDistinct,
    hex as _hex,
    lit,
    lower,
    max as _max,
    round as _round,
    sum as _sum,
    when,
)
from pyspark.sql.types import (
    LongType,
)

pd.options.display.float_format = "{:,.8f}".format
pd.set_option("display.max_colwidth", None)

HDFS_RUCIO_CONTENTS = "/project/awg/cms/rucio_contents/*/part*.avro"
HDFS_RUCIO_DIDS = "/project/awg/cms/rucio_dids/*/part*.avro"
# HDFS_RUCIO_REPLICAS = f"/project/awg/cms/rucio/{datetime.today().strftime('%Y-%m-%d')}/replicas/part*.avro"
HDFS_RUCIO_REPLICAS = f"/project/awg/cms/rucio/2021-08-26/replicas/part*.avro"


def get_ts_thresholds():
    """Returns unix timestamps of 3, 6 and 12 months ago"""
    timestamps = {}
    for num_month in [3, 6, 12]:
        dt = datetime.today() + relativedelta(months=-num_month)
        timestamps[num_month] = int(datetime(dt.year, dt.month, dt.day).timestamp()) * 1000
    return timestamps


def get_disk_rse_ids():
    """Get rse:rse_id map from pickle file

    TODO: Get rse:rse_id map via Rucio python library. I could not run Rucio python library unfortunately.


    Used code in LxPlus (author: David Lange):
    ```py
#!/usr/bin/env python
from subprocess import Popen,PIPE
import os,sys,pickle
def runCommand(comm):
    p = Popen(comm,stdout=PIPE,stderr=PIPE,shell=True)
    pipe=p.stdout.read()
    errpipe=p.stderr.read()
    tupleP=os.waitpid(p.pid,0)
    eC=tupleP[1]
    return eC,pipe.decode(encoding='UTF-8'),errpipe.decode(encoding='UTF-8')
comm="rucio list-rses"
ec,cOut,cErr = runCommand(comm)
rses={}
for l in cOut.split():
    rse=str(l.strip())
    print(rse)
    comm="rucio-admin rse info "+rse
    ec2,cOut2,cErr2 = runCommand(comm)
    id=None
    for l2 in cOut2.split('\n'):
        if "id: " in l2:
            id=l2.split()[1]
            break
    print(id)
    rses[rse]=id
with open("rses.pickle", "wb+") as f:
  pickle.dump(rses, f)
    ```
    """
    with open("rses.pickle", "rb+") as f:
        rses = pickle.load(f)
    return list(
        dict(
            [(k, v) for k, v in rses.items() if not any(tmp in k for tmp in ["Tape", "Test", "Temp"])]
        ).values()
    )


def get_spark_session(yarn=True, verbose=False):
    """
    Get or create the spark context and session.
    """
    sc = SparkContext(appName="cms-monitoring-rucio-last_access-ts")
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def main():
    """
    TODO: Send resulting datasets in threshold groups with their sizes to ElasticSearch or Prometheus ..
    TODO: .. because resulting number of datasets are ~700K. It cannot be served in html table.

    Logic:
        - Get all files on disk by joining "replicas" and "dids" table.
        - "dids" table contains all files, but "replicas" table contains only files on disk.
        - After finding files on disk, get the parent datasets of those files by joining "contents" table.
        - "contents" table provides parent dataset of a file.
        - Dumped "contents" table in HDFS includes only `did_type='D' AND child_type='F'`
        - Then group-by by "dataset" and calculate last accessed_at of dataset by getting "max" accessed_at of files
        which belongs to same dataset.
        - Now we have last access time of dataset which is extracted from its files.
        - Finally, join the result with df_dids_datasets to get size of each dataset from "dids" table.

    Access time filter logic:
        - If "last_access_ts" is less than 3 months ago, then set "months_old" as 3,
        - If "last_access_ts" is less than 3 monthsa ago, then set "months_old" as 6,
        - If "last_access_ts" is less than 3 months ago, then set "months_old" as 12

    The result includes only the datasets whose last access time is 3 months ago.
    And the dataset whose last access time is less than 12 months or 6 months is NOT included in 3 months flag
    which means a dataset can only 1 flag(3, 6 or 12).

    """

    spark = get_spark_session()
    # - DBS terminology is used -
    # DBS:file, Rucio:file
    # DBS:block, Rucio:dataset
    # DBS:dataset, Rucio:container
    df_contents_f_to_b = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .withColumnRenamed("NAME", "block") \
        .withColumnRenamed("CHILD_NAME", "file") \
        .select(["block", "file"]) \
        .cache()

    df_contents_b_to_d = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .withColumnRenamed("NAME", "dataset") \
        .withColumnRenamed("CHILD_NAME", "block") \
        .select(["dataset", "block"]) \
        .cache()

    ###############
    disk_rse_ids = get_disk_rse_ids()
    df_replicas = spark.read.format("avro") \
        .load(HDFS_RUCIO_REPLICAS) \
        .withColumn("rse_id", lower(_hex(col("RSE_ID")))) \
        .withColumn("fsize_replicas", col("BYTES").cast(LongType())) \
        .withColumnRenamed("NAME", "file") \
        .filter(col("rse_id").isin(disk_rse_ids)) \
        .filter(col("SCOPE") == "cms") \
        .select(["file", "rse_id", "fsize_replicas"]) \
        .cache()

    #####
    df_dids_files = spark.read.format("avro") \
        .load(HDFS_RUCIO_DIDS) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "F") \
        .withColumnRenamed("NAME", "file") \
        .withColumnRenamed("ACCESSED_AT", "accessed_at") \
        .withColumn("fsize_dids", col("BYTES").cast(LongType())) \
        .select(["file", "fsize_dids", "accessed_at"]) \
        .cache()

    #######
    df_replicas_j_dids = df_replicas.join(df_dids_files, ["file"], how="left").cache()
    if df_replicas_j_dids.filter(col("fsize_dids").isNull() & col("fsize_replicas").isNull()).head():
        print("We have a problem! At least one of them should not be null !")
        sys.exit(1)

    ##############
    if df_replicas_j_dids.withColumn("bytes_ratio",
                                     when(
                                         col("fsize_dids").isNotNull() & col("fsize_replicas").isNotNull(),
                                         col("fsize_dids") / col("fsize_replicas")
                                     ).otherwise("0")
                                     ).filter((col("bytes_ratio") != 1.0) & (col("bytes_ratio") != 0)).head():
        print("We have a problem, bytes are not equal in DIDS and REPLICAS!")
        sys.exit(1)

    ########
    # BYTES_DID or BYTES_REP should not be null. Just combine them to get BYTES
    df_fsize_filled = df_replicas_j_dids.withColumn("fsize",
                                                    when(col("fsize_dids").isNotNull(), col("fsize_dids"))
                                                    .when(col("fsize_replicas").isNotNull(), col("fsize_replicas"))
                                                    ) \
        .select(['file', 'rse_id', 'accessed_at', 'fsize']) \
        .cache()

    #######
    # Change order of columns by select
    df1 = df_fsize_filled.join(df_contents_f_to_b, ["file"], how="left")
    df1 = df1.filter(col("block").isNotNull()) \
        .select(['file', 'block', 'rse_id', 'accessed_at', 'fsize']) \
        .cache()
    # df1.filter(col("bname").isNull()).count() # 57892 file do not have block name
    # print("We have a problem, a file's block is not known")
    # Total 33TB

    ##################
    df_approximate_dataset_size = df1 \
        .groupby(["block", "rse_id"]).agg(_sum(col("fsize")).alias("block_size_per_rse")) \
        .groupby(["block"]).agg(_max(col("block_size_per_rse")).alias("max_block_size_in_rse")) \
        .join(df_contents_b_to_d, ["block"], how='left') \
        .groupby(["dataset"]) \
        .agg(_round(_sum(col("max_block_size_in_rse")) / 2 ** 40, 10).alias("max_dataset_size(TB)")) \
        .cache()

    ##################
    ts_thresholds = get_ts_thresholds()

    df_block_results = df1.groupby(["block"]) \
        .agg(_max(col("accessed_at")).alias("last_access_time_of_block"),
             _sum(when(col("accessed_at").isNull(), 1).otherwise(0)).alias("#files_null_access_time"),
             _count(lit(1)).alias("#files"),
             countDistinct(col("file")).alias("#files_unique_in_block")
             ) \
        .cache()

    #####################
    df_dataset_results = df_block_results. \
        join(df_contents_b_to_d, ["block"], how='left') \
        .groupby(["dataset"]) \
        .agg(_max(col("last_access_time_of_block")).alias("last_access_time_of_dataset"),
             _sum(col("#files_null_access_time")).alias("#files_null_access_time"),
             _sum(col("#files")).alias("#files"),
             _sum(col("#files_unique")).alias("#files_unique_in_block"),
             ) \
        .withColumn('last_access_at_least_n_months_ago',
                    when(col('last_access_time_of_dataset') < ts_thresholds[12], 12)
                    .when(col('last_access_time_of_dataset') < ts_thresholds[6], 6)
                    .when(col('last_access_time_of_dataset') < ts_thresholds[3], 3)
                    .otherwise(None)
                    ) \
        .cache()

    ###################
    df_dataset_results = df_dataset_results \
        .filter(col("last_access_at_least_n_months_ago").isNotNull() & (col("#files_null_access_time") == 0))
    df = df_dataset_results.join(df_approximate_dataset_size, ["dataset"], how='left')
    # total_size_tb = df.select(["max_dataset_size(TB)"]).groupBy().sum().collect()[0][0]
    df.select(["max_dataset_size(TB)"]).groupBy().sum().collect()
    return df
