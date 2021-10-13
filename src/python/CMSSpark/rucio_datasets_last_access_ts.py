#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
"""Get last access times of datasets by joining Rucio's REPLICAS, DIDS and CONTENTS tables"""
import pickle
import sys
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg as _avg,
    col,
    count as _count,
    countDistinct,
    hex as _hex,
    lit,
    lower,
    max as _max,
    min as _min,
    round as _round,
    sum as _sum,
    when,
)
from pyspark.sql.types import (
    LongType,
)

pd.options.display.float_format = "{:,.2f}".format
pd.set_option("display.max_colwidth", None)

HDFS_RUCIO_CONTENTS = "/project/awg/cms/rucio_contents/*/part*.avro"
HDFS_RUCIO_DIDS = "/project/awg/cms/rucio_dids/*/part*.avro"


# HDFS_RUCIO_REPLICAS, see below function


def get_hdfs_rucio_replicas_path(spark):
    """Get replicas hdfs folder of today or yesterday"""
    today = f"/project/awg/cms/rucio/{datetime.today().strftime('%Y-%m-%d')}/replicas/"
    yesterday = f"/project/awg/cms/rucio/{(datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')}/replicas/"
    HDFS_RUCIO_REPLICAS_EXT = "part*.avro"
    jvm = spark._jvm
    jsc = spark._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    if fs.exists(jvm.org.apache.hadoop.fs.Path(today)):
        print(today + " exists")
        return today + HDFS_RUCIO_REPLICAS_EXT
    elif fs.exists(jvm.org.apache.hadoop.fs.Path(yesterday)):
        print(yesterday + " exists")
        return yesterday + HDFS_RUCIO_REPLICAS_EXT
    else:
        print(yesterday + " NOT exists")
        sys.exit(1)


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


def prepare_spark_dataframes(spark):
    """
        Prepare Spark dataframes using DIDS, REPLICAS AND CONTENTS Rucio tables in HDFS

        --- DBS terminology is used ---
        - file:    file in Rucio
        - block:   dataset in Rucio
        - dataset: container in Rucio
    """
    HDFS_RUCIO_REPLICAS = get_hdfs_rucio_replicas_path(spark)
    # STEP-1: Get file to block mappings from CONTENTS table as spark df
    df_contents_f_to_b = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .withColumnRenamed("NAME", "block") \
        .withColumnRenamed("CHILD_NAME", "file") \
        .select(["block", "file"]) \
        .cache()

    # STEP-2: Get block to dataset mappings from CONTENTS table as spark df
    df_contents_b_to_d = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .withColumnRenamed("NAME", "dataset") \
        .withColumnRenamed("CHILD_NAME", "block") \
        .select(["dataset", "block"]) \
        .cache()

    # STEP-3: Get ids of only Disk RSEs
    disk_rse_ids = get_disk_rse_ids()

    # STEP-4: Create spark dataframe for REPLICAS table by filtering only Disk RSEs of CMS. Importance:
    #           - provides files in Disk RSEs
    #           - provides file size in RSEs (not all of them, see Step-6)
    df_replicas = spark.read.format("avro") \
        .load(HDFS_RUCIO_REPLICAS) \
        .withColumn("rse_id", lower(_hex(col("RSE_ID")))) \
        .withColumn("fsize_replicas", col("BYTES").cast(LongType())) \
        .withColumnRenamed("NAME", "file") \
        .filter(col("rse_id").isin(disk_rse_ids)) \
        .filter(col("SCOPE") == "cms") \
        .select(["file", "rse_id", "fsize_replicas"]) \
        .cache()

    # STEP-5: Create spark dataframe for DIDS table by selecting only Files. Importance:
    #           - provides whole files in CMS
    #           - provides file access times!
    #           - provides file size (compatible with replicas, tested), (not all of them, see step-6)
    df_dids_files = spark.read.format("avro") \
        .load(HDFS_RUCIO_DIDS) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "F") \
        .withColumnRenamed("NAME", "file") \
        .withColumnRenamed("ACCESSED_AT", "accessed_at") \
        .withColumn("fsize_dids", col("BYTES").cast(LongType())) \
        .select(["file", "fsize_dids", "accessed_at"]) \
        .cache()

    # STEP-6: Left join df_replicas and df_dids_files to fill the fsize for all files. Importance:
    #           - fills fsize for all files by combining both REPLICAS values and DIDS values
    df_replicas_j_dids = df_replicas.join(df_dids_files, ["file"], how="left").cache()

    # STEP-7: Check that REPLICAS and DIDS Files join filled the file size values of all files. Yes!
    if df_replicas_j_dids.filter(col("fsize_dids").isNull() & col("fsize_replicas").isNull()).head():
        print("We have a problem! At least one of them should not be null !")
        sys.exit(1)
    # STEP-8: Check that REPLICAS and DIDS Files size values are compatible. Yes!
    elif df_replicas_j_dids.withColumn("bytes_ratio",
                                       when(
                                           col("fsize_dids").isNotNull() & col("fsize_replicas").isNotNull(),
                                           col("fsize_dids") / col("fsize_replicas")
                                       ).otherwise("0")
                                       ).filter((col("bytes_ratio") != 1.0) & (col("bytes_ratio") != 0)).head():
        print("We have a problem, bytes are not equal in DIDS and REPLICAS!")
        sys.exit(1)

    # STEP-9: fsize_dids or fsize_replicas should not be null. Just combine them to fill file sizes.
    #   - Because size of files are filled, I called "complete"
    df_files_complete = df_replicas_j_dids \
        .withColumn("fsize",
                    when(col("fsize_dids").isNotNull(), col("fsize_dids"))
                    .when(col("fsize_replicas").isNotNull(), col("fsize_replicas"))
                    ) \
        .select(['file', 'rse_id', 'accessed_at', 'fsize']) \
        .cache()

    return (df_contents_f_to_b,
            df_contents_b_to_d,
            df_replicas,
            df_dids_files,
            df_replicas_j_dids,
            df_files_complete
            )


def main():
    """
    TODO: Create html page

    Access time filter logic:
        - If "last_access_ts" is less than 3 months ago, then set "months_old" as 3,
        - If "last_access_ts" is less than 6 monthsa ago, then set "months_old" as 6,
        - If "last_access_ts" is less than 12 months ago, then set "months_old" as 12

    The result includes only the datasets whose last access time are 12, 6 or 3 months ago.
    """
    spark = get_spark_session()
    (df_contents_f_to_b,
     df_contents_b_to_d,
     df_replicas,
     df_dids_files,
     df_replicas_j_dids,
     df_files_complete) = prepare_spark_dataframes(spark)

    # ===============================================================================
    # Continue with joins
    # ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    # -------------------------------------------------------------------------------

    # --- STEP-10 / Tests to check dataframes are okay ---:
    #         df_block_file_rse.select("file").distinct().count() =  is 29921156
    #         df_block_file_rse.filter(col("file").isNull()).count() = 0
    #         df_block_file_rse.filter(col("block").isNull()).count() = 57892
    #         Above line means, we cannot extract block names of 57892 file from CONTENTS table ..
    #         .. which provides F:D and D:C mapping (file, dataset, container in Rucio terms)
    #         df_block_file_rse.filter(col("rse_id").isNull()).count() = 0
    #         df_block_file_rse.filter(col("fsize").isNull()).count() = 0
    #         We are all good, just drop null block names.

    # STEP-10: Left join df_files_complete and df_contents_f_to_b to get block names of files.
    #   - There are some files that we cannot extract their block names from CONTENTS table
    #   - So filter out them.
    df_block_file_rse = df_files_complete \
        .join(df_contents_f_to_b, ["file"], how="left") \
        .select(['block', 'file', 'rse_id', 'accessed_at', 'fsize', ]) \
        .filter(col("block").isNotNull()) \
        .cache()

    # --- STEP-11 / Tests to check dataframes are okay ---:
    #         df_all.filter(col("dataset").isNull()).count() = 280821

    # STEP-11: Left join df_block_file_rse and df_contents_b_to_d to get dataset names of blocks&files.
    #   - There are some blocks that we cannot extract their dataset names from CONTENTS table.
    #   - So filter out them.
    df_all = df_block_file_rse \
        .join(df_contents_b_to_d, ["block"], how="left") \
        .select(['dataset', 'block', 'file', 'rse_id', 'accessed_at', 'fsize']) \
        .filter(col("dataset").isNotNull()) \
        .cache()

    # STEP-12: Group by "dataset" and "rses" to calculate:
    #       - dataset_size_in_rse: total size of dataset in a RSE by summing up dataset's all files in that RSE.
    #       - `last_access_time_of_dataset_per_rse`: last access time of dataset in a RSE ...
    #           ... by getting max of file `accessed_at` field of dataset's all files in that RSE.
    #       - `#files_null_access_time_per_rse`: number of files which has NULL `accessed_at` field ...
    #           ... in each dataset in a RSE. ...
    #           ... This important to know to filter out if there is any NULL accessed_at file in calculation.
    #       - `#files_per_rse`: number of files od the dataset in that RSE
    #       - `#files_unique_per_rse`: unique count of dataset files in that RSE
    #       Final result will be like: one dataset can be in multiple RSEs and presumably ...
    #           ... it may have different sizes since a dataset may lost one of its block or file in a RSE?
    df_final_dataset_rse = df_all \
        .groupby(["dataset", "rse_id"]) \
        .agg(_sum(col("fsize")).alias("dataset_size_in_rse"),
             _max(col("accessed_at")).alias("last_access_time_of_dataset_per_rse"),
             _sum(when(col("accessed_at").isNull(), 1).otherwise(0)).alias("#files_null_access_time_per_rse"),
             _count(lit(1)).alias("#files_per_rse"),
             countDistinct(col("file")).alias("#files_unique_per_rse"),
             ) \
        .cache()

    # STEP-13: Get thresholds. They are unix timestamps which are 3, 6 and 12 months ago from today.
    ts_thresholds = get_ts_thresholds()

    # STEP-14:
    #   Filter for calculating last_accessed_at_least_{12|6|3}_months_ago columns.
    #       - To produce correct results, "last_access_time_of_dataset_per_rse" field should not be null
    #           which means a dataset's all files' accessed_at fields are filled.
    #       - And "#files_null_access_time_per_rse"==0 means that there should not be ...
    #           any file with NULL "accessed_at" field.
    # Group by dataset to get final result from all RSEs' datasets.
    #   - max_dataset_size(TB): max size of dataset in all RSEs that contain this dataset
    #   - max_dataset_size(TB): min size of dataset in all RSEs that contain this dataset
    #   - max_dataset_size(TB): avg size of dataset in all RSEs that contain this dataset
    #   - last_access_time_of_dataset: last access time of dataset in all RSEs
    df = df_final_dataset_rse \
        .filter(col("last_access_time_of_dataset_per_rse").isNotNull() &
                (col("#files_null_access_time_per_rse") == 0)
                ) \
        .groupby(["dataset"]) \
        .agg(_round(_max(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("max_dataset_size(TB)"),
             _round(_min(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("min_dataset_size(TB)"),
             _round(_avg(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("avg_dataset_size(TB)"),
             _sum(col("#files_null_access_time_per_rse")).alias("#files_null_access_time_per_dataset"),
             _max(col("last_access_time_of_dataset_per_rse")).alias("last_access_time_of_dataset"),
             ) \
        .withColumn('last_access_more_than_12_months_ago',
                    when(col('last_access_time_of_dataset') < ts_thresholds[12], 1).otherwise(0)
                    ) \
        .withColumn('last_access_more_than_6_months_ago',
                    when(col('last_access_time_of_dataset') < ts_thresholds[6], 1).otherwise(0)
                    ) \
        .withColumn('last_access_more_than_3_months_ago',
                    when(col('last_access_time_of_dataset') < ts_thresholds[3], 1).otherwise(0)
                    ) \
        .filter((col('last_access_more_than_12_months_ago') == 1) |
                (col('last_access_more_than_6_months_ago') == 1) |
                (col('last_access_more_than_3_months_ago') == 1)
                ) \
        .cache()

    # STEP-15: Find datasets which have only null accessed_at fields in its files
    df_all_null_accessed_at = df_final_dataset_rse \
        .filter(col("last_access_time_of_dataset_per_rse").isNull()) \
        .groupby(["dataset"]) \
        .agg(_round(_max(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("max_dataset_size(TB)"),
             _round(_min(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("min_dataset_size(TB)"),
             _round(_avg(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("avg_dataset_size(TB)"),
             _sum(col("#files_null_access_time_per_rse")).alias("#files_null_access_time_per_dataset"),
             _max(col("last_access_time_of_dataset_per_rse")).alias("last_access_time_of_dataset"),
             ) \
        .cache()

    # Total for not null data: not read more than 3,6,12 months which is equal to more than 3 months values.
    df.select(["max_dataset_size(TB)", "min_dataset_size(TB)", "avg_dataset_size(TB)"]).groupBy().sum().show()

    # For 12 months
    df.filter(col("last_access_more_than_12_months_ago") == 1).select(
        ["max_dataset_size(TB)", "min_dataset_size(TB)", "avg_dataset_size(TB)"]).groupBy().sum().show()
    print(df.filter(col("last_access_more_than_12_months_ago") == 1).count())

    # For 6 months
    df.filter(col("last_access_more_than_6_months_ago") == 1).select(
        ["max_dataset_size(TB)", "min_dataset_size(TB)", "avg_dataset_size(TB)"]).groupBy().sum().show()
    print(df.filter(col("last_access_more_than_6_months_ago") == 1).count())

    # For 3 months
    df.filter(col("last_access_more_than_3_months_ago") == 1).select(
        ["max_dataset_size(TB)", "min_dataset_size(TB)", "avg_dataset_size(TB)"]).groupBy().sum().show()
    print(df.filter(col("last_access_more_than_3_months_ago") == 1).count())

    # For all null accessed_at(all files) datasets
    df_all_null_accessed_at.select(
        ["max_dataset_size(TB)", "min_dataset_size(TB)", "avg_dataset_size(TB)"]).groupBy().sum().show()
    print(df_all_null_accessed_at.count())

    return df, df_all_null_accessed_at
