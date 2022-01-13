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

TODAY = datetime.today().strftime('%Y-%m-%d')
HDFS_RUCIO_CONTENTS = f"/project/awg/cms/rucio_contents/{TODAY}/part*.avro"
HDFS_RUCIO_DIDS = f"/project/awg/cms/rucio_dids/{TODAY}/part*.avro"
HDFS_RUCIO_REPLICAS = f"/project/awg/cms/rucio/{TODAY}/replicas/part*.avro"

########################################################
""" ---- Assumptions and explanations of Rucio tables ----
     --- DBS vs Rucio terminology ---
       - file:    [F]ile in Rucio
       - block:   [D]ataset in Rucio
       - dataset: [C]ontainer in Rucio

    NOT: We used DBS terminology otherwise specified implicitly!!!
    
    Mainly:
        ACCESSED_AT (last access time)                     : comes from DIDS
        BYTES (file size)                                  : combined values of DIDS and REPLICAS
        RSE ID - FILE relation                             : comes from REPLICAS
        All file-block, block-dataset membership/ownership : comes from CONTENTS
    
    Process:
        - Basically joining dataframes of Rucio tables to get: Dataset, Block, File, RseId, Access time, Size
        - In function&dataframe names: f, b, d are initial letters of file, block and dataset
        - Get access times of files
        - Get rse ids: filter only Disk Rses
        - Get blocks of files
        - Get datasets of blocks
        - Filter datasets in which all files of it contains access time; drop others
        - Calculate Dataset's last access time by getting max of its file access times
        - Group by datasets, rses to differentiate size and last access time of datasets in rses
        - Group access times of Dataset if they are accessed at least 12, 6, 3 months ago
        - Print total sizes of Datasets with their last access time groups
        
    --- Rucio table usages: ---
    1. CMS_RUCIO_PROD.CONTENTS
        Includes file dataset block relationships in only one degree.
        Provides all files that a dataset contains, or all datasets that a file belongs to
        Provides all datasets that a container contains, or all containers that a dataset belongs to
        DID_TYPE-CHILD_TYPE can be:  D-F or C-D (Rucio terminology first letters)
        Used columns: SCOPE, NAME, CHILD_SCOPE, CHILD_NAME, DID_TYPE, CHILD_TYPE
    
    2. CMS_RUCIO_PROD.DIDS
        Provides all files in CMS scope
        Provides file last access time
        Provides file size (compatibility with Rucio REPLICAS table is checked and they provide same values)
        Only files are selected: DID_TYPE=F
        Used columns: SCOPE, DID_TYPE, NAME, ACCESSED_AT, BYTES
    
    3. CMS_RUCIO_PROD.REPLICAS
        Provides files that exist in Disk RSEs, in CMS scope.
        Provides file sizes in in Disk RSEs (same with DIDS)
        Only Disk RSE_IDs are selected
        Used columns: SCOPE, RSE_ID BYTES NAME
        
Reference
 - Sqoop jobs that dumps Rucio tables to hdfs: https://github.com/dmwm/CMSKubernetes/tree/master/docker/sqoop/scripts
"""


def get_n_months_ago_epoch_msec(n_months_ago):
    """Returns integer unix timestamp msec of n months ago from today"""
    dt = datetime.today() + relativedelta(months=-n_months_ago)  # minus
    return int(datetime(dt.year, dt.month, dt.day).timestamp()) * 1000


def get_disk_rse_ids():
    """Get rse:rse_id map from pickle file and return rse_ids as list

    See CMSMONIT-324 how to fetch only Disk RSE_IDs using Rucio cli (author: David Lange)
    """
    with open("rses.pickle", "rb+") as f:
        rses = pickle.load(f)
    return list(
        dict(
            [(k, v) for k, v in rses.items() if not any(tmp in k for tmp in ["Tape", "Test", "Temp"])]
        ).values()
    )


def get_spark_session(yarn=True, verbose=False):
    """Get or create the spark context and session.
    """
    sc = SparkContext(appName="cms-monitoring-rucio-last_access-ts")
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


# ---------------------------------------------------------------------------------------
# Prepare Spark dataframes in separate functions
# ---------------------------------------------------------------------------------------


def get_df_replicas(spark, disk_rse_ids):
    """Create main replicas dataframe by selecting only Disk RSEs

    Columns selected:
        - file: file name
        - fsize_replicas: represents size of a file in REPLICAS table
        - rse_id

    df_replicas: Main replicas Spark dataframe for this script
    """
    return spark.read.format("avro").load(HDFS_RUCIO_REPLICAS) \
        .withColumn("rse_id", lower(_hex(col("RSE_ID")))) \
        .withColumn("fsize_replicas", col("BYTES").cast(LongType())) \
        .withColumnRenamed("NAME", "file") \
        .filter(col("rse_id").isin(disk_rse_ids)) \
        .filter(col("SCOPE") == "cms") \
        .select(["file", "rse_id", "fsize_replicas"]) \
        .cache()


def get_df_dids_files(spark):
    """Create spark dataframe for DIDS table by selecting only Files.

    Filters:
        - DELETED_AT not null
        - HIDDEN = 0
        - SCOPE = cms
        - DID_TYPE = F

    Columns selected:
        - file: file name
        - fsize_dids: represents size of a file in DIDS table
        - accessed_at: file last access time

    df_dids_files: All files catalog, their sizes and last access times
    """
    return spark.read.format("avro").load(HDFS_RUCIO_DIDS) \
        .filter(col("DELETED_AT").isNull()) \
        .filter(col("HIDDEN") == "0") \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "F") \
        .withColumnRenamed("NAME", "file") \
        .withColumnRenamed("ACCESSED_AT", "accessed_at") \
        .withColumn("fsize_dids", col("BYTES").cast(LongType())) \
        .select(["file", "fsize_dids", "accessed_at"]) \
        .cache()


def get_df_replicas_j_dids(df_replicas, df_dids_files):
    """Left join of df_replicas and df_dids_files to fill the RSE_ID and fsize for all files.

    Be aware that there are 2 file size columns, they will be combined in "df_f_rse_ts_size".

    Columns:
        comes from DID:       file, accessed_at, fsize_dids,
        comes from REPLICAS:  file, rse_id, fsize_replicas

    df_replicas_j_dids: Filled fsize for all files by combining both REPLICAS values and DIDS values
   """
    return df_replicas.join(df_dids_files, ["file"], how="left").cache()


def check_replicas_dids_join_is_desired(df_replicas_j_dids):
    """Check all files have size values in joined dataframe and also 2 tables' size values are equal for same files

    df_replicas_j_dids is left join of REPLICAS and DIDS
    There are 2 tables to get file sizes: REPLICAS, DIDS.
    Not all files have size information, so we'll use combined values of above tables to set sizes of all files.
    Then check:
        1. All files have size information
        2. Size values of DIDS and REPLICAS tables are equal for same files
    """

    # Check that REPLICAS and DIDS join filled the size values of all files. Yes!
    if df_replicas_j_dids.filter(
        col("fsize_dids").isNull() & col("fsize_replicas").isNull()
    ).head():
        print("We have a problem! At least one file does not have size info!")
        return False
    # Check that REPLICAS and DIDS size values are compatible. Yes!
    elif df_replicas_j_dids.withColumn(
        "bytes_ratio",
        when(
            col("fsize_dids").isNotNull() & col("fsize_replicas").isNotNull(),
            col("fsize_dids") / col("fsize_replicas")
        ).otherwise("0")
    ).filter(
        (col("bytes_ratio") != 1.0) & (col("bytes_ratio") != 0.0)
    ).head():
        print("We have a problem, bytes are not equal in DIDS and REPLICAS!")
        return False
    else:
        print("df_replicas_j_dids is desired.")
        return True


def get_df_f_rse_ts_size(df_replicas_j_dids):
    """fsize_dids or fsize_replicas should not be null. Just combine them to fill file sizes.

    Firstly, REPLICAS size value will be used. If there are files with no size values, DIDS size values will be used:
    see "when" function order.

    Columns: file, rse_id, accessed_at, fsize

    df_f_rse_ts_size: files and their rse_id, size and access time are completed
    """
    return df_replicas_j_dids.withColumn("fsize",
                                         when(col("fsize_replicas").isNotNull(), col("fsize_replicas"))
                                         .when(col("fsize_dids").isNotNull(), col("fsize_dids"))
                                         ) \
        .select(['file', 'rse_id', 'accessed_at', 'fsize']) \
        .cache()


def get_df_contents_f_to_b(spark):
    """Get all files that a block contains, or all blocks that a file belongs to.

    Columns selected: block, file

    df_contents_f_to_b: FILE-BLOCK membership/ownership map
    """
    return spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "D") \
        .filter(col("CHILD_TYPE") == "F") \
        .withColumnRenamed("NAME", "block") \
        .withColumnRenamed("CHILD_NAME", "file") \
        .select(["block", "file"]) \
        .cache()


def get_df_b_f_rse_ts_size(df_f_rse_ts_size, df_contents_f_to_b):
    """ Left join df_f_rse_ts_size and df_contents_f_to_b to get block names of files.

    Columns: block(from df_contents_f_to_b), file, rse_id, accessed_at, fsize

    df_b_f_rse_ts_size: add "block" names to "df_f_rse_ts_size" dataframe
    """
    df_b_f_rse_ts_size = df_f_rse_ts_size \
        .join(df_contents_f_to_b, ["file"], how="left") \
        .select(['block', 'file', 'rse_id', 'accessed_at', 'fsize']) \
        .cache()

    print("Stats of df_b_f_rse_ts_size before filtering out null values =>")
    stats_of_df_b_f_rse_ts_size(df_b_f_rse_ts_size)

    return df_b_f_rse_ts_size.filter(col("block").isNotNull()).cache()


def stats_of_df_b_f_rse_ts_size(df_b_f_rse_ts_size):
    """Print statistics of df_b_f_rse_ts_size
    """
    file_count = df_b_f_rse_ts_size.select("file").count()
    distinct_file_count = df_b_f_rse_ts_size.select("file").distinct().count()
    null_file_row_count = df_b_f_rse_ts_size.filter(col("file").isNull()).count()
    null_block_distinct_file_count = df_b_f_rse_ts_size.filter(col("block").isNull()).select("file").distinct().count()
    null_accessed_at_distinct_file_count = \
        df_b_f_rse_ts_size.filter(col("accessed_at").isNull()).select("file").distinct().count()
    null_block_count = df_b_f_rse_ts_size.filter(col("block").isNull()).count()
    null_rse_id_count = df_b_f_rse_ts_size.filter(col("rse_id").isNull()).count()
    null_fsize_count = df_b_f_rse_ts_size.filter(col("fsize").isNull()).count()

    print(
        f"Total file count: {file_count} \n",
        f"Total distinct file count: {distinct_file_count} \n",
        f"Null file row count: {null_file_row_count} \n",
        f"# of distinct files that have no block name: {null_block_distinct_file_count} \n",
        f"# of distinct files that have no accessed_at: {null_accessed_at_distinct_file_count} \n",
        f"Null block row count: {null_block_count} \n",
        f"Null RSE_ID row count: {null_rse_id_count} \n",
        f"Null file_size row count: {null_fsize_count} \n"
    )


def get_df_contents_b_to_d(spark):
    """Get all blocks that a dataset contains, or all datasets that a block belongs to.

    Columns selected: dataset, block

    df_contents_b_to_d: BLOCK-DATASET membership/ownership map
    """
    return spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "C") \
        .filter(col("CHILD_TYPE") == "D") \
        .withColumnRenamed("NAME", "dataset") \
        .withColumnRenamed("CHILD_NAME", "block") \
        .select(["dataset", "block"]) \
        .cache()


def get_df_d_b_f_rse_ts_size(df_b_f_rse_ts_size, df_contents_b_to_d):
    """Left join df_b_f_rse_ts_size and df_contents_b_to_d to get dataset names of blocks.

    Columns: dataset(from df_contents_b_to_d), block, file, rse_id, accessed_at, fsize

    df_d_b_f_rse_ts_size: add "dataset" name to df_b_f_rse_ts_size.
    """
    df_d_b_f_rse_ts_size = df_b_f_rse_ts_size \
        .join(df_contents_b_to_d, ["block"], how="left") \
        .select(['dataset', 'block', 'file', 'rse_id', 'accessed_at', 'fsize']) \
        .cache()

    null_dataset_distinct_block_count = df_d_b_f_rse_ts_size.filter(
        col("dataset").isNull()
    ).select("block").distinct().count()

    print("Stats of df_d_b_f_rse_ts_size before filtering out null values =>")
    print(f"Number of Distinct blocks that has no dataset name: {null_dataset_distinct_block_count}")

    return df_d_b_f_rse_ts_size.filter(col("dataset").isNotNull()).cache()


def get_df_datasets_rses_group_by(df_d_b_f_rse_ts_size):
    """Group by "dataset" and "rse_id" of df_d_b_f_rse_ts_size

    Calculations will produce below columns:
        - "dataset_size_in_rse"
                Total size of a Dataset in an RSE.
                Produced by summing up datasets' all files in that RSE.
        - "last_access_time_of_dataset_in_rse"
                Last access time of a Dataset in an RSE.
                Produced by getting max `accessed_at`(represents single file's access time) of a dataset in an RSE.
        - "#files_with_null_access_time_of_dataset_in_rse"
                Number of files count, which have NULL `accessed_at` values, of a Dataset in an RSE.
                This is important to know to filter out if there is any NULL `accessed_at` value of a Dataset.
        - "#files_of_dataset_in_rse"
                Number of files count of a Dataset in an RSE
        - "#distinct_files_of_dataset_in_rse"
                Number of unique files count of dataset in an RSE

        Final result will be like:
            One dataset can be in multiple RSEs and
            presumably it may have different sizes since a dataset may have lost some of its blocks or files in an RSE?

    Columns: dataset, rse_id,
             dataset_size_in_rse,
             last_access_time_of_dataset_in_rse,
             #files_with_null_access_time_of_dataset_in_rse,
             #files_of_dataset_in_rse,
             #distinct_files_of_dataset_in_rse


    df_datasets_rses_group_by: dataset, rse_id and their size and access time calculations
    """
    return df_d_b_f_rse_ts_size \
        .groupby(["rse_id", "dataset"]) \
        .agg(_sum(col("fsize")).alias("dataset_size_in_rse"),
             _max(col("accessed_at")).alias("last_access_time_of_dataset_in_rse"),
             _sum(
                 when(col("accessed_at").isNull(), 1).otherwise(0)
             ).alias("#files_with_null_access_time_of_dataset_in_rse"),
             _count(lit(1)).alias("#files_of_dataset_in_rse"),
             countDistinct(col("file")).alias("#distinct_files_of_dataset_in_rse"),
             ) \
        .cache()


def stats_of_null_accessed_at(df_datasets_rses_group_by):
    """Statistics of Datasets which have only null accessed_at fields in its files
    """
    df_all_null_accessed_at = df_datasets_rses_group_by \
        .filter(col("last_access_time_of_dataset_in_rse").isNull()) \
        .groupby(["dataset"]) \
        .agg(_round(_max(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("max_dataset_size_in_rses(TB)"),
             _round(_min(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("min_dataset_size_in_rses(TB)"),
             _round(_avg(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("avg_dataset_size_in_rses(TB)"),
             _sum(col("#files_with_null_access_time_of_dataset_in_rse")
                  ).alias("#files_with_null_access_time_per_dataset"),
             ) \
        .cache()
    print("Stats of Datasets which only have NULL access time =>")
    df_all_null_accessed_at.select(
        ["max_dataset_size_in_rses(TB)", "min_dataset_size_in_rses(TB)", "avg_dataset_size_in_rses(TB)"]
    ).groupBy().sum().show()
    print("Count of Datasets which only have NULL access time",
          df_all_null_accessed_at.select("dataset").distinct().count())
    del df_all_null_accessed_at


def get_df_final(df_datasets_rses_group_by):
    """Implement required filtering and calculate last_accessed_at_least_{12|6|3}_months_ago columns.

    Filters:
        - If a dataset contains EVEN a single file with null accessed_at, filter out

    Group by Dataset to get final result from all RSEs' datasets.
      - max_dataset_size_in_rses(TB): max size of dataset in all RSEs that contain this Dataset
      - min_dataset_size_in_rses(TB): min size of dataset in all RSEs that contain this Dataset
      - avg_dataset_size_in_rses(TB): avg size of dataset in all RSEs that contain this Dataset
      - last_access_time_of_dataset_in_all_rses: latest access time of dataset in all RSEs

    Logic:
        Access time filter logic in order:
        - If "last_access_time_of_dataset_in_all_rses" is less than 12 months ago,
            set "is_accessed_at_least_12_months_ago" columns as 1
        - If "last_access_time_of_dataset_in_all_rses" is less than 6 months ago,
            set "is_accessed_at_least_6_months_ago" column as 1
        - If "last_access_time_of_dataset_in_all_rses" is less than 3 months ago,
            set "is_accessed_at_least_3_months_ago" columns as 1

    The final result includes only the Datasets whose last access time is older than 3 months.
    """
    return df_datasets_rses_group_by \
        .filter((
                    col("last_access_time_of_dataset_in_rse").isNotNull()
                ) & (
                    col("#files_with_null_access_time_of_dataset_in_rse") == 0)
                ) \
        .groupby(["dataset"]) \
        .agg(_round(_max(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("max_dataset_size_in_rses(TB)"),
             _round(_min(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("min_dataset_size_in_rses(TB)"),
             _round(_avg(col("dataset_size_in_rse")) / (10 ** 12), 2).alias("avg_dataset_size_in_rses(TB)"),
             _sum(col("#files_with_null_access_time_of_dataset_in_rse")).alias("#files_null_access_time_per_dataset"),
             _max(col("last_access_time_of_dataset_in_rse")).alias("last_access_time_of_dataset_in_all_rses"),
             ) \
        .withColumn('is_accessed_at_least_12_months_ago',
                    when(
                        col('last_access_time_of_dataset_in_all_rses') < get_n_months_ago_epoch_msec(12),
                        1).otherwise(0)
                    ) \
        .withColumn('is_accessed_at_least_6_months_ago',
                    when(col('last_access_time_of_dataset_in_all_rses') < get_n_months_ago_epoch_msec(6),
                         1).otherwise(0)
                    ) \
        .withColumn('is_accessed_at_least_3_months_ago',
                    when(col('last_access_time_of_dataset_in_all_rses') < get_n_months_ago_epoch_msec(3),
                         1).otherwise(0)
                    ) \
        .filter((col('is_accessed_at_least_12_months_ago') == 1) |
                (col('is_accessed_at_least_6_months_ago') == 1) |
                (col('is_accessed_at_least_3_months_ago') == 1)
                ) \
        .cache()


def stats_df_final(df):
    """Stats of Datasets which not accessed at least 3,6,12 months ago
    """

    # 12
    print("<====== Total sizes of Datasets which are not accessed at least 12 months ======>")
    print(
        "Dataset Count :",
        df.filter(col("is_accessed_at_least_12_months_ago") == 1).distinct().count()
    )
    df.filter(col("is_accessed_at_least_12_months_ago") == 1).select(
        ["max_dataset_size_in_rses(TB)", "min_dataset_size_in_rses(TB)", "avg_dataset_size_in_rses(TB)"]
    ).groupBy().sum().show()

    # 6
    print("<====== Total sizes of Datasets which are not accessed at least 6 months ======>")
    print(
        "Dataset Count :",
        df.filter(col("is_accessed_at_least_6_months_ago") == 1).distinct().count()
    )
    df.filter(col("is_accessed_at_least_6_months_ago") == 1).select(
        ["max_dataset_size_in_rses(TB)", "min_dataset_size_in_rses(TB)", "avg_dataset_size_in_rses(TB)"]
    ).groupBy().sum().show()

    # 3
    print("<====== Total sizes of Datasets which are not accessed at least 3 months ======>")
    print(
        "Dataset Count :",
        df.filter(col("is_accessed_at_least_3_months_ago") == 1).distinct().count()
    )
    df.filter(col("is_accessed_at_least_3_months_ago") == 1).select(
        ["max_dataset_size_in_rses(TB)", "min_dataset_size_in_rses(TB)", "avg_dataset_size_in_rses(TB)"]
    ).groupBy().sum().show()


def main():
    """ Main
    """
    # Get rse ids of only Disk RSEs
    disk_rse_ids = get_disk_rse_ids()

    # === Start Spark aggregations ====

    spark = get_spark_session()
    df_replicas = get_df_replicas(spark, disk_rse_ids)
    df_dids_files = get_df_dids_files(spark)
    df_replicas_j_dids = get_df_replicas_j_dids(df_replicas, df_dids_files)
    check_replicas_dids_join_is_desired(df_replicas_j_dids)
    df_f_rse_ts_size = get_df_f_rse_ts_size(df_replicas_j_dids)
    df_contents_f_to_b = get_df_contents_f_to_b(spark)
    df_b_f_rse_ts_size = get_df_b_f_rse_ts_size(df_f_rse_ts_size, df_contents_f_to_b)
    df_contents_b_to_d = get_df_contents_b_to_d(spark)
    df_d_b_f_rse_ts_size = get_df_d_b_f_rse_ts_size(df_b_f_rse_ts_size, df_contents_b_to_d)
    df_datasets_rses_group_by = get_df_datasets_rses_group_by(df_d_b_f_rse_ts_size)
    stats_of_null_accessed_at(df_datasets_rses_group_by)
    df_final = get_df_final(df_datasets_rses_group_by)
    stats_df_final(df_final)
