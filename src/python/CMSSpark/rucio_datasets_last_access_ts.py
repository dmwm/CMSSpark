#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
"""Create html pages for datasets' access times by joining Rucio's REPLICAS, DIDS and CONTENTS tables"""

import pickle
from datetime import datetime

import click
import os
import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg as _avg,
    col,
    collect_list,
    concat_ws,
    count as _count,
    countDistinct,
    greatest,
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

TODAY = datetime.today().strftime('%Y-%m-%d')
HDFS_RUCIO_CONTENTS = "/project/awg/cms/rucio_contents/{}/part*.avro".format(TODAY)
HDFS_RUCIO_DIDS = "/project/awg/cms/rucio_dids/{}/part*.avro".format(TODAY)
HDFS_RUCIO_REPLICAS = "/project/awg/cms/rucio/{}/replicas/part*.avro".format(TODAY)
TB_DENOMINATOR = 10 ** 12

pd.options.display.float_format = "{:,.2f}".format
pd.set_option("display.max_colwidth", -1)

########################################################
""" 
---- Assumptions and explanations of Rucio tables ----
    DBS vs Rucio terminology ---
        - file:    [F]ile in Rucio
        - block:   [D]ataset in Rucio
        - dataset: [C]ontainer in Rucio

    Notes:
        - We used DBS terminology otherwise specified implicitly!!!
        - In function&dataframe names: f, b, d are initial letters of file, block and dataset
        - df_pd stands for pandas dataframe

    Why Assumptions: 
        - It's certain that final result is not 100% correct with below assumptions, but without below assumptions, ...
          ... it's impossible to produce any result.

    Assumptions for datasets not accessed since N months:
        1. Ignore files which do not have: block name, dataset name, accessed_at, size
        2. Ignore blocks which do not have: dataset name
        3. Ignore datasets even if they have a file with NULL access time
        4. Get only datasets with desired size to filter so many small datasets
        5. We may miss some datasets even if they obey the above assumptions, ...
          ... because of filtering the last access time in all RSEs. An example for dataset X:
          - in RSE A: last access is 14 months ago, in RSE B: last access is 1 months ago
          - dataset X will not appear in the main page because it's overall last access is 1 month ago.
    
    Assumptions for datasets never read:
        1. Ignore files which do not have: block name, dataset name, accessed_at, size
        2. Ignore blocks which do not have: dataset name
        3. Get only datasets such that all files of it has NULL access time
        4. Get only datasets with desired size to filter so many small datasets
        5. We may miss some datasets even if they obey the above assumptions, ...
          ... because of filtering the last access time in all RSEs. An example for dataset X:
          - in RSE A: last access is 14 months ago, in RSE B: last access is 1 months ago
          - dataset X will not appear in the main page because it's overall last access is 1 month ago.

    Data sources in Rucio tables:
        ACCESSED_AT (last access time)                     : greatest of DIDS' and REPLICAS' ACCESSED_AT
        CREATED_AT (creation time)                         : greatest of DIDS' and REPLICAS' CREATED_AT
        BYTES (file size)                                  : DIDS and REPLICAS (equal for same files)
        RSE ID - FILE relation                             : comes from REPLICAS
        All file-block, block-dataset membership/ownership : comes from CONTENTS
    

    Steps of datasets not read since N months:
        - Main aim is to get Datasets, their RSEs, last access times and sizes in RSEs.
        - All filters depends on above assumptions
        - Steps:
            1. Get files in disk RSEs, their accessed_at(DIDS and REPLICAS) and size (DIDS and REPLICAS)
            2. Get dataset names of files in RSEs (firstly files to blocks, secondly blocks to datasets)
            3. Calculate last access time of datasets (max accessed_at of all files) 
            4. Get datasets' max,min,avg sizes in all RSEs, the RSEs that it belongs to 
            5. Create sub htmls for details of dataset in a single RSE
    
    Steps of datasets never read:
        - Mostly same for the steps in "datasets not read since N months"
        - Different filtering is used as explained in the assumptions
        - Instead of accessed_at, created_at provided in the resilt

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
        Provides file last access time
        Only Disk RSE_IDs are selected
        Used columns: SCOPE, RSE_ID BYTES NAME
        
Reference
 - Sqoop jobs that dumps Rucio tables to hdfs: https://github.com/dmwm/CMSKubernetes/tree/master/docker/sqoop/scripts

"""


def get_spark_session(yarn=True, verbose=False):
    """Get or create the spark context and session.
    """
    sc = SparkContext(appName="cms-monitoring-rucio-datasets-last-access-ts")
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def get_n_months_ago_epoch_msec(n_months_ago):
    """Returns integer unix timestamp msec of n months ago from today"""
    dt = datetime.today() + relativedelta(months=-n_months_ago)  # minus
    return int(datetime(dt.year, dt.month, dt.day).timestamp()) * 1000


def get_disk_rse_ids(rse_id_name_map_pickle):
    """Get rse:rse_id map from pickle by filtering only Disk rses

    See CMSSpark/static/rucio/rse_name_id_map_pickle.py
    """
    with open(rse_id_name_map_pickle, "rb+") as f:
        rses = pickle.load(f)
    return dict([(k, v) for k, v in rses.items() if not any(tmp in k for tmp in ["Tape", "Test", "Temp"])])


def get_reverted_disk_rses_id_name_map(disk_rses_id_name_map):
    """Revert k:v to v:k"""
    return {v: k for k, v in disk_rses_id_name_map.items()}


# --------------------------------------------------------------------------------
# Intermediate dataset creation functions
# --------------------------------------------------------------------------------


def get_df_replicas(spark, disk_rse_ids):
    """Create main replicas dataframe by selecting only Disk RSEs in Rucio REPLICAS table

    Columns selected:
        - file: file name
        - fsize_replicas: represents size of a file in REPLICAS table
        - rse_id
        - rep_accessed_at
        - rep_created_at
    """
    return spark.read.format("avro").load(HDFS_RUCIO_REPLICAS) \
        .withColumn("rse_id", lower(_hex(col("RSE_ID")))) \
        .withColumn("fsize_replicas", col("BYTES").cast(LongType())) \
        .withColumnRenamed("NAME", "file") \
        .withColumnRenamed("ACCESSED_AT", "rep_accessed_at") \
        .withColumnRenamed("CREATED_AT", "rep_created_at") \
        .filter(col("rse_id").isin(disk_rse_ids)) \
        .filter(col("SCOPE") == "cms") \
        .select(["file", "rse_id", "fsize_replicas", "rep_accessed_at", "rep_created_at"]) \
        .cache()


def get_df_dids_files(spark):
    """Create spark dataframe for DIDS table by selecting only Files in Rucio DIDS table.

    Filters:
        - DELETED_AT not null
        - HIDDEN = 0
        - SCOPE = cms
        - DID_TYPE = F

    Columns selected:
        - file: file name
        - fsize_dids: represents size of a file in DIDS table
        - dids_accessed_at: file last access time
        - dids_created_at: file creation time
    """
    return spark.read.format("avro").load(HDFS_RUCIO_DIDS) \
        .filter(col("DELETED_AT").isNull()) \
        .filter(col("HIDDEN") == "0") \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "F") \
        .withColumnRenamed("NAME", "file") \
        .withColumnRenamed("ACCESSED_AT", "dids_accessed_at") \
        .withColumnRenamed("CREATED_AT", "dids_created_at") \
        .withColumn("fsize_dids", col("BYTES").cast(LongType())) \
        .select(["file", "fsize_dids", "dids_accessed_at", "dids_created_at"]) \
        .cache()


def get_df_replicas_j_dids(df_replicas, df_dids_files):
    """Left join of df_replicas and df_dids_files to fill the RSE_ID, fsize and accessed_at, created_at for all files.

    Be aware that there are 2 columns for each fsize, accessed_at, created_at
    They will be combined in get_df_file_rse_ts_size

    Columns:
        comes from DID:       file, dids_accessed_at, dids_created_at, fsize_dids,
        comes from REPLICAS:  file, rse_id, fsize_replicas, rep_accessed_at, rep_created_at
   """
    return df_replicas.join(df_dids_files, ["file"], how="left").cache()


def get_df_file_rse_ts_size(df_replicas_j_dids):
    """Combines columns to get filled and correct values from join of DIDS and REPLICAS

    Firstly, REPLICAS size value will be used. If there are files with no size values, DIDS size values will be used:
    see "when" function order. For accessed_at and created_at, their max values will be get.

    Columns: file, rse_id, accessed_at, fsize, created_at

    df_file_rse_ts_size: files and their rse_id, size and access time are completed
    """
    return df_replicas_j_dids \
        .withColumn("fsize",
                    when(col("fsize_replicas").isNotNull(), col("fsize_replicas"))
                    .when(col("fsize_dids").isNotNull(), col("fsize_dids"))
                    ) \
        .withColumn("accessed_at",
                    greatest(col("dids_accessed_at"), col("rep_accessed_at"))
                    ) \
        .withColumn("created_at",
                    greatest(col("dids_created_at"), col("rep_created_at"))
                    ) \
        .select(['file', 'rse_id', 'accessed_at', 'fsize', 'created_at']) \
        .cache()


def get_df_contents_f_to_b(spark):
    """Create a dataframe for FILE-BLOCK membership/ownership map

    This dataframe ensures unique (file, block) couples because of table schema PK:
        - CONTENTS table pk: CONSTRAINT CONTENTS_PK PRIMARY KEY (SCOPE, NAME, CHILD_SCOPE, CHILD_NAME)

    Columns selected: block, file
    """
    return spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("CHILD_SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "D") \
        .filter(col("CHILD_TYPE") == "F") \
        .withColumnRenamed("NAME", "block") \
        .withColumnRenamed("CHILD_NAME", "file") \
        .select(["block", "file"]) \
        .cache()


def get_df_block_file_rse_ts_size(df_file_rse_ts_size, df_contents_f_to_b):
    """ Left join df_file_rse_ts_size and df_contents_f_to_b to get block names of files.

    In short: adds "block" names to "df_file_rse_ts_size" dataframe

    Columns: block(from df_contents_f_to_b), file, rse_id, accessed_at, fsize
    """
    df_block_file_rse_ts_size = df_file_rse_ts_size \
        .join(df_contents_f_to_b, ["file"], how="left") \
        .select(['block', 'file', 'rse_id', 'accessed_at', 'created_at', 'fsize']) \
        .cache()

    print("Stats of df_block_file_rse_ts_size before filtering out null values =>")
    stats_of_df_block_file_rse_ts_size(df_block_file_rse_ts_size)

    return df_block_file_rse_ts_size.filter(col("block").isNotNull()).cache()


def stats_of_df_block_file_rse_ts_size(df_block_file_rse_ts_size):
    """Print statistics of df_block_file_rse_ts_size
    """
    file_count = df_block_file_rse_ts_size.select("file").count()
    distinct_file_count = df_block_file_rse_ts_size.select("file").distinct().count()
    null_file_row_count = df_block_file_rse_ts_size.filter(col("file").isNull()).count()
    null_block_distinct_file_count = df_block_file_rse_ts_size.filter(col("block").isNull()).select(
        "file").distinct().count()
    null_accessed_at_distinct_file_count = \
        df_block_file_rse_ts_size.filter(col("accessed_at").isNull()).select("file").distinct().count()
    null_block_count = df_block_file_rse_ts_size.filter(col("block").isNull()).count()
    null_rse_id_count = df_block_file_rse_ts_size.filter(col("rse_id").isNull()).count()
    null_fsize_count = df_block_file_rse_ts_size.filter(col("fsize").isNull()).count()

    print(
        "Total file count: {} \n".format(file_count),
        "Total distinct file count: {} \n".format(distinct_file_count),
        "Null file row count: {} \n".format(null_file_row_count),
        "# of distinct files that have no block name: {} \n".format(null_block_distinct_file_count),
        "# of distinct files that have no accessed_at: {} \n".format(null_accessed_at_distinct_file_count),
        "Null block row count: {} \n".format(null_block_count),
        "Null RSE_ID row count: {} \n".format(null_rse_id_count),
        "Null file_size row count: {} \n".format(null_fsize_count)
    )


def get_df_contents_b_to_d(spark):
    """Create a dataframe for BLOCK-DATASET membership/ownership map

    This dataframe ensures unique (block, dataset) couples because of table schema PK:
        - CONTENTS table pk: CONSTRAINT CONTENTS_PK PRIMARY KEY (SCOPE, NAME, CHILD_SCOPE, CHILD_NAME)

    Columns selected: dataset, block
    """
    return spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("CHILD_SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "C") \
        .filter(col("CHILD_TYPE") == "D") \
        .withColumnRenamed("NAME", "dataset") \
        .withColumnRenamed("CHILD_NAME", "block") \
        .select(["dataset", "block"]) \
        .cache()


def get_df_dataset_file_rse_ts_size(df_block_file_rse_ts_size, df_contents_b_to_d):
    """Left join df_block_file_rse_ts_size and df_contents_b_to_d to get dataset names of blocks.

    In short: adds "dataset" names to "df_block_file_rse_ts_size" dataframe and drops block from it

    Columns: dataset(from df_contents_b_to_d), file, rse_id, accessed_at, fsize
    """
    df_dataset_file_rse_ts_size = df_block_file_rse_ts_size \
        .join(df_contents_b_to_d, ["block"], how="left") \
        .select(['dataset', 'block', 'file', 'rse_id', 'accessed_at', 'created_at', 'fsize']) \
        .cache()

    null_dataset_distinct_block_count = df_dataset_file_rse_ts_size.filter(
        col("dataset").isNull()
    ).select("block").distinct().count()

    print("Stats of df_dataset_file_rse_ts_size before filtering out null values =>")
    print("Number of Distinct blocks that has no dataset name:", null_dataset_distinct_block_count)

    # Drop blocks since we got the dataset of files (our on of main aims till here)
    return df_dataset_file_rse_ts_size.filter(
        col("dataset").isNotNull()
    ).select(['dataset', 'file', 'rse_id', 'accessed_at', 'created_at', 'fsize'])


# --------------------------------------------------------------------------------
# Main dataset creation functions
# --------------------------------------------------------------------------------

def get_df_main_datasets_never_read(df_dataset_file_rse_ts_size, disk_rses_id_name_map, min_tb_limit):
    """Get never accessed datasets' dataframes"""
    reverted_disk_rses_id_name_map = get_reverted_disk_rses_id_name_map(disk_rses_id_name_map)
    df_sub_datasets_never_read = df_dataset_file_rse_ts_size \
        .groupby(["rse_id", "dataset"]) \
        .agg(_round(_sum(col("fsize")) / TB_DENOMINATOR, 5).alias("dataset_size_in_rse_tb"),
             _max(col("accessed_at")).alias("last_access_time_of_dataset_in_rse"),
             _max(col("created_at")).alias("last_create_time_of_dataset_in_rse"),
             _sum(
                 when(col("accessed_at").isNull(), 1).otherwise(0)
             ).alias("#files_with_null_access_time_of_dataset_in_rse"),
             _count(lit(1)).alias("#files_of_dataset_in_rse"),
             countDistinct(col("file")).alias("#distinct_files_of_dataset_in_rse"),
             ) \
        .filter(col("last_access_time_of_dataset_in_rse").isNull()) \
        .filter(col("dataset_size_in_rse_tb") > min_tb_limit) \
        .replace(reverted_disk_rses_id_name_map, subset=['rse_id']) \
        .withColumnRenamed("rse_id", "RSE name") \
        .select(['RSE name',
                 'dataset',
                 'dataset_size_in_rse_tb',
                 'last_create_time_of_dataset_in_rse',
                 '#distinct_files_of_dataset_in_rse'
                 ]) \
        .cache()

    df_main_datasets_never_read = df_sub_datasets_never_read \
        .groupby(["dataset"]) \
        .agg(_max(col("dataset_size_in_rse_tb")).alias("max_dataset_size_in_rses(TB)"),
             _min(col("dataset_size_in_rse_tb")).alias("min_dataset_size_in_rses(TB)"),
             _avg(col("dataset_size_in_rse_tb")).alias("avg_dataset_size_in_rses(TB)"),
             _max(col("last_create_time_of_dataset_in_rse")).alias("last_create_time_of_dataset_in_all_rses"),
             concat_ws(", ", collect_list("RSE name")).alias("RSE(s)"),
             ) \
        .cache()
    return df_main_datasets_never_read, df_sub_datasets_never_read


def get_df_sub_not_read_since(df_dataset_file_rse_ts_size, disk_rses_id_name_map, min_tb_limit, n_months_filter):
    """Get dataframe of datasets that are not read since N months for sub details htmls

    Group by "dataset" and "rse_id" of get_df_dataset_file_rse_ts_size

    Filters:
        - If a dataset contains EVEN a single file with null accessed_at, filter out

    Access time filter logic:
        - If "last_access_time_of_dataset_in_all_rses" is less than "n_months_filter", ...
          ... set "is_not_read_since_{n_months_filter}_months" column as True

    Columns:
        - "dataset_size_in_rse_tb"
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

    df_main_datasets_and_rses: RSE name, dataset and their size and access time calculations
    """
    # New boolean column name to map dataset-rse_id couples are not read at least n_months_filter or not
    bool_column_is_not_read_since_n_months = 'is_not_read_since_{}_months'.format(str(n_months_filter))

    # Get reverted dict to get RSE names from id
    reverted_disk_rses_id_name_map = get_reverted_disk_rses_id_name_map(disk_rses_id_name_map)

    return df_dataset_file_rse_ts_size \
        .groupby(["rse_id", "dataset"]) \
        .agg(_round(_sum(col("fsize")) / TB_DENOMINATOR, 5).alias("dataset_size_in_rse_tb"),
             _max(col("accessed_at")).alias("last_access_time_of_dataset_in_rse"),
             _sum(
                 when(col("accessed_at").isNull(), 1).otherwise(0)
             ).alias("#files_with_null_access_time_of_dataset_in_rse"),
             _count(lit(1)).alias("#files_of_dataset_in_rse"),
             countDistinct(col("file")).alias("#distinct_files_of_dataset_in_rse"),
             ) \
        .withColumn(bool_column_is_not_read_since_n_months,
                    when(
                        col('last_access_time_of_dataset_in_rse') < get_n_months_ago_epoch_msec(n_months_filter),
                        True).otherwise(False)
                    ) \
        .filter((
                    col("last_access_time_of_dataset_in_rse").isNotNull()
                ) & (
                    col("#files_with_null_access_time_of_dataset_in_rse") == 0
                )) \
        .filter(col(bool_column_is_not_read_since_n_months)) \
        .filter(col("dataset_size_in_rse_tb") > min_tb_limit) \
        .replace(reverted_disk_rses_id_name_map, subset=['rse_id']) \
        .withColumnRenamed("rse_id", "RSE name") \
        .select(['RSE name',
                 'dataset',
                 'dataset_size_in_rse_tb',
                 'last_access_time_of_dataset_in_rse',
                 '#distinct_files_of_dataset_in_rse'
                 ]) \
        .cache()


def get_df_main_not_read_since(df_sub_not_read_since):
    """Get dataframe of datasets not read since N months for main htmls.

    Get last access of dataframe in all RSE(s)
    """
    return df_sub_not_read_since \
        .groupby(["dataset"]) \
        .agg(_max(col("dataset_size_in_rse_tb")).alias("max_dataset_size_in_rses(TB)"),
             _min(col("dataset_size_in_rse_tb")).alias("min_dataset_size_in_rses(TB)"),
             _avg(col("dataset_size_in_rse_tb")).alias("avg_dataset_size_in_rses(TB)"),
             _max(col("last_access_time_of_dataset_in_rse")).alias("last_access_time_of_dataset_in_all_rses"),
             concat_ws(", ", collect_list("RSE name")).alias("RSE(s)"),
             ) \
        .cache()


# --------------------------------------------------------------------------------
# HTML creation functions
# --------------------------------------------------------------------------------

def get_html_header_footer_strings(base_html_directory=None):
    """ Reads partial html files and return them as strings
    """
    if base_html_directory is None:
        base_html_directory = os.getcwd()
    with open(os.path.join(base_html_directory, "header.html")) as f:
        header = f.read()
    with open(os.path.join(base_html_directory, "footer.html")) as f:
        footer = f.read()
    return header, footer


def prep_pd_df_never_read_for_sub_html(df_pd_sub_datasets_never_read):
    """ Prepare pandas dataframe of sub details html which serve RSE details for datasets never read
    """
    # Add column with human-readable date
    df_pd_sub_datasets_never_read["last creation UTC"] = pd.to_datetime(
        df_pd_sub_datasets_never_read["last_create_time_of_dataset_in_rse"], unit='ms'
    )
    # Rename
    df_pd_sub_datasets_never_read = df_pd_sub_datasets_never_read.rename(
        columns={
            'dataset_size_in_rse_tb': "dataset size [TB]",
            '#distinct_files_of_dataset_in_rse': "number of distinct files of dataset",
            'last_create_time_of_dataset_in_rse': "last creation msec UTC",
        }
    )
    # Change order
    df_pd_sub_datasets_never_read = df_pd_sub_datasets_never_read[[
        "dataset", "RSE name", "dataset size [TB]", "last creation UTC", "last creation msec UTC",
        "number of distinct files of dataset",
    ]]
    return df_pd_sub_datasets_never_read.set_index(
        ["dataset", "RSE name"]
    ).sort_index()


def prep_pd_df_never_read_for_main_html(df_pd_main_datasets_never_read):
    """Prepare pandas dataframe of main html for datasets never read
    """
    # Filter columns
    df_pd_main_datasets_never_read = df_pd_main_datasets_never_read[[
        "dataset", "max_dataset_size_in_rses(TB)", "min_dataset_size_in_rses(TB)", "avg_dataset_size_in_rses(TB)",
        "last_create_time_of_dataset_in_all_rses", "RSE(s)",
    ]]
    # Filter datasets that their min datset size in RSE is less than 0.4TB
    df_pd_main_datasets_never_read = df_pd_main_datasets_never_read \
        .sort_values(by=['avg_dataset_size_in_rses(TB)'], ascending=False) \
        .reset_index(drop=True)

    # Convert date to string
    df_pd_main_datasets_never_read['last_create_time_of_dataset_in_all_rses'] = pd.to_datetime(
        df_pd_main_datasets_never_read['last_create_time_of_dataset_in_all_rses'], unit='ms'
    )
    # Rename and return
    return df_pd_main_datasets_never_read.rename(
        columns={
            "max_dataset_size_in_rses(TB)": "max size in RSEs [TB]",
            "min_dataset_size_in_rses(TB)": "min size in RSEs [TB]",
            "avg_dataset_size_in_rses(TB)": "avg size in RSEs [TB]",
            "last_create_time_of_dataset_in_all_rses": "last creation in all RSEs UTC",
        }
    ).copy(deep=True)


def prep_pd_df_not_read_since_for_sub_htmls(df_pd_sub_not_read_since):
    """ Prepare pandas dataframe of sub details html which serve RSE details for datasets not read since N months
    """
    # Add column with human-readable date
    df_pd_sub_not_read_since["last access UTC"] = pd.to_datetime(
        df_pd_sub_not_read_since["last_access_time_of_dataset_in_rse"], unit='ms'
    )
    # Rename
    df_pd_sub_not_read_since = df_pd_sub_not_read_since.rename(
        columns={
            'dataset_size_in_rse_tb': "dataset size [TB]",
            '#distinct_files_of_dataset_in_rse': "number of distinct files of dataset",
            'last_access_time_of_dataset_in_rse': "last access msec UTC",
        }
    )
    # Change order
    df_pd_sub_not_read_since = df_pd_sub_not_read_since[[
        "dataset", "RSE name", "dataset size [TB]", "last access UTC", "last access msec UTC",
        "number of distinct files of dataset",
    ]]

    return df_pd_sub_not_read_since.set_index(
        ["dataset", "RSE name"]
    ).sort_index()


def prep_pd_df_not_read_since_for_main_html(df_pd_main_not_read_since):
    """Prepare pandas dataframe of main html for datasets not read since N months
    """
    # Filter columns
    df_pd_main_not_read_since = df_pd_main_not_read_since[[
        "dataset", "max_dataset_size_in_rses(TB)", "min_dataset_size_in_rses(TB)", "avg_dataset_size_in_rses(TB)",
        "last_access_time_of_dataset_in_all_rses", "RSE(s)",
    ]]
    #
    df_pd_main_not_read_since = df_pd_main_not_read_since \
        .sort_values(by=['avg_dataset_size_in_rses(TB)'], ascending=False) \
        .reset_index(drop=True)
    # Convert date to string
    df_pd_main_not_read_since['last_access_time_of_dataset_in_all_rses'] = pd.to_datetime(
        df_pd_main_not_read_since['last_access_time_of_dataset_in_all_rses'], unit='ms'
    )
    # Rename and return
    return df_pd_main_not_read_since.rename(
        columns={
            "max_dataset_size_in_rses(TB)": "max size in RSEs [TB]",
            "min_dataset_size_in_rses(TB)": "min size in RSEs [TB]",
            "avg_dataset_size_in_rses(TB)": "avg size in RSEs [TB]",
            "last_access_time_of_dataset_in_all_rses": "last access in all RSEs",
        }
    ).copy(deep=True)


def create_sub_htmls(df_pd_sub, output_dir, sub_folder):
    """ Creates html files which include RSE details for each dataset

    Generic html creation for all choices:
        - datasets that are not read since N months and
        - dataset that never read

    'Show details' column values come from these htmls. For each dataset there is a html file for RSE detail.
    """
    # Create sub directory
    _folder = os.path.join(output_dir, sub_folder)
    os.makedirs(_folder, exist_ok=True)
    for dataset_name, df_iter in df_pd_sub.groupby(["dataset"]):
        # Slash in dataframe name is replaced with "-_-" in html, javascript will reconvert
        # Dataset names will be html name, and slash is not allowed unix file names
        dataset_name = dataset_name.replace("/", "-_-")
        df_iter.droplevel(["dataset"]).reset_index().to_html(
            os.path.join(_folder, "dataset_{}.html".format(dataset_name)),
            escape=False,
            index=False,
        )


def create_main_html(df_pd_main,
                     static_html_dir,
                     output_dir,
                     sub_folder,
                     main_html_file_name,
                     min_tb_limit,
                     title_replace_str, ):
    """Create and write main html page

    Generic html creation for all choices:
        - datasets that are not read since N months and
        - dataset that never read
    """
    # Get html_header and html_footer
    html_header, html_footer = get_html_header_footer_strings(static_html_dir)
    # header replace
    html_header = html_header.replace("__UPDATE_TIME__", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    html_header = html_header.replace("__are not read since n months__", title_replace_str)
    html_header = html_header.replace("__TB_LIMIT__", str(min_tb_limit))
    # footer replace
    html_footer = html_footer.replace("__SUB_FOLDER_NAME__", sub_folder)

    # Main column is dataset
    main_column = df_pd_main["dataset"].copy()
    df_pd_main["dataset"] = (
        '<a class="dataset">'
        + main_column
        + '</a><br>'
    )
    _fc = '<a class="selname">' + main_column + "</a>"
    df_pd_main["dataset"] = _fc

    # Main pandas html operations
    html = df_pd_main.to_html(escape=False, index=False)
    # cleanup of the default dump
    html = html.replace(
        'table border="1" class="dataframe"',
        'table id="dataframe" class="display compact" style="width:100%;"',
    )
    html = html.replace('style="text-align: right;"', "")
    # Append
    result = html_header + html + html_footer
    with open(os.path.join(output_dir, main_html_file_name), "w") as f:
        f.write(result)


def create_htmls_for_not_read_since(df_pd_main_not_read_since, df_pd_sub_not_read_since, static_html_dir,
                                    output_dir, sub_folder, main_html_file_name, min_tb_limit, title_replace_str, ):
    # Prepare main pandas dataframe for datasets not read since n months
    df_pd_main = prep_pd_df_not_read_since_for_main_html(
        df_pd_main_not_read_since=df_pd_main_not_read_since
    )
    # Prepare sub details pandas dataframe
    df_pd_sub = prep_pd_df_not_read_since_for_sub_htmls(
        df_pd_sub_not_read_since=df_pd_sub_not_read_since
    )
    create_main_html(df_pd_main, static_html_dir, output_dir, sub_folder, main_html_file_name, min_tb_limit,
                     title_replace_str, )
    create_sub_htmls(df_pd_sub, output_dir, sub_folder)


def create_htmls_for_never_read(df_pd_main_datasets_never_read, df_pd_sub_datasets_never_read, static_html_dir,
                                output_dir, sub_folder, main_html_file_name, min_tb_limit, title_replace_str, ):
    # Prepare main pandas dataframe for datasets never read
    df_pd_main = prep_pd_df_never_read_for_main_html(
        df_pd_main_datasets_never_read=df_pd_main_datasets_never_read
    )
    # Prepare sub details pandas dataframe
    df_pd_sub = prep_pd_df_never_read_for_sub_html(
        df_pd_sub_datasets_never_read=df_pd_sub_datasets_never_read
    )
    create_main_html(df_pd_main, static_html_dir, output_dir, sub_folder, main_html_file_name, min_tb_limit,
                     title_replace_str, )
    create_sub_htmls(df_pd_sub, output_dir, sub_folder)


@click.command()
@click.option("--static_html_dir",
              default=None,
              type=str,
              required=True,
              help="Html directory for header footer. For example: ~/CMSSpark/src/html/rucio_datasets_last_access_ts")
@click.option("--output_dir",
              default=None,
              type=str,
              required=True,
              help="I.e. /eos/user/c/cmsmonit/www/rucio/rucio_datasets_last_access.html")
@click.option("--rses_pickle",
              default=None,
              type=str,
              required=True,
              help="Please see rse_name_id_map_pickle.py', Default: ~/CMSSpark/static/rucio/rses.pickle")
@click.option("--min_tb_limit",
              default=1.0,
              type=float,
              required=True,
              help="Minumum TB limit to filter dataset sizes")
# @click.option("--n_months_filter",
#               default=12,
#               type=int,
#               required=True,
#               help="0: datasets never read htmls;  3,6,12: datasets not read since N months html")
def main(static_html_dir=None, output_dir=None, rses_pickle=None, min_tb_limit=None):
    """
        Main function that run Spark dataframe creations and create html pages
    """
    print(f"Static html dir: {static_html_dir}",
          f"Output dir: {output_dir}",
          f"Rses pickle dir: {rses_pickle}",
          f"Min TB limit: {min_tb_limit}")
    #
    os.makedirs(output_dir, exist_ok=True)

    # Get rse ids of only Disk RSEs
    disk_rses_id_name_map = get_disk_rse_ids(rses_pickle)

    # Start Spark aggregations
    #
    spark = get_spark_session()
    df_replicas = get_df_replicas(spark, list(disk_rses_id_name_map.values()))
    df_dids_files = get_df_dids_files(spark)
    df_replicas_j_dids = get_df_replicas_j_dids(df_replicas, df_dids_files)
    df_file_rse_ts_size = get_df_file_rse_ts_size(df_replicas_j_dids)
    df_contents_f_to_b = get_df_contents_f_to_b(spark)
    df_block_file_rse_ts_size = get_df_block_file_rse_ts_size(df_file_rse_ts_size, df_contents_f_to_b)
    df_contents_b_to_d = get_df_contents_b_to_d(spark)
    df_dataset_file_rse_ts_size = get_df_dataset_file_rse_ts_size(df_block_file_rse_ts_size, df_contents_b_to_d)

    # Create htmls for all datasets not read since [3,6,12] months
    for n_months_filter in [3, 6, 12]:
        # Create htmls for datasets not read since N months
        # Get Spark dataframe for RSE details sub htmls
        df_sub_not_read_since = get_df_sub_not_read_since(
            df_dataset_file_rse_ts_size=df_dataset_file_rse_ts_size,
            disk_rses_id_name_map=disk_rses_id_name_map,
            min_tb_limit=min_tb_limit,
            n_months_filter=n_months_filter
        )

        # includes only dataset values, no RSE details
        df_main_not_read_since = get_df_main_not_read_since(df_sub_not_read_since)
        #
        create_htmls_for_not_read_since(df_pd_main_not_read_since=df_main_not_read_since.toPandas(),
                                        df_pd_sub_not_read_since=df_sub_not_read_since.toPandas(),
                                        static_html_dir=static_html_dir,
                                        output_dir=output_dir,
                                        sub_folder=f"rses_{n_months_filter}_months",
                                        main_html_file_name=f"datasets_not_read_since_{n_months_filter}_months.html",
                                        min_tb_limit=min_tb_limit,
                                        title_replace_str=f"have not been read since {n_months_filter} months",
                                        )

    # Create htmls for datasets never read
    # Get both Spark dataframes
    df_main_datasets_never_read, df_sub_datasets_never_read = get_df_main_datasets_never_read(
        df_dataset_file_rse_ts_size=df_dataset_file_rse_ts_size,
        disk_rses_id_name_map=disk_rses_id_name_map,
        min_tb_limit=min_tb_limit,
    )
    #
    create_htmls_for_never_read(df_pd_main_datasets_never_read=df_main_datasets_never_read.toPandas(),
                                df_pd_sub_datasets_never_read=df_sub_datasets_never_read.toPandas(),
                                static_html_dir=static_html_dir,
                                output_dir=output_dir,
                                sub_folder="rses_never",
                                main_html_file_name=f"datasets_never_read.html",
                                min_tb_limit=min_tb_limit,
                                title_replace_str=f"were never read",
                                )


if __name__ == "__main__":
    main()
