#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
"""Get last access times of datasets by joining Rucio's REPLICAS, DIDS and CONTENTS tables"""

import pickle
import sys
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
MIN_TB_LIMIT = 1.0  # TB

pd.options.display.float_format = "{:,.2f}".format
pd.set_option("display.max_colwidth", -1)

########################################################
""" ---- Assumptions and explanations of Rucio tables ----
     DBS vs Rucio terminology ---
        - file:    [F]ile in Rucio
        - block:   [D]ataset in Rucio
        - dataset: [C]ontainer in Rucio
    
    Notes:
        - We used DBS terminology otherwise specified implicitly!!!
        - In function&dataframe names: f, b, d are initial letters of file, block and dataset
    
    Assumptions:
        * It's certain that final result is not 100% correct with below assumptions, but without below assumptions,
          it's impossible to produce any result.
            - Ignore files which do not have: block name, dataset name, accessed_at, size
            - ACCESSED_AT information of files comes from DIDS and REPLICAS tables
            -  BYTES(fsize) information of files comes from DIDS and REPLICAS tables
 
    
    Data sources in Rucio tables:
        ACCESSED_AT (last access time)                     : greatest of DIDS' and REPLICAS' ACCESSED_AT
        BYTES (file size)                                  : DIDS and REPLICAS (equal for same files)
        RSE ID - FILE relation                             : comes from REPLICAS
        All file-block, block-dataset membership/ownership : comes from CONTENTS
    
    
    
    Steps:
        ---- Main aim is to get Datasets, their rses and last access times and sizes in RSEs. Then filter.
        - Get 
            - files in disk RSEs, their accessed_at(DIDS and REPLICAS) and size (DIDS and REPLICAS)
            - reach to dataset names of files in RSEs (files to blocks, blocks to datasets)
            - Calculate last access time of datasets (max of all files) 
            - dataset max,min,avg sizes in all RSEs and 
            - details of dataset in a single RSE
        - Filter (!ATTENTION!)
            - Filter out datasets which have even a single file with NULL accessed_at
            - Filter only datasets which are read since 12 months (FOR NOW)
    
    
    
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


def get_n_months_ago_epoch_msec(n_months_ago):
    """Returns integer unix timestamp msec of n months ago from today"""
    dt = datetime.today() + relativedelta(months=-n_months_ago)  # minus
    return int(datetime(dt.year, dt.month, dt.day).timestamp()) * 1000


def get_disk_rse_ids(rse_id_name_map_pickle):
    """Get rse:rse_id map from pickle filter only Disk rses, returns dict

    See CMSMONIT-324 how to fetch only Disk RSE_IDs using Rucio cli (author: David Lange)
    """
    with open(rse_id_name_map_pickle, "rb+") as f:
        rses = pickle.load(f)
    return dict([(k, v) for k, v in rses.items() if not any(tmp in k for tmp in ["Tape", "Test", "Temp"])])


def get_spark_session(yarn=True, verbose=False):
    """Get or create the spark context and session.
    """
    sc = SparkContext(appName="cms-monitoring-rucio-datasets-last-access-ts")
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
        - rep_accessed_at

    df_replicas: Main replicas Spark dataframe for this script
    """
    return spark.read.format("avro").load(HDFS_RUCIO_REPLICAS) \
        .withColumn("rse_id", lower(_hex(col("RSE_ID")))) \
        .withColumn("fsize_replicas", col("BYTES").cast(LongType())) \
        .withColumnRenamed("NAME", "file") \
        .withColumnRenamed("ACCESSED_AT", "rep_accessed_at") \
        .filter(col("rse_id").isin(disk_rse_ids)) \
        .filter(col("SCOPE") == "cms") \
        .select(["file", "rse_id", "fsize_replicas", "rep_accessed_at"]) \
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
        - dids_accessed_at: file last access time

    df_dids_files: All files catalog, their sizes and last access times
    """
    return spark.read.format("avro").load(HDFS_RUCIO_DIDS) \
        .filter(col("DELETED_AT").isNull()) \
        .filter(col("HIDDEN") == "0") \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "F") \
        .withColumnRenamed("NAME", "file") \
        .withColumnRenamed("ACCESSED_AT", "dids_accessed_at") \
        .withColumn("fsize_dids", col("BYTES").cast(LongType())) \
        .select(["file", "fsize_dids", "dids_accessed_at"]) \
        .cache()


def get_df_replicas_j_dids(df_replicas, df_dids_files):
    """Left join of df_replicas and df_dids_files to fill the RSE_ID, fsize and accessed_at for all files.

    Be aware that there are 2 file size columns, they will be combined in "df_file_rse_ts_size".

    Columns:
        comes from DID:       file, dids_accessed_at, fsize_dids,
        comes from REPLICAS:  file, rse_id, fsize_replicas, rep_accessed_at

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


def get_df_file_rse_ts_size(df_replicas_j_dids):
    """fsize_dids or fsize_replicas should not be null. Just combine them to fill file sizes.

    Firstly, REPLICAS size value will be used. If there are files with no size values, DIDS size values will be used:
    see "when" function order.

    Columns: file, rse_id, accessed_at, fsize

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
        .select(['file', 'rse_id', 'accessed_at', 'fsize']) \
        .cache()


def get_df_contents_f_to_b(spark):
    """Get all files that a block contains, or all blocks that a file belongs to.

    This dataframe ensures unique (file, block) couples because of:
        CONTENTS table pk: CONSTRAINT CONTENTS_PK PRIMARY KEY (SCOPE, NAME, CHILD_SCOPE, CHILD_NAME)

    Columns selected: block, file

    df_contents_f_to_b: FILE-BLOCK membership/ownership map
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

    Columns: block(from df_contents_f_to_b), file, rse_id, accessed_at, fsize

    df_block_file_rse_ts_size: add "block" names to "df_file_rse_ts_size" dataframe
    """
    df_block_file_rse_ts_size = df_file_rse_ts_size \
        .join(df_contents_f_to_b, ["file"], how="left") \
        .select(['block', 'file', 'rse_id', 'accessed_at', 'fsize']) \
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
    """Get all blocks that a dataset contains, or all datasets that a block belongs to.

    This dataframe ensures unique (block, dataset) couples because of:
        CONTENTS table pk: CONSTRAINT CONTENTS_PK PRIMARY KEY (SCOPE, NAME, CHILD_SCOPE, CHILD_NAME)

    Columns selected: dataset, block

    df_contents_b_to_d: BLOCK-DATASET membership/ownership map
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
    
    After getting dataset of blocks(so files), drop blocks.
    
    Columns: dataset(from df_contents_b_to_d), file, rse_id, accessed_at, fsize

    df_dataset_file_rse_ts_size: add "dataset" name to df_block_file_rse_ts_size.
    """
    df_dataset_file_rse_ts_size = df_block_file_rse_ts_size \
        .join(df_contents_b_to_d, ["block"], how="left") \
        .select(['dataset', 'block', 'file', 'rse_id', 'accessed_at', 'fsize']) \
        .cache()

    null_dataset_distinct_block_count = df_dataset_file_rse_ts_size.filter(
        col("dataset").isNull()
    ).select("block").distinct().count()

    print("Stats of df_dataset_file_rse_ts_size before filtering out null values =>")
    print("Number of Distinct blocks that has no dataset name:", null_dataset_distinct_block_count)

    # Drop blocks since we got the dataset of files (our on of main aims till here)
    return df_dataset_file_rse_ts_size.filter(
        col("dataset").isNotNull()
    ).select(['dataset', 'file', 'rse_id', 'accessed_at', 'fsize'])


def get_df_final_datasets_and_rses(df_dataset_file_rse_ts_size, n_months_ago):
    """Group by "dataset" and "rse_id" of df_d_b_f_rse_ts_size

    Filters:
        - If a dataset contains EVEN a single file with null accessed_at, filter out

    Access time filter logic:
        - If "last_access_time_of_dataset_in_all_rses" is less than "n_months_ago",
          set "is_not_read_since_{n_months_ago}_months" column as True

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

        Final result will be like:
            One dataset can be in multiple RSEs and
            presumably it may have different sizes since a dataset may have lost some of its blocks or files in an RSE?

    df_final_datasets_and_rses: dataset, rse_id and their size and access time calculations
    """

    # New boolean column to map dataset-rse_id couples are not read at least n_months_ago or not
    bool_column_is_not_read_since_n_months = 'is_not_read_since_{}_months'.format(str(n_months_ago))

    if n_months_ago not in (3, 6, 12):
        print("Please provide integer 3, 6 or 12")
        sys.exit(0)

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
                        col('last_access_time_of_dataset_in_rse') < get_n_months_ago_epoch_msec(n_months_ago),
                        True).otherwise(False)
                    ) \
        .filter((
                    col("last_access_time_of_dataset_in_rse").isNotNull()
                ) & (
                    col("#files_with_null_access_time_of_dataset_in_rse") == 0
                )) \
        .filter(col(bool_column_is_not_read_since_n_months)) \
        .filter(col("dataset_size_in_rse_tb") > MIN_TB_LIMIT) \
        .select(['rse_id',
                 'dataset',
                 'dataset_size_in_rse_tb',
                 'last_access_time_of_dataset_in_rse',
                 '#distinct_files_of_dataset_in_rse'
                 ]) \
        .cache()
    # !Attention! See last filter that filters only dataset-rse_id couples which are not read at least "n_months_ago"


def get_df_final_datasets(df_final_datasets_and_rses):
    """Calculate main results of datasets by aggregating their values in RSEs.

    Group by Dataset to get final result from all RSEs' datasets.
      - max_dataset_size_in_rses(TB): max size of dataset in all RSEs that contain this Dataset
      - min_dataset_size_in_rses(TB): min size of dataset in all RSEs that contain this Dataset
      - avg_dataset_size_in_rses(TB): avg size of dataset in all RSEs that contain this Dataset
      - last_access_time_of_dataset_in_all_rses: latest access time of dataset in all RSEs



    The final result includes only the Datasets whose last access time is older than 3 months.
    """

    return df_final_datasets_and_rses \
        .groupby(["dataset"]) \
        .agg(_max(col("dataset_size_in_rse_tb")).alias("max_dataset_size_in_rses(TB)"),
             _min(col("dataset_size_in_rse_tb")).alias("min_dataset_size_in_rses(TB)"),
             _avg(col("dataset_size_in_rse_tb")).alias("avg_dataset_size_in_rses(TB)"),
             _max(col("last_access_time_of_dataset_in_rse")).alias("last_access_time_of_dataset_in_all_rses"),
             ) \
        .cache()


def prepare_df_pandas_for_html(df_pandas):
    """Get last access time 12 months of datasets and filter size greater than MIN_TB_LIMIT
    """
    # Filter columns
    df_pandas = df_pandas[[
        "dataset",
        "max_dataset_size_in_rses(TB)",
        "min_dataset_size_in_rses(TB)",
        "avg_dataset_size_in_rses(TB)",
        "last_access_time_of_dataset_in_all_rses",
    ]]

    # Filter datasets that their min datset size in RSE is less than 0.4TB
    df_pandas = df_pandas[
        df_pandas["min_dataset_size_in_rses(TB)"] >= MIN_TB_LIMIT
        ].sort_values(by=['avg_dataset_size_in_rses(TB)'], ascending=False)

    df_pandas = df_pandas.reset_index(drop=True)
    total_row = {
        'dataset': 'total',
        'max_dataset_size_in_rses(TB)': "{:,.2f}".format(df_pandas["max_dataset_size_in_rses(TB)"].sum()),
        'min_dataset_size_in_rses(TB)': "{:,.2f}".format(df_pandas["min_dataset_size_in_rses(TB)"].sum()),
        'avg_dataset_size_in_rses(TB)': "{:,.2f}".format(df_pandas["avg_dataset_size_in_rses(TB)"].sum()),
        'last_access_time_of_dataset_in_all_rses': "-",
    }
    return df_pandas.copy(deep=True), total_row


def get_html_strings(base_html_directory=None):
    """Reads partial html files and return them as strings"""
    if base_html_directory is None:
        base_html_directory = os.getcwd()

    with open(os.path.join(base_html_directory, "header.html")) as f:
        header = f.read()
    with open(os.path.join(base_html_directory, "footer.html")) as f:
        footer = f.read()
    return header, footer


def create_html_sub_toggle_rse_parts(df_pandas_final_datasets_and_rses, output_dir):
    """Creates detaild sun RSE part for datasets rows"""
    df_pandas_final_datasets_and_rses = df_pandas_final_datasets_and_rses.set_index(["dataset", "rse_id"]).sort_index()
    _folder = os.path.join(output_dir, "rsedetails")
    os.makedirs(_folder, exist_ok=True)
    for dataset_name, df_iter in df_pandas_final_datasets_and_rses.groupby(["dataset"]):
        # Slash in dataframe name is replaced with "-_-" in html, javascript will reconvert
        # Dataset names will be html name, and slash is not allowed unix file names
        dataset_name = dataset_name.replace("/", "-_-")
        df_iter.droplevel(["dataset"]).to_html(
            os.path.join(_folder, "dataset_{}.html".format(dataset_name)),
            escape=False,
        )


def create_html(df_pandas, total_row, n_months_ago, base_html_directory=None, output_dir="./"):
    """Create and write html page
    """
    # Get html_header and html_footer
    html_header, html_footer = get_html_strings(base_html_directory)
    html_header = html_header.replace("UPDATE_TIME", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
    html_header = html_header.replace("Rucio Datasets which are not read since N months",
                                      "Rucio Datasets which are not read since {} months".format(n_months_ago))

    # To stick total column to first row
    #   total_on_header = """<tr >
    #     <th>{dataset}</th>
    #     <th align="right">{max_dataset_size_in_rses(TB)}</th>
    #     <th align="right">{min_dataset_size_in_rses(TB)}</th>
    #     <th align="right">{avg_dataset_size_in_rses(TB)}</th>
    #     <th align="right">{last_access_time_of_dataset_in_all_rses}</th>
    #   </tr>
    # </thead>
    #           """.format(**total_row)

    # Main column is dataset
    main_column = df_pandas["dataset"].copy()
    df_pandas["dataset"] = (
        '<a class="dataset">'
        + main_column
        + '</a><br>'
    )
    _fc = '<a class="selname">' + main_column + "</a>"
    df_pandas["dataset"] = _fc

    # Main pandas html operations

    html = df_pandas.to_html(escape=False, index=False)

    # Add total ro to header. Be careful there should be only one  <thead>...</thead>!
    # html = html.replace(" </thead>", total_on_header)

    html = html.replace(
        'table border="1" class="dataframe"',
        'table id="dataframe" class="display compact" style="width:100%;"',
    )
    html = html.replace('style="text-align: right;"', "")
    # cleanup of the default dump
    html = html.replace(
        'table border="1" class="dataframe"',
        'table id="dataframe" class="display compact" style="width:100%;"',
    )
    html = html.replace('style="text-align: right;"', "")

    # Append
    result = html_header + html + html_footer
    with open(os.path.join(output_dir, "rucio_datasets_last_access.html"), "w") as f:
        f.write(result)


@click.command()
@click.option("--html_directory", default=None, required=True,
              help="For example: ~/CMSSpark/src/html/rucio_datasets_last_access_ts")
@click.option("--output_dir", default=None, required=True,
              help="I.e. /eos/user/c/cmsmonit/www/rucio/rucio_datasets_last_access.html")
@click.option("--rses_pickle", default=None, required=True,
              help="Please see rse_name_id_map_pickle.py', Default: ~/CMSSpark/static/rucio/rses.pickle")
def main(html_directory=None, output_dir=None, rses_pickle=None):
    """
        Main function that create Spark dataframes and create html page from the final result
    """

    # Get rse ids of only Disk RSEs
    disk_rses_id_name_map = get_disk_rse_ids(rses_pickle)

    # === Start Spark aggregations ====

    spark = get_spark_session()
    df_replicas = get_df_replicas(spark, list(disk_rses_id_name_map.values()))
    df_dids_files = get_df_dids_files(spark)
    df_replicas_j_dids = get_df_replicas_j_dids(df_replicas, df_dids_files)
    check_replicas_dids_join_is_desired(df_replicas_j_dids)
    df_file_rse_ts_size = get_df_file_rse_ts_size(df_replicas_j_dids)
    df_contents_f_to_b = get_df_contents_f_to_b(spark)
    df_block_file_rse_ts_size = get_df_block_file_rse_ts_size(df_file_rse_ts_size, df_contents_f_to_b)
    df_contents_b_to_d = get_df_contents_b_to_d(spark)
    df_dataset_file_rse_ts_size = get_df_dataset_file_rse_ts_size(df_block_file_rse_ts_size, df_contents_b_to_d)

    # Create html page from final Spark dataframe for datasets not accessed at least 12 months
    n_months_ago = 12

    # Get pandas dataframe
    df_final_datasets_and_rses_12 = get_df_final_datasets_and_rses(df_dataset_file_rse_ts_size, n_months_ago)
    df_final_datasets_12 = get_df_final_datasets(df_final_datasets_and_rses_12)
    #
    df_pandas_final_datasets_and_rses = df_final_datasets_and_rses_12.toPandas()
    #
    df_pandas = df_final_datasets_12.toPandas()
    df_pandas, total_row = prepare_df_pandas_for_html(df_pandas)

    # Replace RSE ID with its name
    reverted_disk_rses_id_name_map = {v: k for k, v in disk_rses_id_name_map.items()}
    df_pandas_final_datasets_and_rses = df_pandas_final_datasets_and_rses.replace(
        {"rse_id": reverted_disk_rses_id_name_map}
    )

    # Create dataset sub rse details part
    create_html_sub_toggle_rse_parts(df_pandas_final_datasets_and_rses, output_dir)

    # Create main html
    create_html(df_pandas=df_pandas,
                total_row=total_row,
                n_months_ago=n_months_ago,
                base_html_directory=html_directory,
                output_dir=output_dir)


if __name__ == "__main__":
    main()
