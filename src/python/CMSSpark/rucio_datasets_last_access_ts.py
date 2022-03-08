#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
"""Create html pages for datasets' access times by joining Rucio's REPLICAS, DIDS and CONTENTS tables

ATTENTION: Please see explanations in CMSSpark/src/html/src/html/rucio_datasets_last_access_ts for Disk and Tape.
"""

import pickle
from datetime import datetime

import click
import os
import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, collect_list, concat_ws, greatest, lit, lower, when,
    avg as _avg,
    count as _count,
    hex as _hex,
    max as _max,
    min as _min,
    round as _round,
    sum as _sum,
)

from pyspark.sql.types import (
    LongType,
)

from CMSSpark import schemas as cms_schemas

TODAY = datetime.today().strftime('%Y-%m-%d')
HDFS_RUCIO_DIDS = '/project/awg/cms/rucio/{}/dids/part*.avro'.format(TODAY)
HDFS_RUCIO_REPLICAS = '/project/awg/cms/rucio/{}/replicas/part*.avro'.format(TODAY)
HDFS_DBS_FILES = '/project/awg/cms/CMS_DBS3_PROD_GLOBAL/old/FILES/part-m-00000'
HDFS_DBS_DATASETS = '/project/awg/cms/CMS_DBS3_PROD_GLOBAL/old/DATASETS/part-m-00000'
BACKFILL_PREFIX = '/store/backfill/'  # will be filtered out
TB_DENOMINATOR = 10 ** 12

pd.options.display.float_format = '{:,.2f}'.format
pd.set_option('display.max_colwidth', -1)


def get_spark_session(yarn=True, verbose=False):
    """Get or create the spark context and session.
    """
    sc = SparkContext(appName='cms-monitoring-rucio-datasets-last-access-ts')
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def get_n_months_ago_epoch_msec(n_months_ago):
    """Returns integer unix timestamp msec of n months ago from today"""
    dt = datetime.today() + relativedelta(months=-n_months_ago)  # minus
    return int(datetime(dt.year, dt.month, dt.day).timestamp()) * 1000


def get_rse_ids(rse_id_name_map_pickle, is_disk):
    """Get rse:rse_id map from pickle by filtering Disk or Tape

    See CMSSpark/static/rucio/rse_name_id_map_pickle.py
    """
    with open(rse_id_name_map_pickle, 'rb+') as f:
        rses = pickle.load(f)
    if is_disk:
        return dict([(k, v) for k, v in rses.items() if not any(tmp in k for tmp in ['Tape', 'Test', 'Temp'])])
    else:
        print("################################################DISK")
        return dict([(k, v) for k, v in rses.items() if k.endswith("_Tape")])


def get_reverted_rses_id_name_map(rses_id_name_map):
    """Revert k:v to v:k"""
    return {v: k for k, v in rses_id_name_map.items()}


# --------------------------------------------------------------------------------
# Intermediate dataset creation functions
# --------------------------------------------------------------------------------

def get_df_dbs_f_d(spark):
    """Create a dataframe for FILE-DATASET membership/ownership map

    Columns selected: f_name, d_name
    """
    csvreader = spark.read.format('csv') \
        .option('nullValue', 'null') \
        .option('mode', 'FAILFAST')
    dbs_files = csvreader.schema(cms_schemas.schema_files()) \
        .load(HDFS_DBS_FILES) \
        .withColumnRenamed('f_logical_file_name', 'f_name')
    dbs_datasets = csvreader.schema(cms_schemas.schema_datasets()) \
        .load(HDFS_DBS_DATASETS) \
        .select(['d_dataset_id', 'd_dataset'])
    df_dbs_f_d = dbs_files.join(dbs_datasets, dbs_files.f_dataset_id == dbs_datasets.d_dataset_id, how='left') \
        .withColumnRenamed('f_dataset_id', 'dataset_id') \
        .withColumnRenamed('d_dataset', 'dataset') \
        .select(['f_name', 'dataset'])
    # (df_dbs_f_d.select('f_name').distinct().count() == df_dbs_f_d.count()) => True

    return df_dbs_f_d


def get_df_replicas(spark, filtered_rse_ids):
    """Create main replicas dataframe by selecting only Disk or Tape RSEs in Rucio REPLICAS table

    Columns selected:
        - f_name: file name
        - f_size_replicas: represents size of a file in REPLICAS table
        - rse_id
        - rep_accessed_at
        - rep_created_at
    """
    return spark.read.format('avro').load(HDFS_RUCIO_REPLICAS) \
        .withColumn('rse_id', lower(_hex(col('RSE_ID')))) \
        .withColumn('f_size_replicas', col('BYTES').cast(LongType())) \
        .withColumnRenamed('NAME', 'f_name') \
        .withColumnRenamed('ACCESSED_AT', 'rep_accessed_at') \
        .withColumnRenamed('CREATED_AT', 'rep_created_at') \
        .filter(col('rse_id').isin(filtered_rse_ids)) \
        .filter(col('SCOPE') == 'cms') \
        .filter(~col('NAME').startswith(BACKFILL_PREFIX)) \
        .select(['f_name', 'rse_id', 'f_size_replicas', 'rep_accessed_at', 'rep_created_at']) \
        .cache()


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
        .select(['f_name', 'f_size_dids', 'dids_accessed_at', 'dids_created_at']) \
        .cache()


def get_df_replicas_j_dids(df_replicas, df_dids_files):
    """Left join of df_replicas and df_dids_files to fill the RSE_ID, f_size and accessed_at, created_at for all files.

    Be aware that there are 2 columns for each f_size, accessed_at, created_at
    They will be combined in get_df_file_rse_ts_size

    Columns:
        comes from DID:       file, dids_accessed_at, dids_created_at, f_size_dids,
        comes from REPLICAS:  file, rse_id, f_size_replicas, rep_accessed_at, rep_created_at
   """
    return df_replicas.join(df_dids_files, ['f_name'], how='left').cache()


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
        .select(['f_name', 'rse_id', 'accessed_at', 'f_size', 'created_at']) \
        .cache()


def get_df_dataset_file_rse_ts_size(df_file_rse_ts_size, df_dbs_f_d):
    """ Left join df_file_rse_ts_size and df_dbs_f_d to get dataset names of files.

    In short: adds 'dataset' names to 'df_file_rse_ts_size' dataframe by joining DBS tables

    Columns: block(from df_contents_f_to_b), file, rse_id, accessed_at, f_size
    """
    df_dataset_file_rse_ts_size = df_file_rse_ts_size \
        .join(df_dbs_f_d, ['f_name'], how='left') \
        .select(['dataset', 'f_name', 'rse_id', 'accessed_at', 'created_at', 'f_size']) \
        .cache()

    f_c = df_dataset_file_rse_ts_size.select('f_name').distinct().count()
    f_w_no_dataset_c = df_dataset_file_rse_ts_size.filter(col('dataset').isNull()).select('f_name').distinct().count()
    print('Distinct file count:', f_c)
    print('Files with no dataset name count:', f_w_no_dataset_c)
    print('% of null dataset name in all files:', round(f_w_no_dataset_c / f_c, 3) * 100)

    return df_dataset_file_rse_ts_size.filter(col('dataset').isNotNull()).cache()


# --------------------------------------------------------------------------------
# Main dataset creation functions
# --------------------------------------------------------------------------------

def get_df_main_datasets_never_read(df_dataset_file_rse_ts_size, filtered_rses_id_name_map, min_tb_limit):
    """Get never accessed datasets' dataframes"""
    reverted_filtered_rses_id_name_map = get_reverted_rses_id_name_map(filtered_rses_id_name_map)
    df_sub_datasets_never_read = df_dataset_file_rse_ts_size \
        .groupby(['rse_id', 'dataset']) \
        .agg(_round(_sum(col('f_size')) / TB_DENOMINATOR, 5).alias('dataset_size_in_rse_tb'),
             _max(col('accessed_at')).alias('last_access_time_of_dataset_in_rse'),
             _max(col('created_at')).alias('last_create_time_of_dataset_in_rse'),
             _count(lit(1)).alias('#_files_of_dataset_in_rse'),
             ) \
        .filter(col('last_access_time_of_dataset_in_rse').isNull()) \
        .filter(col('dataset_size_in_rse_tb') > min_tb_limit) \
        .replace(reverted_filtered_rses_id_name_map, subset=['rse_id']) \
        .withColumnRenamed('rse_id', 'RSE name') \
        .select(['RSE name',
                 'dataset',
                 'dataset_size_in_rse_tb',
                 'last_create_time_of_dataset_in_rse',
                 '#_files_of_dataset_in_rse'
                 ]) \
        .cache()

    df_main_datasets_never_read = df_sub_datasets_never_read \
        .groupby(['dataset']) \
        .agg(_max(col('dataset_size_in_rse_tb')).alias('max_dataset_size_in_rses(TB)'),
             _min(col('dataset_size_in_rse_tb')).alias('min_dataset_size_in_rses(TB)'),
             _avg(col('dataset_size_in_rse_tb')).alias('avg_dataset_size_in_rses(TB)'),
             _sum(col('dataset_size_in_rse_tb')).alias('sum_dataset_size_in_rses(TB)'),
             _max(col('last_create_time_of_dataset_in_rse')).alias('last_create_time_of_dataset_in_all_rses'),
             concat_ws(', ', collect_list('RSE name')).alias('RSE(s)'),
             ) \
        .cache()
    return df_main_datasets_never_read, df_sub_datasets_never_read


def get_df_sub_not_read_since(df_dataset_file_rse_ts_size, filtered_rses_id_name_map, min_tb_limit, n_months_filter):
    """Get dataframe of datasets that are not read since N months for sub details htmls

    Group by 'dataset' and 'rse_id' of get_df_dataset_file_rse_ts_size

    Filters:
        - If a dataset contains EVEN a single file with null accessed_at, filter out

    Access time filter logic:
        - If 'last_access_time_of_dataset_in_all_rses' is less than 'n_months_filter', ...
          ... set 'is_not_read_since_{n_months_filter}_months' column as True

    Columns:
        - 'dataset_size_in_rse_tb'
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
    # New boolean column name to map dataset-rse_id couples are not read at least n_months_filter or not
    bool_column_is_not_read_since_n_months = 'is_not_read_since_{}_months'.format(str(n_months_filter))

    # Get reverted dict to get RSE names from id
    reverted_filtered_rses_id_name_map = get_reverted_rses_id_name_map(filtered_rses_id_name_map)

    return df_dataset_file_rse_ts_size \
        .groupby(['rse_id', 'dataset']) \
        .agg(_round(_sum(col('f_size')) / TB_DENOMINATOR, 5).alias('dataset_size_in_rse_tb'),
             _max(col('accessed_at')).alias('last_access_time_of_dataset_in_rse'),
             _sum(
                 when(col('accessed_at').isNull(), 0).otherwise(1)
             ).alias('#_accessed_files_of_dataset_in_rse'),
             _count(lit(1)).alias('#_files_of_dataset_in_rse'),
             ) \
        .withColumn(bool_column_is_not_read_since_n_months,
                    when(
                        col('last_access_time_of_dataset_in_rse') < get_n_months_ago_epoch_msec(n_months_filter),
                        True).otherwise(False)
                    ) \
        .filter(col('last_access_time_of_dataset_in_rse').isNotNull()) \
        .filter(col(bool_column_is_not_read_since_n_months)) \
        .filter(col('dataset_size_in_rse_tb') > min_tb_limit) \
        .replace(reverted_filtered_rses_id_name_map, subset=['rse_id']) \
        .withColumnRenamed('rse_id', 'RSE name') \
        .select(['RSE name',
                 'dataset',
                 'dataset_size_in_rse_tb',
                 'last_access_time_of_dataset_in_rse',
                 '#_files_of_dataset_in_rse',
                 '#_accessed_files_of_dataset_in_rse',
                 ]) \
        .cache()


def get_df_main_not_read_since(df_sub_not_read_since):
    """Get dataframe of datasets not read since N months for main htmls.

    Get last access of dataframe in all RSE(s)
    """
    return df_sub_not_read_since \
        .groupby(['dataset']) \
        .agg(_max(col('dataset_size_in_rse_tb')).alias('max_dataset_size_in_rses(TB)'),
             _min(col('dataset_size_in_rse_tb')).alias('min_dataset_size_in_rses(TB)'),
             _avg(col('dataset_size_in_rse_tb')).alias('avg_dataset_size_in_rses(TB)'),
             _sum(col('dataset_size_in_rse_tb')).alias('sum_dataset_size_in_rses(TB)'),
             _max(col('last_access_time_of_dataset_in_rse')).alias('last_access_time_of_dataset_in_all_rses'),
             concat_ws(', ', collect_list('RSE name')).alias('RSE(s)'),
             ) \
        .cache()


# --------------------------------------------------------------------------------
# HTML creation functions
# --------------------------------------------------------------------------------

def get_html_template_string(base_html_directory, is_disk):
    """ Reads main html file and return is as string
    """
    if base_html_directory is None:
        base_html_directory = os.getcwd()
    if is_disk:
        with open(os.path.join(base_html_directory, 'main_disk.html')) as f:
            return f.read()  # main html
    else:
        with open(os.path.join(base_html_directory, 'main_tape.html')) as f:
            return f.read()  # main html


def prep_pd_df_never_read_for_sub_html(df_pd_sub_datasets_never_read):
    """ Prepare pandas dataframe of sub details html which serve RSE details for datasets never read
    """
    # Add column with human-readable date
    df_pd_sub_datasets_never_read['last creation UTC'] = pd.to_datetime(
        df_pd_sub_datasets_never_read['last_create_time_of_dataset_in_rse'], unit='ms'
    )
    # Rename
    df_pd_sub_datasets_never_read = df_pd_sub_datasets_never_read.rename(
        columns={
            'dataset_size_in_rse_tb': 'dataset size [TB]',
            '#_files_of_dataset_in_rse': 'number of files in dataset',
            'last_create_time_of_dataset_in_rse': 'last creation msec UTC',
        }
    )
    # Change order
    df_pd_sub_datasets_never_read = df_pd_sub_datasets_never_read[[
        'dataset', 'RSE name', 'dataset size [TB]', 'last creation UTC', 'last creation msec UTC',
        'number of files in dataset',
    ]]
    return df_pd_sub_datasets_never_read.set_index(
        ['dataset', 'RSE name']
    ).sort_index()


def prep_pd_df_never_read_for_main_html(df_pd_main_datasets_never_read):
    """Prepare pandas dataframe of main html for datasets never read
    """
    # Filter columns
    df_pd_main_datasets_never_read = df_pd_main_datasets_never_read[[
        'dataset', 'max_dataset_size_in_rses(TB)', 'min_dataset_size_in_rses(TB)', 'avg_dataset_size_in_rses(TB)',
        'sum_dataset_size_in_rses(TB)', 'last_create_time_of_dataset_in_all_rses', 'RSE(s)',
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
            'max_dataset_size_in_rses(TB)': 'max size in RSEs [TB]',
            'min_dataset_size_in_rses(TB)': 'min size in RSEs [TB]',
            'avg_dataset_size_in_rses(TB)': 'avg size in RSEs [TB]',
            'sum_dataset_size_in_rses(TB)': 'total size in RSEs [TB]',
            'last_create_time_of_dataset_in_all_rses': 'last creation in all RSEs UTC',
        }
    ).copy(deep=True)


def prep_pd_df_not_read_since_for_sub_htmls(df_pd_sub_not_read_since):
    """ Prepare pandas dataframe of sub details html which serve RSE details for datasets not read since N months
    """
    # Add column with human-readable date
    df_pd_sub_not_read_since['last access UTC'] = pd.to_datetime(
        df_pd_sub_not_read_since['last_access_time_of_dataset_in_rse'], unit='ms'
    )
    # Rename
    df_pd_sub_not_read_since = df_pd_sub_not_read_since.rename(
        columns={
            'dataset_size_in_rse_tb': 'dataset size [TB]',
            '#_files_of_dataset_in_rse': 'number of files in dataset',
            '#_accessed_files_of_dataset_in_rse': 'number of accessed files',
            'last_access_time_of_dataset_in_rse': 'last access msec UTC',
        }
    )
    # Change order
    df_pd_sub_not_read_since = df_pd_sub_not_read_since[[
        'dataset', 'RSE name', 'dataset size [TB]', 'last access UTC', 'last access msec UTC',
        'number of files in dataset', 'number of accessed files'
    ]]

    return df_pd_sub_not_read_since.set_index(
        ['dataset', 'RSE name']
    ).sort_index()


def prep_pd_df_not_read_since_for_main_html(df_pd_main_not_read_since):
    """Prepare pandas dataframe of main html for datasets not read since N months
    """
    # Filter columns
    df_pd_main_not_read_since = df_pd_main_not_read_since[[
        'dataset', 'max_dataset_size_in_rses(TB)', 'min_dataset_size_in_rses(TB)', 'avg_dataset_size_in_rses(TB)',
        'sum_dataset_size_in_rses(TB)', 'last_access_time_of_dataset_in_all_rses', 'RSE(s)',
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
            'max_dataset_size_in_rses(TB)': 'max size in RSEs [TB]',
            'min_dataset_size_in_rses(TB)': 'min size in RSEs [TB]',
            'avg_dataset_size_in_rses(TB)': 'avg size in RSEs [TB]',
            'sum_dataset_size_in_rses(TB)': 'total size in RSEs [TB]',
            'last_access_time_of_dataset_in_all_rses': 'last access in all RSEs',
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
    for dataset_name, df_iter in df_pd_sub.groupby(['dataset']):
        # Slash in dataframe name is replaced with '-_-' in html, javascript will reconvert
        # Dataset names will be html name, and slash is not allowed unix file names
        dataset_name = dataset_name.replace('/', '-_-')
        df_iter.droplevel(['dataset']).reset_index().to_html(
            os.path.join(_folder, 'dataset_{}.html'.format(dataset_name)),
            escape=False,
            index=False,
        )


def create_main_html(is_disk,
                     df_pd_main,
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
    # Get html template
    html_template = get_html_template_string(base_html_directory=static_html_dir, is_disk=is_disk)
    # header replace
    html_template = html_template.replace('__UPDATE_TIME__', datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))
    html_template = html_template.replace('__PAGE_TITLE__', title_replace_str)
    html_template = html_template.replace('__TB_LIMIT__', str(min_tb_limit))
    # footer replace
    html_template = html_template.replace('__SUB_FOLDER_NAME__', sub_folder)

    # Main column is dataset
    main_column = df_pd_main['dataset'].copy()
    df_pd_main['dataset'] = (
        '<a class="dataset">'
        + main_column
        + '</a><br>'
    )
    _fc = '<a class="selname">' + main_column + '</a>'
    df_pd_main['dataset'] = _fc

    # Main pandas html operations
    html = df_pd_main.to_html(escape=False, index=False)
    # cleanup of the default dump
    html = html.replace(
        'table border="1" class="dataframe"',
        'table id="dataframe" class="display compact" style="width:100%;"',
    )
    html = html.replace('style="text-align: right;"', '')

    # Append
    page = html_template.replace("____MAIN_BLOCK____", html)
    with open(os.path.join(output_dir, main_html_file_name), 'w') as f:
        f.write(page)


def create_htmls_for_not_read_since(is_disk, df_pd_main_not_read_since, df_pd_sub_not_read_since, static_html_dir,
                                    output_dir, sub_folder, main_html_file_name, min_tb_limit, title_replace_str, ):
    # Prepare main pandas dataframe for datasets not read since n months
    df_pd_main = prep_pd_df_not_read_since_for_main_html(
        df_pd_main_not_read_since=df_pd_main_not_read_since
    )
    # Prepare sub details pandas dataframe
    df_pd_sub = prep_pd_df_not_read_since_for_sub_htmls(
        df_pd_sub_not_read_since=df_pd_sub_not_read_since
    )
    create_main_html(is_disk, df_pd_main, static_html_dir, output_dir, sub_folder, main_html_file_name,
                     min_tb_limit, title_replace_str, )
    create_sub_htmls(df_pd_sub, output_dir, sub_folder)


def create_htmls_for_never_read(is_disk, df_pd_main_datasets_never_read, df_pd_sub_datasets_never_read,
                                static_html_dir,
                                output_dir, sub_folder, main_html_file_name, min_tb_limit, title_replace_str, ):
    # Prepare main pandas dataframe for datasets never read
    df_pd_main = prep_pd_df_never_read_for_main_html(
        df_pd_main_datasets_never_read=df_pd_main_datasets_never_read
    )
    # Prepare sub details pandas dataframe
    df_pd_sub = prep_pd_df_never_read_for_sub_html(
        df_pd_sub_datasets_never_read=df_pd_sub_datasets_never_read
    )
    create_main_html(is_disk, df_pd_main, static_html_dir, output_dir, sub_folder, main_html_file_name,
                     min_tb_limit, title_replace_str, )
    create_sub_htmls(df_pd_sub, output_dir, sub_folder)


@click.command()
@click.option('--disk', 'is_disk', flag_value=True)
@click.option('--tape', 'is_disk', flag_value=False)
@click.option('--static_html_dir', default=None, type=str, required=True,
              help='Html directory for main.html file. For example: ~/CMSSpark/src/html/rucio_datasets_last_access_ts')
@click.option('--output_dir', default=None, type=str, required=True, help='I.e. /eos/user/c/cmsmonit/www/rucio')
@click.option('--rses_pickle', default=None, type=str, required=True,
              help='Please see rse_name_id_map_pickle.py | Default: ~/CMSSpark/static/rucio/rses.pickle')
@click.option('--min_tb_limit', default=None, type=float, required=True,
              help='Minimum TB limit to filter dataset sizes. 1TB for disk, 10TB for Tape')
def main(is_disk=None, static_html_dir=None, output_dir=None, rses_pickle=None, min_tb_limit=None):
    """
        Main function that run Spark dataframe creations and create html pages
    """
    print(f'Disk or Tape: {is_disk}\n')
    print(f'Static html dir: {static_html_dir}\n')
    print(f'Output dir: {output_dir}\n')
    print(f'Rses pickle dir: {rses_pickle}\n')
    print(f'Min TB limit: {min_tb_limit}\n')
    #
    if is_disk:
        output_dir = os.path.join(output_dir, "disk")
    else:
        output_dir = os.path.join(output_dir, "tape")

    os.makedirs(output_dir, exist_ok=True)

    # Get rse ids of only Disk or only Tape RSEs
    filtered_rses_id_name_map = get_rse_ids(rse_id_name_map_pickle=rses_pickle, is_disk=is_disk)

    # Start Spark aggregations
    #
    spark = get_spark_session()
    df_dbs_f_d = get_df_dbs_f_d(spark)
    df_replicas = get_df_replicas(spark, list(filtered_rses_id_name_map.values()))
    df_dids_files = get_df_dids_files(spark)
    df_replicas_j_dids = get_df_replicas_j_dids(df_replicas, df_dids_files)
    df_file_rse_ts_size = get_df_file_rse_ts_size(df_replicas_j_dids)
    df_dataset_file_rse_ts_size = get_df_dataset_file_rse_ts_size(df_file_rse_ts_size, df_dbs_f_d)

    # Create htmls for all datasets not read since [3,6,12] months
    for n_months_filter in [3, 6, 12]:
        # Create htmls for datasets not read since N months
        # Get Spark dataframe for RSE details sub htmls
        df_sub_not_read_since = get_df_sub_not_read_since(
            df_dataset_file_rse_ts_size=df_dataset_file_rse_ts_size,
            filtered_rses_id_name_map=filtered_rses_id_name_map,
            min_tb_limit=min_tb_limit,
            n_months_filter=n_months_filter
        )

        # includes only dataset values, no RSE details
        df_main_not_read_since = get_df_main_not_read_since(df_sub_not_read_since)
        #
        title_not_read_since = f'Datasets which have not been read since {n_months_filter} months'
        create_htmls_for_not_read_since(is_disk=is_disk,
                                        df_pd_main_not_read_since=df_main_not_read_since.toPandas(),
                                        df_pd_sub_not_read_since=df_sub_not_read_since.toPandas(),
                                        static_html_dir=static_html_dir,
                                        output_dir=output_dir,
                                        sub_folder=f'rses_{n_months_filter}_months',
                                        main_html_file_name=f'datasets_not_read_since_{n_months_filter}_months.html',
                                        min_tb_limit=min_tb_limit,
                                        title_replace_str=title_not_read_since,
                                        )

    # Create htmls for datasets never read
    # Get both Spark dataframes
    df_main_datasets_never_read, df_sub_datasets_never_read = get_df_main_datasets_never_read(
        df_dataset_file_rse_ts_size=df_dataset_file_rse_ts_size,
        filtered_rses_id_name_map=filtered_rses_id_name_map,
        min_tb_limit=min_tb_limit,
    )
    #
    title_never_read = 'Datasets which were never read'
    create_htmls_for_never_read(is_disk=is_disk,
                                df_pd_main_datasets_never_read=df_main_datasets_never_read.toPandas(),
                                df_pd_sub_datasets_never_read=df_sub_datasets_never_read.toPandas(),
                                static_html_dir=static_html_dir,
                                output_dir=output_dir,
                                sub_folder='rses_never',
                                main_html_file_name=f'datasets_never_read.html',
                                min_tb_limit=min_tb_limit,
                                title_replace_str=title_never_read,
                                )


if __name__ == '__main__':
    main()
