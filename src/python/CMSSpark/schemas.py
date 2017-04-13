#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : schemas.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Schema module for DBS/PhEDEx/AAA/EOS/CMSSW meta-data on HDFS
"""

# system modules
import os
import sys
import time
import json

# spark modules
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, BooleanType, LongType

def schema_processing_eras():
    """
    DBS PROCESSING_ERAS table schema

    PROCESSING_ERA_ID NOT NULL NUMBER(38)
    PROCESSING_ERA_NAME NOT NULL VARCHAR2(120)
    CREATION_DATE NOT NULL INTEGER
    CREATE_BY NOT NULL VARCHAR2(500)
    DESCRIPTION NOT NULL VARCHAR2(40)

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("processing_era_id", IntegerType(), True),
            StructField("processing_version", StringType(), True),
            StructField("creation_date", IntegerType(), True),
            StructField("create_by", StringType(), True),
            StructField("description", StringType(), True)
        ])

def schema_acquisition_eras():
    """
    DBS ACQUISITION_ERAS table schema

    ACQUISITION_ERA_ID NOT NULL NUMBER(38)
    ACQUISITION_ERA_NAME NOT NULL VARCHAR2(120)
    START_DATE NOT NULL INTEGER
    END_DATE NOT NULL INTEGER
    CREATION_DATE NOT NULL INTEGER
    CREATE_BY NOT NULL VARCHAR2(500)
    DESCRIPTION NOT NULL VARCHAR2(40)

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("acquisition_era_id", IntegerType(), True),
            StructField("acquisition_era_name", StringType(), True),
            StructField("start_date", IntegerType(), True),
            StructField("end_date", IntegerType(), True),
            StructField("creation_date", IntegerType(), True),
            StructField("create_by", StringType(), True),
            StructField("description", StringType(), True)
        ])

def schema_dataset_access_types():
    """
    DBS DATASET_ACCESS_TYPES table schema

    DATASET_ACCESS_TYPE_ID NOT NULL NUMBER(38)
    DATASET_ACCESS_TYPE NOT NULL VARCHAR2(100)

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("dataset_access_type_id", IntegerType(), True),
            StructField("dataset_access_type", StringType(), True)
        ])

def schema_datasets():
    """
    DBS DATASETS table schema

    DATASET_ID NOT NULL NUMBER(38)
    DATASET NOT NULL VARCHAR2(700)
    IS_DATASET_VALID NOT NULL NUMBER(38)
    PRIMARY_DS_ID NOT NULL NUMBER(38)
    PROCESSED_DS_ID NOT NULL NUMBER(38)
    DATA_TIER_ID NOT NULL NUMBER(38)
    DATASET_ACCESS_TYPE_ID NOT NULL NUMBER(38)
    ACQUISITION_ERA_ID NUMBER(38)
    PROCESSING_ERA_ID NUMBER(38)
    PHYSICS_GROUP_ID NUMBER(38)
    XTCROSSSECTION FLOAT(126)
    PREP_ID VARCHAR2(256)
    CREATION_DATE NUMBER(38)
    CREATE_BY VARCHAR2(500)
    LAST_MODIFICATION_DATE NUMBER(38)
    LAST_MODIFIED_BY VARCHAR2(500)

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("d_dataset_id", IntegerType(), True),
            StructField("d_dataset", StringType(), True),
            StructField("d_is_dataset_valid", IntegerType(), True),
            StructField("d_primary_ds_id", IntegerType(), True),
            StructField("d_processed_ds_id", IntegerType(), True),
            StructField("d_data_tier_id", IntegerType(), True),
            StructField("d_dataset_access_type_id", IntegerType(), True),
            StructField("d_acquisition_era_id", IntegerType(), True),
            StructField("d_processing_era_id", IntegerType(), True),
            StructField("d_physics_group_id", IntegerType(), True),
            StructField("d_xtcrosssection", DoubleType(), True),
            StructField("d_prep_id", StringType(), True),
            StructField("d_creation_date", DoubleType(), True),
            StructField("d_create_by", StringType(), True),
            StructField("d_last_modification_date", DoubleType(), True),
            StructField("d_last_modified_by", StringType(), True)
        ])

def schema_blocks():
    """
    DBS BLOCKS table schema

    BLOCK_ID NOT NULL NUMBER(38)
    BLOCK_NAME NOT NULL VARCHAR2(500)
    DATASET_ID NOT NULL NUMBER(38)
    OPEN_FOR_WRITING NOT NULL NUMBER(38)
    ORIGIN_SITE_NAME NOT NULL VARCHAR2(100)
    BLOCK_SIZE NUMBER(38)
    FILE_COUNT NUMBER(38)
    CREATION_DATE NUMBER(38)
    CREATE_BY VARCHAR2(500)
    LAST_MODIFICATION_DATE NUMBER(38)
    LAST_MODIFIED_BY VARCHAR2(500)

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("b_block_id", IntegerType(), True),
            StructField("b_block_name", StringType(), True),
            StructField("b_dataset_id", IntegerType(), True),
            StructField("b_open_for_writing", IntegerType(), True),
            StructField("b_origin_site_name", StringType(), True),
            StructField("b_block_size", DoubleType(), True),
            StructField("b_file_count", IntegerType(), True),
            StructField("b_creation_date", DoubleType(), True),
            StructField("b_create_by", StringType(), True),
            StructField("b_last_modification_date", DoubleType(), True),
            StructField("b_last_modified_by", StringType(), True)
        ])               
                         
def schema_files():
    """
    DBS FILES table schema

    FILE_ID NOT NULL NUMBER(38)
    LOGICAL_FILE_NAME NOT NULL VARCHAR2(500)
    IS_FILE_VALID NOT NULL NUMBER(38)
    DATASET_ID NOT NULL NUMBER(38)
    BLOCK_ID NOT NULL NUMBER(38)
    FILE_TYPE_ID NOT NULL NUMBER(38)
    CHECK_SUM NOT NULL VARCHAR2(100)
    EVENT_COUNT NOT NULL NUMBER(38)
    FILE_SIZE NOT NULL NUMBER(38)
    BRANCH_HASH_ID NUMBER(38)
    ADLER32 VARCHAR2(100)
    MD5 VARCHAR2(100)
    AUTO_CROSS_SECTION FLOAT(126)
    CREATION_DATE NUMBER(38)
    CREATE_BY VARCHAR2(500)
    LAST_MODIFICATION_DATE NUMBER(38)
    LAST_MODIFIED_BY VARCHAR2(500)

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("f_file_id", IntegerType(), True),
            StructField("f_logical_file_name", StringType(), True),
            StructField("f_is_file_valid", IntegerType(), True),
            StructField("f_dataset_id", IntegerType(), True),
            StructField("f_block_id", IntegerType(), True),
            StructField("f_file_type_id", IntegerType(), True),
            StructField("f_check_sum", StringType(), True),
            StructField("f_event_count", IntegerType(), True),
            StructField("f_file_size", DoubleType(), True),
            StructField("f_branch_hash_id", IntegerType(), True),
            StructField("f_adler32", StringType(), True),
            StructField("f_md5", StringType(), True),
            StructField("f_auto_cross_section", DoubleType(), True),
            StructField("f_creation_date", DoubleType(), True),
            StructField("f_create_by", StringType(), True),
            StructField("f_last_modification_date", DoubleType(), True),
            StructField("f_last_modified_by", StringType(), True)
        ])

def schema_mod_configs():
    """
    DBS DATASET_OUTPUT_MOD_CONFIGS table schema

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("mc_ds_output_mod_config_id", IntegerType(), True),
            StructField("mc_dataset_id", IntegerType(), True),
            StructField("mc_output_mod_config_id", IntegerType(), True)
        ])

def schema_out_configs():
    """
    DBS OUTPUT_MODULE_CONFIGS table schema

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("oc_output_mod_config_id", IntegerType(), True),
            StructField("oc_app_exec_id", IntegerType(), True),
            StructField("oc_release_version_id", IntegerType(), True),
            StructField("oc_parameter_set_hash_id", IntegerType(), True),
            StructField("oc_output_module_label", StringType(), True),
            StructField("oc_global_tag", StringType(), True),
            StructField("oc_scenario", StringType(), True),
            StructField("oc_creation_date", IntegerType(), True),
            StructField("oc_create_by", StringType(), True)
        ])

def schema_rel_versions():
    """
    DBS RELEASE_VERSIONS table schema

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("r_release_version_id", IntegerType(), True),
            StructField("r_release_version", StringType(), True)
        ])

def schema_phedex():
    """
    PhEDEx schema on HDFS

    :returns: StructType consisting StructField array
    """
    return StructType([StructField("now_sec", DoubleType(), True),
                     StructField("dataset_name", StringType(), True),
                     StructField("dataset_id", IntegerType(), True),
                     StructField("dataset_is_open", StringType(), True),
                     StructField("dataset_time_create", DoubleType(), True),
                     StructField("dataset_time_update", DoubleType(), True),
                     StructField("block_name", StringType(), True), 
                     StructField("block_id", IntegerType(), True),
                     StructField("block_files", IntegerType(), True),
                     StructField("block_bytes", DoubleType(), True),
                     StructField("block_is_open", StringType(), True),
                     StructField("block_time_create", DoubleType(), True),
                     StructField("block_time_update", DoubleType(), True),
                     StructField("node_name", StringType(), True),
                     StructField("node_id", IntegerType(), True),
                     StructField("br_is_active", StringType(), True),
                     StructField("br_src_files", LongType(), True),
                     StructField("br_src_bytes", LongType(), True),
                     StructField("br_dest_files", LongType(), True),
                     StructField("br_dest_bytes", LongType(), True),
                     StructField("br_node_files", LongType(), True),
                     StructField("br_node_bytes", LongType(), True),
                     StructField("br_xfer_files", LongType(), True),
                     StructField("br_xfer_bytes", LongType(), True),
                     StructField("br_is_custodial", StringType(), True),
                     StructField("br_user_group_id", IntegerType(), True),
                     StructField("replica_time_create", DoubleType(), True),
                     StructField("replica_time_updater", DoubleType(), True)])

