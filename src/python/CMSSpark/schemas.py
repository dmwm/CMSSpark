#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Schema module for DBS/PhEDEx/AAA/EOS/CMSSW/JobMonitoring meta-data on HDFS
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

def schema_data_tiers():
    """
    DBS DATA_TIERS table schema

    DATA_TIER_ID NOT NULL NUMBER(38)
    DATA_TIER_NAME NOT NULL VARCHAR2(100)
    CREATION_DATE NOT NULL NUMBER(38)
    CREATE_BY NOT NULL VARCHAR2(100)

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("data_tier_id", IntegerType(), True),
            StructField("data_tier_name", StringType(), True),
            StructField("data_tier_creation_date", DoubleType(), True),
            StructField("data_tier_create_by", StringType(), True)
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

def schema_file_lumis():
    """DBS FILE_LUMIS table schema

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("fl_run_num", IntegerType(), True),
            StructField("fl_lumi_section_num", StringType(), True),
            StructField("fl_file_id", IntegerType(), True)
        ])

def schema_phedex_summary():
    """PhEDEx summary table schema
    site,dataset,size,date,replica_date

    :returns: StructType consisting StructField array
    """
    return StructType([
            StructField("date", LongType(), True),
            StructField("site", StringType(), True),
            StructField("dataset", StringType(), True),
            StructField("size", LongType(), True),
            StructField("replica_date", LongType(), True)
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
                     StructField("block_bytes", LongType(), True),
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

def schema_dbs_phedex():
    """
    Schema for DBS+PhEDEx aggregation, see dbs_phedex.py and adjust accordingly.

    dataset_name,evts,size,date,dataset_access_type,acquisition_era_name,r_release_version,node_name,pbr_size,dataset_is_open,max_replica_time
    "/14TeV_T1T1_2BC_350_100_MadGraph/Summer12-UpgrdStdGeom_DESIGN42_V17-v1/GEN",73528,4.4569181E7,1.343676821E9,VALID,DBS2_UNKNOWN_ACQUISION_ERA,CMSSW_4_2_8_SLHCstd2_patch2,T1_US_FNAL_Buffer,4.4569181E7,y,1.361368512E9
    """
    return StructType([
            StructField("dataset_name", StringType(), True),
            StructField("evts", IntegerType(), True),
            StructField("size", DoubleType(), True),
            StructField("date", DoubleType(), True),
            StructField("dataset_access_type", StringType(), True),
            StructField("acquisition_era_name", StringType(), True),
            StructField("r_release_version", StringType(), True),
            StructField("node_name", StringType(), True),
            StructField("pbr_size", DoubleType(), True),
            StructField("dataset_is_open", StringType(), True),
            StructField("max_replica_time", DoubleType(), True)
        ])

def schema_cmssw():
    """Schema for CMSSW record
    {"UNIQUE_ID":{"string":"08F8DD3A-0FFE-E611-B710-BC305B3909F1-1"},"FILE_LFN":{"string":"/store/data/Run2016F/JetHT/AOD/23Sep2016-v1/70000/D2B97318-A186-E611-A1EA-F8BC123BBE3C.root"},"FILE_SIZE":{"string":"3865077537"},"CLIENT_DOMAIN":{"string":"in2p3.fr"},"CLIENT_HOST":{"string":"sbgwn141"},"SERVER_DOMAIN":{"string":"in2p3.fr"},"SERVER_HOST":{"string":"sbgse20"},"SITE_NAME":{"string":"T2_FR_IPHC"},"READ_BYTES_AT_CLOSE":{"string":"438385807"},"READ_BYTES":{"string":"438385807"},"READ_SINGLE_BYTES":{"string":"8913451"},"READ_SINGLE_OPERATIONS":{"string":"19"},"READ_SINGLE_AVERAGE":{"string":"469129"},"READ_SINGLE_SIGMA":{"string":"1956390"},"READ_VECTOR_BYTES":{"string":"429472356"},"READ_VECTOR_OPERATIONS":{"string":"58"},"READ_VECTOR_AVERAGE":{"string":"7404700"},"READ_VECTOR_SIGMA":{"string":"6672770"},"READ_VECTOR_COUNT_AVERAGE":{"string":"37.4138"},"READ_VECTOR_COUNT_SIGMA":{"string":"35.242"},"FALLBACK":{"string":"-"},"USER_DN":{"string":"/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=vmariani/CN=751637/CN=Valentina Mariani/CN=1516526926/CN=481221408/CN=1301887600/CN=1846615186/CN=2041527197"},"APP_INFO":{"string":"3809_https://glidein.cern.ch/3809/170228:163214:vmariani:crab:JetHT:Run2016F:DSm:4b_0"},"START_TIME":{"string":"1488325657"},"END_TIME":{"string":"1488326400"},"START_DATE":{"long":1488322057000},"END_DATE":{"long":1488322800000},"INSERT_DATE":{"long":1488323999000}}
    """
    return StructType([
        StructField("UNIQUE_ID", StringType(), True),
        StructField("FILE_LFN", StringType(), True),
        StructField("FILE_SIZE", StringType(), True),
        StructField("CLIENT_DOMAIN", StringType(), True),
        StructField("CLIENT_HOST", StringType(), True),
        StructField("SERVER_DOMAIN", StringType(), True),
        StructField("SERVER_HOST", StringType(), True),
        StructField("SITE_NAME", StringType(), True),
        StructField("READ_BYTES_AT_CLOSE", StringType(), True),
        StructField("READ_BYTES", StringType(), True),
        StructField("READ_SINGLE_BYTES", StringType(), True),
        StructField("READ_SINGLE_OPERATIONS", StringType(), True),
        StructField("READ_SINGLE_AVERAGE", StringType(), True),
        StructField("READ_SINGLE_SIGMA", StringType(), True),
        StructField("READ_VECTOR_BYTES", StringType(), True),
        StructField("READ_VECTOR_OPERATIONS", StringType(), True),
        StructField("READ_VECTOR_AVERAGE", StringType(), True),
        StructField("READ_VECTOR_SIGMA", StringType(), True),
        StructField("READ_VECTOR_COUNT_AVERAGE", StringType(), True),
        StructField("READ_VECTOR_COUNT_SIGMA", StringType(), True),
        StructField("USER_DN", StringType(), True),
        StructField("APP_INFO", StringType(), True),
        StructField("START_TIME", StringType(), True),
        StructField("END_TIME", StringType(), True),
        StructField("START_DATE", LongType(), True),
        StructField("END_DATE", LongType(), True),
        StructField("INSERT_DATE", LongType(), True)
    ])

def schema_jm():
    """Schema for JobMonitoring record
    {"JobId":{"string":"1672451388"},"FileName":{"string":"//store/mc/PhaseIIFall16GS82/QCD_Flat_Pt-15to7000_TuneCUETP8M1_14TeV_pythia8/GEN-SIM/90X_upgrade2023_realistic_v1-v1/110000/92A9E11F-C0F1-E611-9A55-001E67E6F8AF.root"},"IsParentFile":{"string":"0"},"ProtocolUsed":{"string":"Remote"},"SuccessFlag":{"string":"1"},"FileType":{"string":"EDM"},"LumiRanges":{"string":"unknown"},"StrippedFiles":{"string":"0"},"BlockId":{"string":"602064"},"StrippedBlocks":{"string":"0"},"BlockName":{"string":"Dummy"},"InputCollection":{"string":"DoesNotApply"},"Application":{"string":"CMSSW"},"Type":{"string":"reprocessing"},"SubmissionTool":{"string":"wmagent"},"InputSE":null,"TargetCE":null,"SiteName":{"string":"T0_CH_CERN"},"SchedulerName":{"string":"PYCONDOR"},"JobMonitorId":{"string":"unknown"},"TaskJobId":{"string":"1566463230"},"SchedulerJobIdV2":{"string":"664eef36-f1c3-11e6-88b9-02163e0184a6-367_0"},"TaskId":{"string":"35076445"},"TaskMonitorId":{"string":"wmagent_pdmvserv_task_SMP-PhaseIIFall16GS82-00005__v1_T_170213_041344_640"},"JobExecExitCode":{"string":"0"},"JobExecExitTimeStamp":{"long":1488375506000},"StartedRunningTimeStamp":{"long":1488374686000},"FinishedTimeStamp":{"long":1488375506000},"WrapWC":{"string":"820"},"WrapCPU":{"string":"1694.3"},"ExeCPU":{"string":"0"},"UserId":{"string":"124370"},"GridName":{"string":"Alan Malta Rodrigues"}}
    """
    return StructType([
        StructField("JobId", StringType(), True),
        StructField("FileName", StringType(), True),
        StructField("IsParentFile", StringType(), True),
        StructField("ProtocolUsed", StringType(), True),
        StructField("SuccessFlag", StringType(), True),
        StructField("FileType", StringType(), True),
        StructField("LumiRanges", StringType(), True),
        StructField("StrippedFiles", StringType(), True),
        StructField("BlockId", StringType(), True),
        StructField("StrippedBlocks", StringType(), True),
        StructField("BlockName", StringType(), True),
        StructField("InputCollection", StringType(), True),
        StructField("Application", StringType(), True),
        StructField("ApplicationVersion", StringType(), True),
        StructField("Type", StringType(), True),
        StructField("GenericType", StringType(), True),
        StructField("NewGenericType", StringType(), True),
        StructField("NewType", StringType(), True),
        StructField("SubmissionTool", StringType(), True),
        StructField("InputSE", StringType(), True),
        StructField("TargetCE", StringType(), True),
        StructField("SiteName", StringType(), True),
        StructField("SchedulerName", StringType(), True),
        StructField("JobMonitorId", StringType(), True),
        StructField("TaskJobId", StringType(), True),
        StructField("SchedulerJobIdV2", StringType(), True),
        StructField("TaskId", StringType(), True),
        StructField("TaskMonitorId", StringType(), True),
        StructField("NEventsPerJob", StringType(), True),
        StructField("NTaskSteps", StringType(), True),
        StructField("JobExecExitCode", StringType(), True),
        StructField("JobExecExitTimeStamp", LongType(), True),
        StructField("StartedRunningTimeStamp", LongType(), True),
        StructField("FinishedTimeStamp", LongType(), True),
        StructField("WrapWC", StringType(), True),
        StructField("WrapCPU", StringType(), True),
        StructField("ExeCPU", StringType(), True),
        StructField("NCores", StringType(), True),
        StructField("NEvProc", StringType(), True),
        StructField("NEvReq", StringType(), True),
        StructField("WNHostName", StringType(), True),
        StructField("JobType", StringType(), True),
        StructField("UserId", StringType(), True),
        StructField("GridName", StringType(), True)
        ])

def schema_asodb():
    """
    ASO table schema
    Map for values of tm_transfer_state and tm_publication_state in
    https://github.com/dmwm/CRABServer/blob/master/src/python/ServerUtilities.py#L61-L77
    
    tm_id VARCHAR(60) NOT NULL,
    tm_username VARCHAR(30) NOT NULL,
    tm_taskname VARCHAR(255) NOT NULL,
    tm_destination VARCHAR(100) NOT NULL,
    tm_destination_lfn VARCHAR(1000) NOT NULL,
    tm_source VARCHAR(100) NOT NULL,
    tm_source_lfn VARCHAR(1000) NOT NULL,
    tm_filesize NUMBER(20) NOT NULL,
    tm_publish NUMBER(1) NOT NULL,
    tm_jobid NUMBER(10) NOT NULL,
    tm_job_retry_count NUMBER(5),
    tm_type VARCHAR(20) NOT NULL,
    tm_aso_worker VARCHAR(100),
    tm_transfer_retry_count NUMBER(5) DEFAULT 0,
    tm_transfer_max_retry_count NUMBER(5) DEFAULT 2,
    tm_publication_retry_count NUMBER(5) DEFAULT 0,
    tm_publication_max_retry_count NUMBER(5) DEFAULT 2,
    tm_rest_host VARCHAR(50) NOT NULL,
    tm_rest_uri VARCHAR(255) NOT NULL,
    tm_transfer_state NUMBER(1) NOT NULL,
    tm_publication_state NUMBER(1) NOT NULL,
    tm_transfer_failure_reason VARCHAR(1000),
    tm_publication_failure_reason VARCHAR(1000),
    tm_fts_id VARCHAR(255),
    tm_fts_instance VARCHAR(255),
    tm_last_update NUMBER(11) NOT NULL,
    tm_start_time NUMBER(11) NOT NULL,
    tm_end_time NUMBER(11)
    """
    return StructType([
        StructField("tm_id", StringType(), True),
        StructField("tm_username", StringType(), True),
        StructField("tm_taskname", StringType(), True),
        StructField("tm_destination", StringType(), True),
        StructField("tm_destination_lfn", StringType(), True),
        StructField("tm_source", StringType(), True),
        StructField("tm_source_lfn", StringType(), True),
        StructField("tm_filesize", DoubleType(), True),
        StructField("tm_publish", DoubleType(), True),
        StructField("tm_jobid", IntegerType(), True),
        StructField("tm_job_retry_count", IntegerType(), True),
        StructField("tm_type", StringType(), True),
        StructField("tm_aso_worker", StringType(), True),
        StructField("tm_transfer_retry_count", IntegerType(), True),
        StructField("tm_transfer_max_retry_count", IntegerType(), True),
        StructField("tm_publication_retry_count", IntegerType(), True),
        StructField("tm_publication_max_retry_count", IntegerType(), True),
        StructField("tm_rest_host", StringType(), True),
        StructField("tm_rest_uri", StringType(), True),
        StructField("tm_transfer_state", IntegerType(), True),
        StructField("tm_publication_state", IntegerType(), True),
        StructField("tm_transfer_failure_reason", StringType(), True),
        StructField("tm_publication_failure_reason", StringType(), True),
        StructField("tm_fts_id", StringType(), True),
        StructField("tm_fts_instance", StringType(), True),
        StructField("tm_last_update", DoubleType(), True),
        StructField("tm_start_time", DoubleType(), True),
        StructField("tm_end_time", DoubleType(), True),
    ])


def aggregated_data_schema():
    """
    dn: string (nullable = true)
    dataset_name: string (nullable = true)
    site_name: string (nullable = true)
    app: string (nullable = true)
    uid: integer (nullable = true)
    stream: string (nullable = true)
    timestamp: integer (nullable = true)
    nacc: integer (nullable = true)
    distinct_users: integer (nullable = true)
    site_tier: string (nullable = true)
    cpu_time: double (nullable = true)
    wc_time: double (nullable = true)
    primary_name: string (nullable = true)
    processing_name: string (nullable = true)
    data_tier: string (nullable = true)
    """

    return StructType([
        StructField("dn", StringType(), True),
        StructField("dataset_name", StringType(), True),
        StructField("site_name", StringType(), True),
        StructField("app", StringType(), True),
        StructField("uid", LongType(), True),
        StructField("stream", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("nacc", IntegerType(), True),
        StructField("distinct_users", IntegerType(), True),
        StructField("site_tier", StringType(), True),
        StructField("cpu_time", DoubleType(), True),
        StructField("wc_time", DoubleType(), True),
        StructField("primary_name", StringType(), True),
        StructField("processing_name", StringType(), True),
        StructField("data_tier", StringType(), True),
    ])

def schema_empty_aaa():
    """
    src_experiment_site: string (nullable = true)
    user_dn: string (nullable = true)
    file_lfn: string (nullable = true)
    """
    return StructType([
        StructField("src_experiment_site", StringType(), True),
        StructField("user_dn", StringType(), True),
        StructField("file_lfn", StringType(), True),
    ])

def schema_empty_eos():
    """
    src_experiment_site: string (nullable = true)
    """
    return StructType([
        StructField("file_lfn", StringType(), True),
        StructField("user_dn", StringType(), True),
        StructField("host", StringType(), True),
    ])

