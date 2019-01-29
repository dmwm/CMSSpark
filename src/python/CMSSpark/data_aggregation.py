#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Justinas Rumševičius <justinas.rumsevicius AT gmail [DOT] com>
"""
Spark script to collect data from DBS and AAA, CMSSW, EOS, JM streams on HDFS and aggregate them into
records that would be fed into MONIT system.
"""

import re
import time
import argparse
import hashlib

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, cmssw_tables, aaa_tables_enr, eos_tables, jm_tables, phedex_tables
from CMSSpark.spark_utils import spark_context, print_rows, split_dataset
from CMSSpark.utils import elapsed_time
from CMSSpark.data_collection import  yesterday, short_date_string, long_date_string, output_dataframe, run_query, short_date_to_unix
from pyspark.sql.functions import desc
from pyspark.sql.functions import split, col

LET_PAT = re.compile(r'^CN=[a-zA-Z]')
NUM_PAT = re.compile(r'^CN=[0-9]')

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process DBS + [AAA, CMSSW, EOS, JM] metadata"

        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)

        self.parser.add_argument("--inst", action="store",
            dest="inst", default="global", help='DBS instance on HDFS: global (default), phys01, phys02, phys03')
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select data for specific date (YYYYMMDD)')
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="Run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="Verbose output")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help='Output directory path')
        self.parser.add_argument("--aaa_hdir", action="store",
            dest="aaa_hdir", default="", help='AAA input directory path')


def run_agg_jm(date, ctx, sql_context, verbose=False):
    """
    Runs aggregation for JobMonitoring stream for a certain date.
    Function produces a dataframe that contains site name, dataset name, number of access, distinct users and stream.
    Result dataframe is sorted by nacc.
    """
    print('Starting JobMonitoring part')

    # Make a UNIX timestamp from date
    unix_date = short_date_to_unix(short_date_string(date))

    # Convert date
    date = long_date_string(date)

    # Create JobMonitoring tables in sql_context
    jm_df = jm_tables(ctx, sql_context, date=date, verbose=verbose)

    if verbose:
        print('Found %s records in JobMonitoring stream' % jm_df['jm_df'].count())

    # - site name                +
    # - dataset name             +
    # - app                      +
    # - uid                      +
    # - dn                       +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: crab             +
    # - timestamp                +
    # - site tier                +
    # - cpu time                 +

    cols = ['SiteName AS site_name',
            'dataset_name',
            'count(dataset_name) AS nacc',
            'count(distinct(UserId)) AS distinct_users',
            '\"crab\" AS stream',
            '%s AS timestamp' % unix_date,
            'first_value(tier_from_site_name(SiteName)) AS site_tier',
            'SUM(WrapCPU) AS cpu_time']

    # Build a query with "cols" columns
#    query = ("SELECT %s FROM jm_df "\
#             "JOIN f_b_s_df ON f_b_s_df.file_name = jm_df.FileName "\
#             "GROUP BY jm_df.SiteName, dataset_name") \
    cols = ['SiteName AS site_name',
            'dataset_name',
            'stream4app(jm_df.SubmissionTool) AS app',
            'dn2uuid(GridName) AS uid',
            'parse_dn(GridName) AS dn',
            '\"crab\" AS stream',
            '%s AS timestamp' % unix_date,
            'WrapCPU AS cpu',
            'WrapWC as wc']
    query = "SELECT %s FROM jm_df "\
             "JOIN f_b_s_df ON f_b_s_df.file_name = jm_df.FileName " \
             % ','.join(cols)
    cols = ['dn', 'dataset_name', 'site_name', 'app',
            'uid', 'stream', 'timestamp',
            'count(dataset_name) AS nacc',
            'count(dn) AS distinct_users',
            'tier_from_site_name(site_name) AS site_tier',
            'SUM(cpu) AS cpu_time',
            'SUM(wc) AS wc_time']
    query = "SELECT %s FROM (%s) QUERY1 GROUP BY dn, dataset_name, site_name, app, uid, stream, site_tier, timestamp" \
            % (','.join(cols), query)

    result = run_query(query, sql_context, verbose)

    # result = result.sort(desc("nacc"))

    # Split "dataset" column into "primds", "procds" and "tier"
    result = split_dataset_col(result, 'dataset_name')

    if verbose:
        print('Finished JobMonitoring part (output is %s records)' % result.count())
    else:
        print('Finished JobMonitoring part')

    return result


def run_agg_eos(date, ctx, sql_context, verbose=False):
    """
    Runs aggregation for EOS stream for a certain date.
    Function produces a dataframe that contains site name, dataset name, number of access, distinct users and stream.
    Site name is taken from f_b_s_df table which is joined by file name.
    Result dataframe is sorted by nacc.
    """
    print('Starting EOS part')

    # Make a UNIX timestamp from date
    unix_date = short_date_to_unix(short_date_string(date))

    # Convert date
    date = short_date_string(date)

    # Create EOS tables in sql_context
    eos_df = eos_tables(sql_context, date=date, verbose=verbose)

    if verbose:
        print('Found %s records in EOS stream' % eos_df['eos_df'].count())

    # - site name                +
    # - dataset name             +
    # - app                      +
    # - uid                      +
    # - dn                       +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: eos              +
    # - timestamp                +
    # - site tier                +
    # - cpu time                -1

    cols = ['site_name',
            'dataset_name',
            'count(dataset_name) AS nacc',
            'count(distinct(eos_df.user_dn)) AS distinct_users',
            '\"eos\" as stream',
            '%s AS timestamp' % unix_date,
            'first_value(tier_from_site_name(site_name)) AS site_tier',
            '-1 AS cpu_time']

    # Build a query with "cols" columns
#    query = ("SELECT %s FROM eos_df " \
#             "JOIN f_b_s_df ON f_b_s_df.file_name = eos_df.file_lfn " \
#             "GROUP BY site_name, dataset_name") \
#             % ','.join(cols)
    cols = ['site_name',
            'dataset_name',
            'parse_app(eos_df.application) AS app',
            'dn2uuid(eos_df.user_dn) AS uid',
            'parse_dn(eos_df.user_dn) AS dn',
            '\"eos\" as stream',
            '%s AS timestamp' % unix_date,
            '-1 AS cpu']
    query = "SELECT %s FROM eos_df " \
             "JOIN f_b_s_df ON f_b_s_df.file_name = eos_df.file_lfn " \
             % ','.join(cols)
    cols = ['dn', 'dataset_name', 'site_name', 'app',
            'uid', 'stream', 'timestamp',
            'count(dataset_name) AS nacc',
            'count(dn) AS distinct_users',
            'tier_from_site_name(site_name) AS site_tier',
            '-1 AS cpu_time', '-1 AS wc_time']
    query = "SELECT %s FROM (%s) QUERY1 GROUP BY dn, dataset_name, site_name, app, uid, stream, site_tier, timestamp" \
            % (','.join(cols), query)

    result = run_query(query, sql_context, verbose)

    # result = result.sort(desc("nacc"))

    # Split "dataset" column into "primds", "procds" and "tier"
    result = split_dataset_col(result, 'dataset_name')

    if verbose:
        print('Finished EOS part (output is %s records)' % result.count())
    else:
        print('Finished EOS part')

    return result


def run_agg_aaa(date, ctx, sql_context, hdir='hdfs:///project/monitoring/archive/xrootd/enr/gled', verbose=False):
    """
    Runs aggregation for JobMonitoring stream for a certain date.
    Function produces a dataframe that contains site name, dataset name, number of access, distinct users and stream.
    Data is taken from /project/monitoring/archive/xrootd/enr/gled and not the default location (raw instead of enr)
    because enr records have src_experiment_site. src_experiment_site is used as site_name.
    Result dataframe is sorted by nacc.
    """
    print('Starting AAA part')

    # Make a UNIX timestamp from date
    unix_date = short_date_to_unix(short_date_string(date))

    # Convert date
    date = short_date_string(date)

    # Create AAA tables in sql_context
    aaa_df = aaa_tables_enr(sql_context, hdir=hdir, date=date, verbose=verbose)

    if verbose:
        print('Found %s records in AAA stream' % aaa_df['aaa_df'].count())

    # - site name                +
    # - dataset name             +
    # - app                      +
    # - uid                      +
    # - dn                       +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: aaa              +
    # - timestamp                +
    # - site tier                +
    # - cpu time                -1

    cols = ['src_experiment_site AS site_name',
            'dataset_name',
            'count(dataset_name) AS nacc',
            'count(distinct(aaa_df.user_dn)) AS distinct_users',
            '\"aaa\" as stream',
            '%s AS timestamp' % unix_date,
            'first_value(tier_from_site_name(src_experiment_site)) AS site_tier',
            '-1 AS cpu_time']

    # Build a query with "cols" columns
#    query = ("SELECT %s FROM aaa_df " \
#             "JOIN f_b_s_df ON f_b_s_df.file_name = aaa_df.file_lfn " \
#             "GROUP BY src_experiment_site, dataset_name") \
#             % ','.join(cols)
    cols = ['src_experiment_site AS site_name',
            'dataset_name',
            '\"xrootd\" AS app',
            'dn2uuid(aaa_df.user_dn) AS uid',
            'parse_dn(aaa_df.user_dn) AS dn',
            '\"aaa\" as stream',
            '%s AS timestamp' % unix_date,
            '-1 AS cpu']
    query = "SELECT %s FROM aaa_df " \
             "JOIN f_b_s_df ON f_b_s_df.file_name = aaa_df.file_lfn " \
             % ','.join(cols)
    cols = ['dn', 'dataset_name', 'site_name', 'app',
            'uid', 'stream', 'timestamp',
            'count(dataset_name) AS nacc',
            'count(dn) AS distinct_users',
            'tier_from_site_name(site_name) AS site_tier',
            '-1 AS cpu_time', '-1 AS wc_time']
    query = "SELECT %s FROM (%s) QUERY1 GROUP BY dn, dataset_name, site_name, app, uid, stream, site_tier, timestamp" \
            % (','.join(cols), query)

    result = run_query(query, sql_context, verbose)

    # result = result.sort(desc("nacc"))

    # Split "dataset" column into "primds", "procds" and "tier"
    result = split_dataset_col(result, 'dataset_name')

    if verbose:
        print('Finished AAA part (output is %s records)' % result.count())
    else:
        print('Finished AAA part')

    return result


def run_agg_cmssw(date, ctx, sql_context, verbose=False):
    """
    Runs aggregation for CMSSW stream for a certain date.
    Function produces a dataframe that contains site name, dataset name, number of access, distinct users and stream.
    Result dataframe is sorted by nacc.
    """
    print('Starting CMSSW part')

    # Make a UNIX timestamp from date
    unix_date = short_date_to_unix(short_date_string(date))

    # Convert date
    date = long_date_string(date)

    # Create CMSSW tables in sql_context
    cmssw_df = cmssw_tables(ctx, sql_context, date=date, verbose=verbose)

    if verbose:
        print('Found %s records in CMSSW stream' % cmssw_df['cmssw_df'].count())

    # - site name                +
    # - dataset name             +
    # - app                      +
    # - uid                      +
    # - dn                       +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: cmssw            +
    # - timestamp                +
    # - site tier                +
    # - cpu time                -1
    

    cols = ['cmssw_df.SITE_NAME AS site_name',
            'dataset_name',
            'count(dataset_name) AS nacc',
            'count(distinct(USER_DN)) AS distinct_users',
            '\"cmssw\" as stream',
            '%s AS timestamp' % unix_date,
            'first_value(tier_from_site_name(cmssw_df.SITE_NAME)) AS site_tier',
            '-1 AS cpu_time']

    # Build a query with "cols" columns
#    query = ("SELECT %s FROM cmssw_df "\
#             "JOIN f_b_s_df ON f_b_s_df.file_name = cmssw_df.FILE_LFN "\
#             "GROUP BY cmssw_df.SITE_NAME, dataset_name") \
#             % ','.join(cols)
    cols = ['cmssw_df.SITE_NAME AS site_name',
            'dataset_name',
            'parse_app(cmssw_df.APP_INFO) AS app',
            'dn2uuid(cmssw_df.USER_DN) AS uid',
            'parse_dn(cmssw_df.USER_DN) AS dn',
            'stream4app(cmssw_df.APP_INFO) as stream',
            '%s AS timestamp' % unix_date,
            '-1 AS cpu']
    query = "SELECT %s FROM cmssw_df "\
             "JOIN f_b_s_df ON f_b_s_df.file_name = cmssw_df.FILE_LFN " \
             % ','.join(cols)
    cols = ['dn', 'dataset_name', 'site_name', 'app',
            'uid', 'stream', 'timestamp',
            'count(dataset_name) AS nacc',
            'count(dn) AS distinct_users',
            'tier_from_site_name(site_name) AS site_tier',
            '-1 AS cpu_time', '-1 AS wc_time']
    query = "SELECT %s FROM (%s) QUERY1 GROUP BY dn, dataset_name, site_name, app, uid, stream, site_tier, timestamp" \
            % (','.join(cols), query)

    result = run_query(query, sql_context, verbose)

    # result = result.sort(desc("nacc"))

    # Split "dataset" column into "primds", "procds" and "tier"
    result = split_dataset_col(result, 'dataset_name')

    if verbose:
        print('Finished CMSSW part (output is %s records)' % result.count())
    else:
        print('Finished CMSSW part')

    return result


def quiet_logs(sc):
    """
    Sets logger's level to ERROR so INFO logs would not show up.
    """
    print('Will set log level to ERROR')
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
    print('Did set log level to ERROR')


def create_file_block_site_table(ctx, sql_context, verbose=False):
    """
    Joins fdf, bdf, ddf and PhEDEx tables and produces table with file name, block name, dataset name and site name.
    Site name is obtained from PhEDEx. Before site name is used, it is cleaned with clean_site_name function.
    After join is complete, only unique records are left in the table by using DISTINCT function.
    """
    print('Starting file_block_site generation')

    cols = ['f_logical_file_name AS file_name',
            'b_block_name AS block_name',
            'clean_site_name(node_name) AS site_name',
            'd_dataset AS dataset_name']

    # Join FDF and BDF by f_block_id and b_block_id
    query = ("SELECT %s FROM fdf " \
             "JOIN bdf ON fdf.f_block_id = bdf.b_block_id "\
             "JOIN ddf ON fdf.f_dataset_id = ddf.d_dataset_id "\
             "JOIN phedex_df ON bdf.b_block_name = phedex_df.block_name") % ','.join(cols)

    if verbose:
        print('Will run query to generate temp file_block_site table')

    result = run_query(query, sql_context, verbose)
    result.registerTempTable('f_b_all_df')

    query_distinct = ("SELECT DISTINCT * FROM f_b_all_df ORDER  BY file_name")
    result_distinct = run_query(query_distinct, sql_context, verbose)
    result_distinct.registerTempTable('f_b_s_df')

    if verbose:
        print('Temp table from joined DDF, FDF, BDF and PhEDEx')
        print('After DISTINCT query count changed %s -> %s' % (result.count(), result_distinct.count()))
        result_distinct.show(20)
        print_rows(result_distinct, query_distinct, verbose, 5)
        result_distinct.printSchema()

    print('Finished file_block_site generation')


def clean_site_name(s):
    """
    Splits site name by _ (underscore), takes no more than first three parts and joins them with _.
    This way site name always have at most three parts, separated by _.
    First three parts represent a tier, a country and a lab/university name.
    """
    split = s.split('_')
    split = split[0:3]

    # Remove empty strings which may appear when s is T0_USA_
    split = filter(None, split)

    join = '_'.join(split)
    return join

def parse_dn(dn):
    "Parse user DN and extract only user name and real name"
    dn = str(dn).split('&')[0]
    cns = [x for x in dn.split('/') if x.startswith('CN=') and not NUM_PAT.match(x)]
    if len(cns):
        name = cns[-1].split('=')[-1] # /CN=user/CN=First Last Name we return First Last Name
    else:
        name = str(dn) # when we're unable to split DN with / we return it as is
    return name.replace('CN=', '')

def stream4app(app):
    "Parse CMSSW APP_INFO attribute and assign appropriate stream"
    if not app:
        return 'cmssw'
    if app and 'crab' in app:
        return 'crab'
    return app

def parse_app(app):
    "Parse CMSSW APP_INFO attribute"
    if not app or app == "')":
        return "unknown"
    if 'crab' in app:
        return 'crab'
    return app

def dn2uuid(dn):
    "Convert user DN to UID, we take first 16 digits of the int base 16 of the dn hash"
    return int(hashlib.sha1(parse_dn(dn)).hexdigest(), 16) % (10**16)

def tier_from_site_name(s):
    """
    Splits site name by _ (underscore), and takes only the first part that represents tier.
    """
    split = s.split('_')
    tier = str(split[0])
    return tier

def split_dataset_col(df, dcol):
    """
    Split dataset name in DataFrame into primary_name, processing_name , data_tier components.
    Keep original column
    """
    ndf = df.withColumn("primary_name", split(col(dcol), "/").alias('primary_name').getItem(1))\
            .withColumn("processing_name", split(col(dcol), "/").alias('processing_name').getItem(2))\
            .withColumn("data_tier", split(col(dcol), "/").alias('data_tier').getItem(3))
    return ndf


def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()

    print("Input arguments: %s" % opts)

    start_time = time.time()
    verbose = opts.verbose
    # VK: turn verbose output for debugging purposes
    verbose = 1
    yarn = opts.yarn
    inst = opts.inst
    date = opts.date
    fout = opts.fout
    aaa_hdir = opts.aaa_hdir

    if  inst.lower() in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)

    # Create spark context
    ctx = spark_context('cms', yarn, verbose)

    quiet_logs(ctx)

    # Create SQL context to be used for SQL queries
    sql_context = SQLContext(ctx)

    # Initialize DBS tables
    dbs_tables(sql_context, inst=inst, verbose=verbose, tables=['fdf', 'bdf', 'ddf'])

    # Initialize PhEDEx table to be used in file_block_site table
    if date:
        ddate = '%s-%s-%s' % (date[:4], date[4:6], date[6:8])
        phedex_tables(sql_context, verbose=verbose, fromdate=ddate, todate=ddate)
    else:
        phedex_tables(sql_context, verbose=verbose)

    # Register clean_site_name to be used with SQL queries
    sql_context.udf.register("clean_site_name", clean_site_name)

    # Register tier_from_site_name to be used with SQL queries
    sql_context.udf.register("tier_from_site_name", tier_from_site_name)

    # Register dn2uuid to be used with SQL queries
    sql_context.udf.register("dn2uuid", dn2uuid)

    # Register parse_app to be used with SQL queries
    sql_context.udf.register("parse_app", parse_app)

    # Register stream4app to be used with SQL queries
    sql_context.udf.register("stream4app", stream4app)

    # Register parse_dn to be used with SQL queries
    sql_context.udf.register("parse_dn", parse_dn)

    f_b_s_start_time = time.time()
    # Create temp table with file name, block name, site name and site from PhEDEx
    create_file_block_site_table(ctx, sql_context, verbose)
    f_b_s_elapsed_time = elapsed_time(f_b_s_start_time)

    cmssw_start_time = time.time()
    aggregated_cmssw_df = run_agg_cmssw(date, ctx, sql_context, verbose)
    cmssw_elapsed_time = elapsed_time(cmssw_start_time)

    aaa_start_time = time.time()
    if len(aaa_hdir) > 0:
        aggregated_aaa_df = run_agg_aaa(date, ctx, sql_context, aaa_hdir, verbose)
    else:
        aggregated_aaa_df = run_agg_aaa(date, ctx, sql_context, verbose=verbose)

    aaa_elapsed_time = elapsed_time(aaa_start_time)

    eos_start_time = time.time()
    aggregated_eos_df = run_agg_eos(date, ctx, sql_context, verbose)
    eos_elapsed_time = elapsed_time(eos_start_time)

    jm_start_time = time.time()
    aggregated_jm_df = run_agg_jm(date, ctx, sql_context, verbose)
    jm_elapsed_time = elapsed_time(jm_start_time)

    if verbose:
        print('Will union outputs from all streams to a single dataframe')
    # Schema for output is:
    # site name, dataset name, number of accesses, distinct users, stream
    all_df = aggregated_cmssw_df.unionAll(aggregated_aaa_df)
    all_df = all_df.unionAll(aggregated_eos_df)
    all_df = all_df.unionAll(aggregated_jm_df)
    all_df = all_df.sort(desc("nacc"))

    if verbose:
        print('Done joining all outputs to a single dataframe')

    fout = fout + "/" + short_date_string(date)

    # output_dataframe(fout + "/Aggregated/CMSSW/" + short_date_string(date), aggregated_cmssw_df, verbose)
    # output_dataframe(fout + "/Aggregated/AAA/" + short_date_string(date), aggregated_aaa_df, verbose)
    # output_dataframe(fout + "/Aggregated/EOS/" + short_date_string(date), aggregated_eos_df, verbose)
    # output_dataframe(fout + "/Aggregated/JobMonitoring/" + short_date_string(date), aggregated_jm_df, verbose)

    output_dataframe(fout, all_df, verbose)

    if verbose:
        cmssw_df_size = aggregated_cmssw_df.count()
        aaa_df_size = aggregated_aaa_df.count()
        eos_df_size = aggregated_eos_df.count()
        jm_df_size = aggregated_jm_df.count()
        all_df_size = all_df.count()

        print('CMSSW:')
        aggregated_cmssw_df.show(10)
        aggregated_cmssw_df.printSchema()

        print('AAA:')
        aggregated_aaa_df.show(10)
        aggregated_aaa_df.printSchema()

        print('EOS:')
        aggregated_eos_df.show(10)
        aggregated_eos_df.printSchema()

        print('JobMonitoring:')
        aggregated_jm_df.show(10)
        aggregated_jm_df.printSchema()

        print('Aggregated all:')
        all_df.show(10)
        all_df.printSchema()

        print('Output record count:')
        print('Output record count CMSSW         : %s' % cmssw_df_size)
        print('Output record count AAA           : %s' % aaa_df_size)
        print('Output record count EOS           : %s' % eos_df_size)
        print('Output record count JobMonitoring : %s' % jm_df_size)
        print('Output record count Total:        : %s' % all_df_size)

    ctx.stop()

    print('Start time         : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(start_time)))
    print('End time           : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Total elapsed time : %s' % elapsed_time(start_time))

    print('FileBlockSite elapsed time : %s' % f_b_s_elapsed_time)
    print('AAA elapsed time           : %s' % aaa_elapsed_time)
    print('CMSSW elapsed time         : %s' % cmssw_elapsed_time)
    print('EOS elapsed time           : %s' % eos_elapsed_time)
    print('JobMonitoring elapsed time : %s' % jm_elapsed_time)


if __name__ == '__main__':
    main()
