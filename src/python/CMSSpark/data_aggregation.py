#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Justinas Rumševičius <justinas.rumsevicius AT gmail [DOT] com>
"""
Spark script to collect data from DBS and AAA, CMSSW, EOS, JM streams on HDFS and aggregate them into
records that would be fed into MONIT system.
"""

import time
import argparse

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, cmssw_tables, aaa_tables, eos_tables, jm_tables, phedex_tables
from CMSSpark.spark_utils import spark_context, print_rows, split_dataset
from CMSSpark.utils import elapsed_time
from CMSSpark.data_collection import  yesterday, short_date_string, long_date_string, output_dataframe, run_query
from pyspark.sql.functions import desc

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


def run_agg_jm(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting JobMonitoring part'

    # Convert date
    date = long_date_string(date)

    # Create JobMonitoring tables in sql_context
    jm_df = jm_tables(ctx, sql_context, date=date, verbose=verbose)

    if verbose:
        print 'Found ' + str(jm_df['jm_df'].count()) + ' records in JobMonitoring stream'

    # - site name                +
    # - dataset name             +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: crab             +

    cols = ['SiteName AS site_name',
            'dataset_name',
            'count(dataset_name) AS nacc',
            'count(distinct(UserId)) AS distinct_users',
            '\"crab\" as stream']

    # Build a query with "cols" columns
    query = ("SELECT %s FROM jm_df "\
             "JOIN f_b_s_df ON f_b_s_df.file_name = jm_df.FileName "\
             "GROUP BY jm_df.SiteName, dataset_name") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    result = result.sort(desc("nacc"))

    if verbose:
        print 'Finished JobMonitoring part (output is ' + str(result.count()) + ' records)'

    return result


def run_agg_eos(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting EOS part'

    date = short_date_string(date)

    # Create EOS tables in sql_context
    eos_df = eos_tables(sql_context, date=date, verbose=verbose)

    if verbose:
        print 'Found ' + str(eos_df['eos_df'].count()) + ' records in EOS stream'

    # - site name                +
    # - dataset name             +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: eos              +

    cols = ['site_name',
            'dataset_name',
            'count(dataset_name) AS nacc',
            'count(distinct(eos_df.user_dn)) AS distinct_users',
            '\"eos\" as stream']


    # Build a query with "cols" columns
    query = ("SELECT %s FROM eos_df " \
             "JOIN f_b_s_df ON f_b_s_df.file_name = eos_df.file_lfn " \
             "GROUP BY site_name, dataset_name") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    result = result.sort(desc("nacc"))

    if verbose:
        print 'Finished EOS part (output is ' + str(result.count()) + ' records)'

    return result


def run_agg_aaa(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting AAA part'

    # Convert date
    date = short_date_string(date)

    # Use enr instead of raw files
    hdir = 'hdfs:///project/monitoring/archive/xrootd/enr/gled'

    # Create AAA tables in sql_context
    aaa_df = aaa_tables(sql_context, hdir=hdir, date=date, verbose=verbose)

    if verbose:
        print 'Found ' + str(aaa_df['aaa_df'].count()) + ' records in AAA stream'

    # - site name                +
    # - dataset name             +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: aaa              +

    cols = ['src_experiment_site AS site_name',
            'dataset_name',
            'count(dataset_name) AS nacc',
            'count(distinct(aaa_df.user_dn)) AS distinct_users',
            '\"aaa\" as stream']


    # Build a query with "cols" columns
    query = ("SELECT %s FROM aaa_df " \
             "JOIN f_b_s_df ON f_b_s_df.file_name = aaa_df.file_lfn " \
             "GROUP BY src_experiment_site, dataset_name") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    result = result.sort(desc("nacc"))

    if verbose:
        print 'Finished AAA part (output is ' + str(result.count()) + ' records)'

    return result


def run_agg_cmssw(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting CMSSW part'

    # Convert date
    date = long_date_string(date)

    # Create CMSSW tables in sql_context
    cmssw_df = cmssw_tables(ctx, sql_context, date=date, verbose=verbose)

    if verbose:
        print 'Found ' + str(cmssw_df['cmssw_df'].count()) + ' records in CMSSW stream'

    # - site name                +
    # - dataset name             +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: cmssw            +

    cols = ['cmssw_df.SITE_NAME AS site_name',
            'dataset_name',
            'count(dataset_name) AS nacc',
            'count(distinct(USER_DN)) AS distinct_users',
            '\"cmssw\" as stream']

    # Build a query with "cols" columns
    query = ("SELECT %s FROM cmssw_df "\
             "JOIN f_b_s_df ON f_b_s_df.file_name = cmssw_df.FILE_LFN "\
             "GROUP BY cmssw_df.SITE_NAME, dataset_name") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    result = result.sort(desc("nacc"))

    if verbose:
        print 'Finished CMSSW part (output is ' + str(result.count()) + ' records)'

    return result


def quiet_logs( sc ):
    print '*** WILL QUIET LOGS ***'
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
    print '*** DID QUIET LOGS ***'


def create_file_block_site_table(ctx, sql_context, verbose=False):
    if verbose:
        print 'Starting file_block_site generation'

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
        print 'Will run query to generate temp file_block_site table'

    result = run_query(query, sql_context, verbose)
    result.registerTempTable('f_b_all_df')

    query_distinct = ("SELECT DISTINCT * FROM f_b_all_df ORDER  BY file_name")
    result_distinct = run_query(query_distinct, sql_context, verbose)
    result_distinct.registerTempTable('f_b_s_df')

    if verbose:
        print 'Temp table from joined DDF, FDF, BDF and PhEDEx'
        print 'After DISTINCT: ' + str(result.count()) + ' -> ' + str(result_distinct.count())
        result_distinct.show(20)
        print_rows(result_distinct, query_distinct, verbose, 5)
        result_distinct.printSchema()
        print 'Finished file_block_site generation'


def clean_site_name(s):
    split = s.split('_')
    split = split[0:3]

    # Remove empty strings which may appear when s is T0_USA_
    split = filter(None, split)

    join = '_'.join(split)
    return join


def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()

    print("Input arguments: %s" % opts)

    start_time = time.time()
    verbose = opts.verbose
    yarn = opts.yarn
    inst = opts.inst
    date = opts.date
    fout = opts.fout

    if  inst.lower() in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)

    # Create spark context
    ctx = spark_context('cms', yarn, verbose)

    quiet_logs(ctx)

    # Create SQL context to be used for SQL queries
    sql_context = HiveContext(ctx)

    # Initialize DBS tables
    dbs_tables(sql_context, inst=inst, verbose=verbose)

    # Initialize PhEDEx table to be used in file_block_site table
    phedex_tables(sql_context, verbose=verbose)

    # Register clean_site_name to be used with SQL queries
    sql_context.udf.register("clean_site_name", clean_site_name)

    # Create temp table with file name, block name, site name and site from PhEDEx
    create_file_block_site_table(ctx, sql_context, verbose)

    cmssw_start_time = time.time()
    aggregated_cmssw_df = run_agg_cmssw(date, fout, ctx, sql_context, verbose)
    cmssw_elapsed_time = elapsed_time(cmssw_start_time)

    aaa_start_time = time.time()
    aggregated_aaa_df = run_agg_aaa(date, fout, ctx, sql_context, verbose)
    aaa_elapsed_time = elapsed_time(aaa_start_time)

    eos_start_time = time.time()
    aggregated_eos_df = run_agg_eos(date, fout, ctx, sql_context, verbose)
    eos_elapsed_time = elapsed_time(eos_start_time)

    jm_start_time = time.time()
    aggregated_jm_df = run_agg_jm(date, fout, ctx, sql_context, verbose)
    jm_elapsed_time = elapsed_time(jm_start_time)

#    all_df = aggregated_cmssw_df.unionAll(aggregated_aaa_df)
#    all_df = all_df.unionAll(aggregated_eos_df)
#    all_df = all_df.sort(desc("nacc"))

    if verbose:
        cmssw_df_size = aggregated_cmssw_df.count()
        aaa_df_size = aggregated_aaa_df.count()
        eos_df_size = aggregated_eos_df.count()
        jm_df_size = aggregated_jm_df.count()

        print "CMSSW:"
        aggregated_cmssw_df.show(20)
        aggregated_cmssw_df.printSchema()

        print "AAA:"
        aggregated_aaa_df.show(20)
        aggregated_aaa_df.printSchema()

        print "EOS:"
        aggregated_eos_df.show(20)
        aggregated_eos_df.printSchema()

        print "JobMonitoring:"
        aggregated_jm_df.show(20)
        aggregated_jm_df.printSchema()


#    print "Aggregated all:"
#    Schema for output is:
#    site name, dataset name, number of accesses, distinct users, stream

#    all_df.show(20)
#    all_df.printSchema()
#    all_df_size = all_df.count()

#    fout = fout + "/Aggregated/" + short_date_string(date)

    output_dataframe(fout + "/Aggregated/CMSSW/" + short_date_string(date), aggregated_cmssw_df, verbose)
    output_dataframe(fout + "/Aggregated/AAA/" + short_date_string(date), aggregated_aaa_df, verbose)
    output_dataframe(fout + "/Aggregated/EOS/" + short_date_string(date), aggregated_eos_df, verbose)
    output_dataframe(fout + "/Aggregated/JobMonitoring/" + short_date_string(date), aggregated_jm_df, verbose)

    ctx.stop()

    if verbose:
        print 'Output record count:'
        print 'Output record count CMSSW         : ' + str(cmssw_df_size)
        print 'Output record count AAA           : ' + str(aaa_df_size)
        print 'Output record count EOS           : ' + str(eos_df_size)
        print 'Output record count JobMonitoring : ' + str(jm_df_size)
#        print 'Output record count Total: ' + str(all_df_size)

    print('Start time         : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(start_time)))
    print('End time           : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Total elapsed time : %s' % elapsed_time(start_time))

    print('AAA elapsed time           : %s' % aaa_elapsed_time)
    print('CMSSW elapsed time         : %s' % cmssw_elapsed_time)
    print('EOS elapsed time           : %s' % eos_elapsed_time)
    print('JobMonitoring elapsed time : %s' % jm_elapsed_time)


if __name__ == '__main__':
    main()
