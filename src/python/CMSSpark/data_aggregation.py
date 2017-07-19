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


def run_agg_aaa(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting AAA part'

    # Convert date
    date = short_date_string(date)

    # Create AAA tables in sql_context
    aaa_tables(sql_context, date=date, verbose=verbose)

    # - dataset name             +
    # - site name                +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: cmssw            +

    # AAA columns
    aaa_cols = ['count(d_dataset) AS nacc',
                'count(distinct(aaa_df.user_dn)) AS distinct_users',
                '\"aaa\" as stream']

    # DBS columns
    ddf_cols = ['d_dataset AS dataset_name']

    # AAA has dataset/block id. Join it with DBS to get dataset/block names.
    # PhEDEx has dataset/block name (as well as ids). Join by names to be sure.
    # Use PhEDEx node_name as site name

    # PhEDEx columns
    phedex_cols = ['node_name AS site_name']

    # Concatenate arrays with column names (i.e. use all column names from arrays)
    cols = aaa_cols + ddf_cols + phedex_cols

    # Build a query with "cols" columns
    query = ("SELECT %s FROM ddf "\
             "JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id "\
             "JOIN bdf ON fdf.f_block_id = bdf.b_block_id "\
             "JOIN phedex_df ON (phedex_df.block_name = bdf.b_block_name AND phedex_df.dataset_name = ddf.d_dataset) "\
             "JOIN aaa_df ON fdf.f_logical_file_name = aaa_df.file_lfn "\
             "GROUP BY node_name, d_dataset") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    result = result.sort(desc("nacc"))

    if verbose:
        print 'Query done'

    if verbose:
        print 'Finished AAA part'

    return result


def run_agg_cmssw(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting CMSSW part'

    # Convert date
    date = long_date_string(date)

    # Create CMSSW tables in sql_context
    cmssw_tables(ctx, sql_context, date=date, verbose=verbose)

    # - dataset name             +
    # - site name                +
    # - number of access (nacc)  +
    # - distinct users           +
    # - stream: cmssw            +

    # CMSSW columns
    cmssw_cols = ['SITE_NAME AS site_name',
                  'count(d_dataset) AS nacc',
                  'count(distinct(USER_DN)) AS distinct_users',
                  '\"cmssw\" as stream']

    # DBS columns
    ddf_cols = ['d_dataset AS dataset_name']

    # Concatenate arrays with column names (i.e. use all column names from arrays)
    cols = cmssw_cols + ddf_cols

    # Build a query with "cols" columns
    query = ("SELECT %s FROM ddf "\
             "JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id "\
             "JOIN cmssw_df ON fdf.f_logical_file_name = cmssw_df.FILE_LFN "\
             "GROUP BY SITE_NAME, d_dataset") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    result = result.sort(desc("nacc"))

    if verbose:
        print 'Query done'

    if verbose:
        print 'Finished CMSSW part'

    return result


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

    # Create SQL context to be used for SQL queries
    sql_context = HiveContext(ctx)

    # Initialize DBS tables
    dbs_tables(sql_context, inst=inst, verbose=verbose)

    # Initialize PhEDEx tables (join with AAA, EOS)
    phedex_tables(sql_context, verbose=verbose)

    cmssw_start_time = time.time()
    aggregated_cmssw_df = run_agg_cmssw(date, fout, ctx, sql_context, verbose)
    cmssw_elapsed_time = elapsed_time(cmssw_start_time)

    aaa_start_time = time.time()
    aggregated_aaa_df = run_agg_aaa(date, fout, ctx, sql_context, verbose)
    aaa_elapsed_time = elapsed_time(aaa_start_time)

    all_df = aggregated_cmssw_df.unionAll(aggregated_aaa_df)
    all_df = all_df.sort(desc("nacc"))

    print "CMSSW:"
    aggregated_cmssw_df.show(20)
    aggregated_cmssw_df.printSchema()
    cmssw_df_size = aggregated_cmssw_df.count()

    print "AAA:"
    aggregated_aaa_df.show(20)
    aggregated_aaa_df.printSchema()
    aaa_df_size = aggregated_aaa_df.count()

    print "Aggregated all:"

    all_df.show(20)
    all_df.printSchema()
    all_df_size = all_df.count()

    fout = fout + "/Aggregated/" + short_date_string(date)

    output_dataframe(fout, all_df, verbose)

    ctx.stop()

    print "Record count: CMSSW: " + str(cmssw_df_size) + " AAA: " + str(aaa_df_size) + " Total: " + str(all_df_size)

    print('Start time         : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(start_time)))
    print('End time           : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Total elapsed time : %s' % elapsed_time(start_time))

    print('AAA elapsed time   : %s' % aaa_elapsed_time)
    print('CMSSW elapsed time : %s' % cmssw_elapsed_time)
    # print('EOS elapsed time   : %s' % eos_elapsed_time)
    # print('JM elapsed time    : %s' % jm_elapsed_time)


if __name__ == '__main__':
    main()
