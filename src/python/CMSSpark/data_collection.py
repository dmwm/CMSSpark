#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Justinas Rumševičius <justinas.rumsevicius AT gmail [DOT] com>
"""
Spark script to join data from DBS and AAA, CMSSW, EOS, JM streams on HDFS.
"""

import time
import argparse

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, cmssw_tables, aaa_tables, eos_tables, jm_tables
from CMSSpark.spark_utils import spark_context, print_rows, split_dataset
from CMSSpark.spark_utils import delete_hadoop_directory
from CMSSpark.utils import elapsed_time

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


def yesterday():

    # Current time - 24 hours
    return time.gmtime(time.time() - 60 * 60 * 24)


def short_date_string(date):

    # Convert given date into YYYY/MM/DD date format - 2017/07/05
    # Used by EOS and AAA
    # Date is with leading zeros (if needed)

    if  not date:
        # If no date is present, use yesterday as default
        date = time.strftime("%Y/%m/%d", yesterday())
        return date

    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")

    year = date[:4]
    month = date[4:6]
    day = date[6:]
    return '%s/%s/%s' % (year, month, day)


def long_date_string(date):

    # Convert given date into year=YYYY/month=MM/day=DD date format - year=2017/month=7/day=5
    # Used by CMSSW and JobMonitoring (CRAB)
    # Date is without leading zeros

    if  not date:
        date = time.strftime("year=%Y/month=%-m/date=%d", yesterday())
        return date

    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")

    year = date[:4]
    month = int(date[4:6])
    day = int(date[6:])
    return 'year=%s/month=%s/day=%s' % (year, month, day)


def output_dataframe(fout, df, verbose=False):

    # Write out results back to HDFS
    # fout parameter defines area on HDFS
    # It is either absolute path or area under /user/USERNAME

    if df:
        if fout:
            if verbose:
                print 'Output destination: ' + fout

            # Try to remove output directory if it exists
            delete_hadoop_directory(fout)

            # This outputs one record per line in JSON format
            # There is no comma at the end of each line!
            # df.toJSON().saveAsTextFile(fout)

            # This outputs records in CSV format
            df.write.format("com.databricks.spark.csv").option("header", "true").save(fout)
        else:
            print 'No output destination is specified!'
    else:
        print 'No dataframe!'


def run_query(query, sql_context, verbose=False):

    # This function runs query in given sql_context and outputs result to
    # directory specified by fout

    if verbose:
        print 'Will execute SQL Query: ' + query

    # Execute query
    query_result = sql_context.sql(query)

    query_result.persist(StorageLevel.MEMORY_AND_DISK)

    if verbose:
        print 'Executed SQL Query: ' + query + '. Number of rows ' + str(query_result.count())

    return query_result


def run_cmssw(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting CMSSW part'

    # Create fout by adding stream name and date paths
    fout = fout + "/CMSSW/" + short_date_string(date)

    # Convert date
    date = long_date_string(date)

    # Create CMSSW tables in sql_context
    cmssw_tables(ctx, sql_context, date=date, verbose=verbose)

    if verbose:
        print 'Will build query for CMSSW and DBS tables'

    # - file name         +
    # - file size         +
    # - primds            +
    # - procds            +
    # - tier              +
    # - site name         +
    # - file replicas
    # - user dn           +
    # - start/end time    +
    # - read bytes        +
    # - cpu/wc values
    # - source: cmssw     +

    # CMSSW columns
    cmssw_cols = ['FILE_LFN AS file_name',
                  'FILE_SIZE AS file_size',
                  'SITE_NAME AS site_name',
                  'user_dn',
                  'START_TIME AS start_time',
                  'END_TIME as end_time',
                  'READ_BYTES as read_bytes',
                  '"cmssw" AS source']

    # DBS columns
    ddf_cols = ['d_dataset']

    # Concatenate arrays with column names (i.e. use all column names from arrays)
    cols = cmssw_cols + ddf_cols

    # Build a query with "cols" columns. Join DDF, FDF and CMSSW tables
    query = ("SELECT %s FROM ddf "
             "JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id "
             "JOIN cmssw_df ON fdf.f_logical_file_name = cmssw_df.FILE_LFN") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    if verbose:
        print 'Query done. Will split "dataset" column'

    # Split "dataset" column into "primds", "procds" and "tier"
    result = split_dataset(result, 'd_dataset')

    output_dataframe(fout, result, verbose)

    if verbose:
        print 'Finished CMSSW part'


def run_aaa(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting AAA part'

    # Create fout by adding stream name and date paths
    fout = fout + "/AAA/" + short_date_string(date)

    # Convert date
    date = short_date_string(date)

    # Create AAA tables in sql_context
    aaa_tables(sql_context, date=date, verbose=verbose)

    if verbose:
        print 'Will build query for AAA and DBS tables'

    # - file name         +
    # - file size         +
    # - primds            +
    # - procds            +
    # - tier              +
    # - site name
    # - file replicas
    # - user dn           +
    # - start/end time    +
    # - read bytes        +
    # - cpu/wc values
    # - source: xrootd    +

    # AAA columns
    aaa_cols = ['file_lfn AS file_name',
                'file_size',
                'user_dn',
                'start_time',
                'end_time',
                'read_bytes',
                '"xrootd" AS source']

    # DBS columns
    ddf_cols = ['d_dataset']

    # Concatenate arrays with column names (i.e. use all column names from arrays)
    cols = aaa_cols + ddf_cols

    # Build a query with "cols" columns. Join DDF, FDF and AAA tables
    query = ("SELECT %s FROM ddf "
             "JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id "
             "JOIN aaa_df ON fdf.f_logical_file_name = aaa_df.file_lfn") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    # Split "dataset" column into "primds", "procds" and "tier"
    result = split_dataset(result, 'd_dataset')

    output_dataframe(fout, result, verbose)

    if verbose:
        print 'Finished AAA part'


def run_eos(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting EOS part'

    # Create fout by adding stream name and date paths
    fout = fout + "/EOS/" + short_date_string(date)

    # Convert date
    date = short_date_string(date)

    # Create EOS tables in sql_context
    eos_tables(sql_context, date=date, verbose=verbose)

    if verbose:
        print 'Will build query for EOS and DBS tables'

    # - file name       +
    # - file size       +
    # - primds          +
    # - procds          +
    # - tier            +
    # - site name
    # - file replicas
    # - user dn         +
    # - start/end time  +
    # - read bytes
    # - cpu/wc values
    # - source: eos     +

    # EOS columns
    # Same timestamp is used in both start and end times
    eos_cols = ['file_lfn AS file_name',
                'user_dn',
                '"eos" AS source',
                'timestamp AS start_time',
                'timestamp AS end_time']

    # DBS columns
    ddf_cols = ['d_dataset']
    fdf_cols = ['f_file_size AS file_size']

    # Concatenate arrays with column names (i.e. use all column names from arrays)
    cols = eos_cols + ddf_cols + fdf_cols

    # Build a query with "cols" columns. Join DDF, FDF and EOS tables
    query = ("SELECT %s FROM ddf "
             "JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id "
             "JOIN eos_df ON fdf.f_logical_file_name = eos_df.file_lfn") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    # Split "dataset" column into "primds", "procds" and "tier"
    result = split_dataset(result, 'd_dataset')

    output_dataframe(fout, result, verbose)

    if verbose:
        print 'Finished EOS part'


def run_jm(date, fout, ctx, sql_context, verbose=False):

    if verbose:
        print 'Starting JobMonitoring part'

    # Create fout by adding stream name and date paths
    fout = fout + "/CRAB/" + short_date_string(date)

    # Convert date
    date = long_date_string(date)

    # Create JobMonitoring tables in sql_context
    jm_tables(ctx, sql_context, date=date, verbose=verbose)

    if verbose:
        print 'Will build query for JM and DBS tables'

    # - file name       +
    # - file size       +
    # - primds          +
    # - procds          +
    # - tier            +
    # - site name       +
    # - file replicas
    # - user dn
    # - start/end time  +
    # - read bytes
    # - cpu/wc values   +
    # - source: crab    +

    # JobMonitoring (CRAB) columns
    # For cpu value WrapCPU is used. Records also have ExeCPU.
    jm_cols = ['FileName AS file_name',
               'SiteName AS site_name',
               'WrapWC AS wc',
               'WrapCPU AS cpu',
               'StartedRunningTimeStamp AS start_time',
               'FinishedTimeStamp AS end_time',
               '"crab" AS source']

    # DBS columns
    ddf_cols = ['d_dataset']
    fdf_cols = ['f_file_size AS file_size']

    # Concatenate arrays with column names (i.e. use all column names from arrays)
    cols = jm_cols + ddf_cols + fdf_cols

    # Build a query with "cols" columns. Join DDF, FDF and JobMonitoring tables
    query = ("SELECT %s FROM ddf "
             "JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id "
             "JOIN jm_df ON fdf.f_logical_file_name = jm_df.FileName") % ','.join(cols)

    result = run_query(query, sql_context, verbose)

    # Split "dataset" column into "primds", "procds" and "tier"
    result = split_dataset(result, 'd_dataset')

    output_dataframe(fout, result, verbose)

    if verbose:
        print 'Finished JobMonitoring part'


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

    # Initialize DBS tables (will be used with AAA, CMSSW)
    dbs_tables(sql_context, inst=inst, verbose=verbose)

    aaa_start_time = time.time()

    run_aaa(date, fout, ctx, sql_context, verbose)

    aaa_elapsed_time = elapsed_time(aaa_start_time)
    cmssw_start_time = time.time()

    run_cmssw(date, fout, ctx, sql_context, verbose)

    cmssw_elapsed_time = elapsed_time(cmssw_start_time)
    eos_start_time = time.time()

    run_eos(date, fout, ctx, sql_context, verbose)

    eos_elapsed_time = elapsed_time(eos_start_time)
    jm_start_time = time.time()

    run_jm(date, fout, ctx, sql_context, verbose)

    jm_elapsed_time = elapsed_time(jm_start_time)

    ctx.stop()

    print('Start time         : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(start_time)))
    print('End time           : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Total elapsed time : %s' % elapsed_time(start_time))

    print('AAA elapsed time   : %s' % aaa_elapsed_time)
    print('CMSSW elapsed time : %s' % cmssw_elapsed_time)
    print('EOS elapsed time   : %s' % eos_elapsed_time)
    print('JM elapsed time    : %s' % jm_elapsed_time)


if __name__ == '__main__':
    main()
