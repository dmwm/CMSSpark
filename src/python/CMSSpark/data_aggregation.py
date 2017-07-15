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
from CMSSpark.spark_utils import dbs_tables, cmssw_tables, aaa_tables, eos_tables, jm_tables
from CMSSpark.spark_utils import spark_context, print_rows, split_dataset
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
        print 'SQL Query: ' + query

    # Execute query
    query_result = sql_context.sql(query)

    query_result.persist(StorageLevel.MEMORY_AND_DISK)

    # If verbose is enabled, print first three rows (for debug reasons)
    print_rows(query_result, query, verbose, 3)

    return query_result


def run_agg_cmssw(date, fout, ctx, sql_context, verbose=False):

    # Create fout by adding stream name and date paths
    fout = fout + "/AGGREGATED/" + short_date_string(date)

    # Convert date
    date = long_date_string(date)

    # Create CMSSW tables in sql_context
    cmssw_tables(ctx, sql_context, date=date, verbose=verbose)

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

    result.show()

    # output_dataframe(fout, result, verbose)

    if verbose:
        print 'Finished CMSSW part'


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

    run_agg_cmssw(date, fout, ctx, sql_context, verbose)

    ctx.stop()

    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(start_time)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(start_time))


if __name__ == '__main__':
    main()
