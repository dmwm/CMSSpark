#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Justinas Rumševičius <justinas.rumsevicius AT gmail [DOT] com>
"""
Spark script to join data from DBS and PhEDEx streams on HDFS.
"""

import time
import argparse

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, cmssw_tables
from CMSSpark.spark_utils import spark_context, print_rows, split_dataset
from CMSSpark.utils import elapsed_time

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process DBS+CMSSW metadata"

        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)

        self.parser.add_argument("--inst", action="store",
            dest="inst", default="global", help='DBS instance on HDFS: global (default), phys01, phys02, phys03')
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select CMSSW data for specific date (YYYYMMDD)')
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help='Output file name')

def cmssw_date(date):
    # Convert given date into CMSSW date format
    if  not date:
        date = time.strftime("year=%Y/month=%-m/date=%d", time.gmtime(time.time()-60*60*24))
        return date
    if  len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:])

    return 'year=%s/month=%s/day=%s' % (year, month, day)

def run_cmssw(date=None, fout=None, yarn=None, verbose=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    tables = {}

    # Update tables dictionary by loading and adding DBS and CMSSW tables
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    tables.update(cmssw_tables(ctx, sqlContext, date=cmssw_date(date), verbose=verbose))

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
                  '"cmssw" as source']

    # DBS columns
    ddf_cols = ['d_dataset']

    # Concatenate arrays with column names (i.e. use all column names from arrays)
    cols = cmssw_cols + ddf_cols

    # Build a query with "cols" columns. Join DDF, FDF and CMSSW tables
    query = ("SELECT %s FROM ddf "
             "JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id "
             "JOIN cmssw_df ON fdf.f_logical_file_name = cmssw_df.FILE_LFN") % ','.join(cols)

    if verbose:
        print 'SQL Query: ' + query

    # Execute query
    cmssw_result = sqlContext.sql(query)

    if verbose:
        print 'Query done. Will split "dataset" column'

    # Split "dataset" column into "primds", "procds" and "tier"
    cmssw_result = split_dataset(cmssw_result, 'd_dataset')

    if verbose:
        print 'Will output data'

    # If verbose is enabled, print first three rows (for debug reasons)
    print_rows(cmssw_result, query, verbose, 3)

    # Write out results back to HDFS, the fout parameter defines area on HDFS
    # It is either absolute path or area under /user/USERNAME
    if  fout:
        fout = fout + "/CMSSW"

        if verbose:
            print 'Output destination: ' + fout

        # This outputs one record per line
        # There is no comma at the end of each line!
        cmssw_result.toJSON().saveAsTextFile(fout)
    else:
        print 'No output destination is specified!'

    ctx.stop()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    print("Input arguments: %s" % opts)

    start_time = time.time()
    verbose = opts.verbose
    yarn = opts.yarn
    inst = opts.inst
    date = opts.date
    fout = opts.fout

    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)

    run_cmssw(date, fout, yarn, verbose, inst)

    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(start_time)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(start_time))

if __name__ == '__main__':
    main()
