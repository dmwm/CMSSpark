#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
import os
import re
import sys
import time
import json

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import info
from CMSSpark.conf import OptionParser

def run(fout, yarn=None, verbose=None, patterns=None, antipatterns=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = SQLContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sqlContext, inst=inst, verbose=verbose))
    bdf = tables['bdf']
    fdf = tables['fdf']
    flf = tables['flf']

    # join tables
    cols = ['*'] # to select all fields from table
    cols = ['b_block_id','b_block_name','f_block_id','f_logical_file_name']

    # join tables
    stmt = 'SELECT %s FROM bdf JOIN fdf on bdf.b_block_id = fdf.f_block_id' % ','.join(cols)
    print(stmt)
    joins = sqlContext.sql(stmt)

    # keep table around
    joins.persist(StorageLevel.MEMORY_AND_DISK)

    # construct conditions
    cols = ['b_block_name', 'f_logical_file_name']
    pat = '%00047DB7-9F77-E011-ADC8-00215E21D9A8.root'
#    pat = '%02ACAA1A-9F32-E111-BB31-0002C90B743A.root'
    fjoin = joins.select(cols).where(col('f_logical_file_name').like(pat))

    print_rows(fjoin, stmt, verbose)

    # keep table around
    fjoin.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        fjoin.write.format("com.databricks.spark.csv")\
                .option("header", "true").save(fout)

    ctx.stop()

@info
def main():
    "Main function"
    optmgr  = OptionParser('dbs_lfn')
    optmgr.parser.add_argument("--patterns", action="store",
        dest="patterns", default="", help='Select datasets patterns')
    optmgr.parser.add_argument("--antipatterns", action="store",
        dest="antipatterns", default="", help='Select datasets antipatterns')
    msg = 'DBS instance on HDFS: global (default), phys01, phys02, phys03'
    optmgr.parser.add_argument("--inst", action="store",
        dest="inst", default="global", help=msg)
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    inst = opts.inst
    if  inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    patterns = opts.patterns.split(',') if opts.patterns else []
    antipatterns = opts.antipatterns.split(',') if opts.antipatterns else []
    run(opts.fout, opts.yarn, opts.verbose, patterns, antipatterns, inst)

if __name__ == '__main__':
    main()
