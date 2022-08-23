#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File       : dbs_block_lumis.py
Author     : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description: Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, spark_context
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
    # cols = ['*']  # to select all fields from table
    cols = ['b_block_id', 'b_block_name', 'f_block_id', 'f_file_id', 'fl_file_id', 'fl_lumi_section_num']

    # join tables
    stmt = 'SELECT %s FROM bdf JOIN fdf on bdf.b_block_id = fdf.f_block_id JOIN flf on fdf.f_file_id=flf.fl_file_id' % ','.join(cols)
    print(stmt)
    joins = sqlContext.sql(stmt)

    # keep table around
    joins.persist(StorageLevel.MEMORY_AND_DISK)

    # construct conditions
    #    adler = ['ad8f6ad2','9c441343','f68d5dca','81c90e2a','471d2524','a3c1f077','6f0018a0','8bb03b60','d504882c','5ede357f','b05303c3','716d1776','7e9cf258','1945804b','ec7bc1d7','12c87747','94f2aa32']
    #    cond = 'f_adler32 in %s' % adler
    #    cond = cond.replace('[', '(').replace(']', ')')
    #    fjoin = joins.where(cond).distinct().select(cols)

    #    print_rows(fjoin, stmt, verbose)
    fjoin = joins \
        .groupBy(['b_block_name']) \
        .agg({'fl_lumi_section_num': 'count'}) \
        .withColumnRenamed('count(fl_lumi_section_num)', 'nlumis') \
 \
        # keep table around
    fjoin.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        fjoin.write.format("com.databricks.spark.csv") \
            .option("header", "true").save(fout)

    ctx.stop()


@info
def main():
    """Main function"""
    optmgr = OptionParser('dbs_block_lumis')
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
    if inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    patterns = opts.patterns.split(',') if opts.patterns else []
    antipatterns = opts.antipatterns.split(',') if opts.antipatterns else []
    run(opts.fout, opts.yarn, opts.verbose, patterns, antipatterns, inst)


if __name__ == '__main__':
    main()
