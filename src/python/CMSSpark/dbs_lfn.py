#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : dbs_lfn.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
import click
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

# CMSSpark modules
from CMSSpark import conf as c
from CMSSpark.spark_utils import dbs_tables, print_rows, spark_context
from CMSSpark.utils import info


def run(fout, yarn=None, verbose=None, patterns=None, antipatterns=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sql_context = SQLContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sql_context, inst=inst, verbose=verbose))
    bdf = tables['bdf']
    fdf = tables['fdf']
    flf = tables['flf']

    # join tables
    cols = ['*']  # to select all fields from table
    cols = ['b_block_id', 'b_block_name', 'f_block_id', 'f_logical_file_name']

    # join tables
    stmt = 'SELECT %s FROM bdf JOIN fdf on bdf.b_block_id = fdf.f_block_id' % ','.join(cols)
    print(stmt)
    joins = sql_context.sql(stmt)

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
    if fout:
        fjoin.write.format("com.databricks.spark.csv") \
            .option("header", "true").save(fout)

    ctx.stop()


@info
@click.command()
@c.common_options(c.ARG_YARN, c.ARG_FOUT, c.ARG_VERBOSE)
# Custom options
@click.option("--patterns", default="", help="Select datasets patterns")
@click.option("--antipatterns", default="", help="Select datasets antipatterns")
@click.option("--inst", default="global", help="DBS instance on HDFS: global (default), phys01, phys02, phys03")
def main(yarn, fout, verbose, patterns, antipatterns, inst):
    """Main function"""
    click.echo('dbs_lfn')
    click.echo(f'Input Arguments: yarn:{yarn}, fout:{fout}, verbose:{verbose}, '
               f'patterns:{patterns}, antipatterns:{antipatterns}, inst:{inst}')
    if inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    patterns = patterns.split(',') if patterns else []
    antipatterns = antipatterns.split(',') if antipatterns else []
    run(fout, yarn, verbose, patterns, antipatterns, inst)


if __name__ == '__main__':
    main()
