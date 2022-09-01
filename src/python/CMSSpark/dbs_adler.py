#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File       : dbs_adler.py
Author     : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description: Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
import click
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark import conf as c
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context
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
    tables.update(phedex_tables(sql_context, verbose=verbose))
    phedex_df = tables['phedex_df']
    ddf = tables['ddf']
    fdf = tables['fdf']

    print("### ddf from main", ddf)

    # join tables
    # cols = ['*']  # to select all fields from table
    cols = ['d_dataset_id', 'd_dataset', 'f_logical_file_name', 'f_adler32']

    # join tables
    stmt = 'SELECT %s FROM ddf JOIN fdf on ddf.d_dataset_id = fdf.f_dataset_id' % ','.join(cols)
    print(stmt)
    joins = sql_context.sql(stmt)

    # construct conditions
    adler = ['ad8f6ad2', '9c441343', 'f68d5dca', '81c90e2a', '471d2524', 'a3c1f077', '6f0018a0', '8bb03b60', 'd504882c',
             '5ede357f', 'b05303c3', '716d1776', '7e9cf258', '1945804b', 'ec7bc1d7', '12c87747', '94f2aa32']
    cond = 'f_adler32 in %s' % adler
    cond = cond.replace('[', '(').replace(']', ')')
    #    scols = ['f_logical_file_name']
    fjoin = joins.where(cond).distinct().select(cols)

    print_rows(fjoin, stmt, verbose)

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
    click.echo('dbs_adler')
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
