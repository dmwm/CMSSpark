#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : dbs_events.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
import click
from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark import conf as c
from CMSSpark.spark_utils import dbs_tables, spark_context
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
    tables.update(dbs_tables(sql_context, inst=inst, verbose=verbose, tables=['ddf', 'bdf', 'fdf']))
    ddf = tables['ddf']
    bdf = tables['bdf']
    fdf = tables['fdf']

    # join tables
    cols = ['d_dataset', 'd_dataset_id', 'd_creation_date', 'b_block_id', 'b_file_count', 'f_block_id', 'f_file_id',
            'f_dataset_id', 'f_event_count', 'f_file_size', 'f_is_file_valid']

    # join tables and select files
    # ideally this would select only valid files in valid data sets. However currently it appears that all data sets are valid
    stmt = 'SELECT %s FROM ddf JOIN bdf on ddf.d_dataset_id = bdf.b_dataset_id JOIN fdf on bdf.b_block_id=fdf.f_block_id' % ','.join(cols)

    print(stmt)
    joins = sql_context.sql(stmt)

    # keep table around
    joins.persist(StorageLevel.MEMORY_AND_DISK)

    # construct aggregation
    fjoin = joins \
        .groupBy(['d_dataset', 'd_creation_date']) \
        .agg({'f_file_id': 'count', 'f_event_count': 'sum', 'f_file_size': 'sum', 'f_is_file_valid': 'sum'}) \
        .withColumnRenamed('d_dataset', 'dataset') \
        .withColumnRenamed('count(f_file_id)', 'nfiles') \
        .withColumnRenamed('sum(f_event_count)', 'nevents') \
        .withColumnRenamed('sum(f_file_size)', 'size') \
        .withColumnRenamed('d_creation_date', 'creation_date') \
        .withColumnRenamed('sum(f_is_file_valid)', 'nfiles_valid')

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
    click.echo('dbs_events')
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
