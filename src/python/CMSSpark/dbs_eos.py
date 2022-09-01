#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : dbs_eos.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : Spark script to parse DBS and EOS records on HDFS.
"""

# system modules
import click
import time

from pyspark import StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit

# CMSSpark modules
from CMSSpark import conf as c
from CMSSpark.spark_utils import dbs_tables, print_rows
from CMSSpark.spark_utils import spark_context, eos_tables, split_dataset
from CMSSpark.utils import info


def eos_date(date):
    """Convert given date into eos date format"""
    if not date:
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))
        return date
    if len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    return '%s/%s/%s' % (year, month, day)


def eos_date_unix(date):
    """Convert EOS date into UNIX timestamp"""
    return time.mktime(time.strptime(date, '%Y/%m/%d'))


def run(date, fout, yarn=None, verbose=None, inst='GLOBAL'):
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
    ddf = tables['ddf']  # dataset table
    fdf = tables['fdf']  # file table

    if verbose:
        for row in ddf.head(1):
            print("### ddf row", row)

    # read CMSSW avro rdd
    date = eos_date(date)
    tables.update(eos_tables(sql_context, date=date, verbose=verbose))
    eos_df = tables['eos_df']  # EOS table

    if verbose:
        for row in eos_df.head(1):
            print("### eos_df row", row)

    # merge DBS and CMSSW data
    cols = ['d_dataset', 'd_dataset_id', 'f_logical_file_name', 'file_lfn']
    stmt = 'SELECT %s FROM ddf JOIN fdf ON ddf.d_dataset_id = fdf.f_dataset_id JOIN eos_df ON fdf.f_logical_file_name = eos_df.file_lfn' % ','.join(cols)
    joins = sql_context.sql(stmt)
    print_rows(joins, stmt, verbose)

    # perform aggregation
    fjoin = joins.groupBy(['d_dataset']) \
        .agg({'file_lfn': 'count'}) \
        .withColumnRenamed('count(file_lfn)', 'count') \
        .withColumnRenamed('d_dataset', 'dataset') \
        .withColumn('date', lit(eos_date_unix(date))) \
        .withColumn('count_type', lit('eos'))

    # keep table around
    fjoin.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        ndf = split_dataset(fjoin, 'dataset')
        ndf.write.format("com.databricks.spark.csv") \
            .option("header", "true").save(fout)

    ctx.stop()


@info
@click.command()
@c.common_options(c.ARG_DATE, c.ARG_YARN, c.ARG_FOUT, c.ARG_VERBOSE)
# Custom options
@click.option("--inst", default="global", help="DBS instance on HDFS: global (default), phys01, phys02, phys03")
def main(date, yarn, fout, verbose, inst):
    """Main function"""
    click.echo('dbs_eos')
    click.echo(f'Input Arguments: date:{date}, yarn:{yarn}, fout:{fout}, verbose:{verbose}, inst:{inst}')
    if inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    run(date, fout, yarn, verbose, inst)


if __name__ == '__main__':
    main()
