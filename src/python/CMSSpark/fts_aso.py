#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : fts_aso.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : Spark script to parse FTS records on HDFS.
"""

# system modules
import click
import time

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.functions import sum as agg_sum

# CMSSpark modules
from CMSSpark import conf as c
from CMSSpark.spark_utils import fts_tables, spark_context
from CMSSpark.utils import info


def fts_date(date):
    """Convert given date into FTS date format"""
    if not date:
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))
        return date
    if len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format")
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    return '%s/%s/%s' % (year, month, day)


def fts_date_unix(date):
    """Convert FTS date into UNIX timestamp"""
    return time.mktime(time.strptime(date, '%Y/%m/%d'))


def run(date, fout, yarn=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sql_context = SQLContext(ctx)

    # read FTS tables
    date = fts_date(date)
    tables = {}
    tables.update(fts_tables(sql_context, date=date, verbose=verbose))
    fts_df = tables['fts_df']  # fts table

    # example to extract transfer records for ASO
    # VK: the commented lines show how to extract some info from fts_df via SQL
    #    cols = ['data.job_metadata.issuer', 'data.f_size']
    #    stmt = 'SELECT %s FROM fts_df' % ','.join(cols)
    #    joins = sql_context.sql(stmt)
    #    fjoin = joins.groupBy(['issuer'])\
    #            .agg({'f_size':'sum'})\
    #            .withColumnRenamed('sum(f_size)', 'sum_file_size')\
    #            .withColumnRenamed('issuer', 'issuer')

    # we can use fts_df directly for groupby/aggregated tasks
    fjoin = fts_df.groupBy(['job_metadata.issuer']) \
        .agg(agg_sum(fts_df.f_size).alias("sum_f_size"))

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
@c.common_options(c.ARG_DATE, c.ARG_YARN, c.ARG_FOUT, c.ARG_VERBOSE)
def main(date, yarn, fout, verbose):
    """Main function"""
    click.echo('fts_aso')
    click.echo(f'Input Arguments: date:{date}, yarn:{yarn}, fout:{fout}, verbose:{verbose}')
    run(date, fout, yarn, verbose)


if __name__ == '__main__':
    main()
