#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : hdfs_an.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : HDFS data anonymization
"""

# system modules
import click
import hashlib
import unicodedata

from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType

# CMSSpark options
from CMSSpark import conf as c
from CMSSpark.spark_utils import spark_context


def hashfunc(rec):
    """Generic hash function for given record"""
    keyhash = hashlib.md5()
    try:
        keyhash.update(rec)
    except TypeError:  # python3
        keyhash.update(rec.encode('ascii'))
    return keyhash.hexdigest()


def hash_private_info(message):
    """hash function for given message"""
    if message is None:
        return
    elif isinstance(message, str):
        message = unicodedata.normalize('NFKD', message).encode('ASCII', 'ignore')
    elif not isinstance(message, str):
        print("### message", message, type(message))
        return
    return hashfunc(message)


def run(fin, attrs, yarn, fout, verbose, nparts=3000):
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sql_context = SQLContext(ctx)

    # Reading all the files in a directory
    paths = [fin]
    res = sql_context.read.json(paths)

    data = res.select("data.*")
    data.repartition(nparts)
    print("### number of new data paritions", data.rdd.getNumPartitions())

    anonymize = udf(hash_private_info, returnType=StringType())

    # Use the above udf to anonymize data
    for attr in attrs:
        col = attr + '_hash'
        data = data.withColumn(col, anonymize(getattr(data, attr)))

    # drop attributes
    data = data.drop(*attrs)

    # Save to csv
    data.write.option("compression", "gzip").json(fout)


@click.command()
@c.common_options(c.ARG_HDIR, c.ARG_DATE, c.ARG_YARN, c.ARG_FOUT, c.ARG_VERBOSE)
# Custom options
@click.option("--attrs", default="", help="Comma separated list of attributes to anonimise")
@click.option("--nparts", default=100, help="Comma separated list of attributes to anonimise")
def main(hdir, date, yarn, fout, verbose, attrs, nparts):
    """Main function"""
    click.echo('hdfs_app')
    click.echo(f'Input Arguments: hdir:{hdir}, date:{date}, yarn:{yarn}, fout:{fout}, verbose:{verbose}, '
               f'attrs:{attrs}, nparts:{nparts}')
    attrs = attrs.split(',')
    run(hdir, attrs, yarn, fout, verbose, nparts)


if __name__ == '__main__':
    main()
