#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : hdfs_an.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : HDFS data anonymization
"""

# system modules
import hashlib
import unicodedata

from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType

# CMSSpark options
from CMSSpark.spark_utils import spark_context
from CMSSpark.conf import OptionParser


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


def main():
    optmgr = OptionParser('hdfs_app')
    msg = 'HDFS path to process'
    msg = 'Comma separated list of attributes to anonimise'
    optmgr.parser.add_argument("--attrs", action="store",
                               dest="attrs", default="", help=msg)
    optmgr.parser.add_argument("--nparts", action="store",
                               dest="nparts", default=100, help=msg)
    opts = optmgr.parser.parse_args()
    attrs = opts.attrs.split(',')
    run(opts.hdir, attrs, opts.yarn, opts.fout, opts.verbose, opts.nparts)


if __name__ == '__main__':
    main()
