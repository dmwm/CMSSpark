#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : cmssw_avro2json.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : convert avro file to json on HDFS. Here is usage example
Usage       : run_spark cmssw_avro2json.py --yarn --date "20200102" --verbose --fout /cms/tmp/cmssw/
"""

# system modules
import time
import click

from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark import conf as c
from CMSSpark.spark_utils import cmssw_tables, spark_context


def transform(rec):
    xdf = {}
    for key, val in rec.items():
        xdf[key.lower()] = val
    tstamp = int(time.time()) * 1000
    mid = "43b78d48-7509-e1cc-dc2c-a071d2cfdd7c"
    metadata = {"_id": "43b78d48-7509-e1cc-dc2c-a071d2cfdd7c",
                "hostname": "monit.cern.ch",
                "kafka_timestamp": tstamp,
                "partition": "1",
                "producer": "convert_avro2json",
                "timestamp": tstamp,
                "topic": "cmssw_pop_raw_metric",
                "type": "metric",
                "type_prefix": "raw",
                "version": "001"}
    return {"data": xdf, "metadata": metadata}


def convert(ctx, sql_context, date, fin, fout, verbose):
    """Helper function to convert fin (avro) to fout (json)"""
    if fin:
        pdf = cmssw_tables(ctx, sql_context, hdir=fin, date=date)
    else:
        pdf = cmssw_tables(ctx, sql_context, date=date)
    xdf = pdf['cmssw_df']
    if verbose:
        records = xdf.take(1)
        print("original record", records)
    new_rdd = xdf.rdd.map(lambda row: row.asDict(True))
    new_rdd = new_rdd.map(lambda row: transform(row))
    if verbose:
        records = new_rdd.take(1)
        print("converted record", records)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        xdf = sql_context.createDataFrame(new_rdd)
        xdf.write.format("json").save(fout)


@click.command()
@c.common_options(c.ARG_FIN, c.ARG_DATE, c.ARG_YARN, c.ARG_FOUT, c.ARG_VERBOSE)
def main(fin, date, yarn, fout, verbose):
    """Main function"""
    click.echo('cmssw_avro2json')
    click.echo(f'Input Arguments: fin:{fin}, date:{date}, yarn:{yarn}, fout:{fout}, verbose:{verbose}')
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sql_context = SQLContext(ctx)
    convert(ctx, sql_context, date, fin, fout, verbose)


if __name__ == '__main__':
    main()
