#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : cern_monit.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : Spark script to read data from HDFS location and send them to CERN MONIT system.
"""

# system modules
import click
import json
import os
import time

from pyspark import SparkFiles
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark import conf as c
from CMSSpark.schemas import aggregated_data_schema
from CMSSpark.spark_utils import spark_context, print_rows, union_all
from CMSSpark.utils import info

# CMSMonitoring modules
try:
    from CMSMonitoring.StompAMQ7 import StompAMQ7 as StompAMQ
except ImportError:
    StompAMQ = False


def print_data(data):
    """Helper function for testing purposes"""
    for row in data:
        print(row)


def send2monit(data):
    """
    Helper function which wraps StompAMQ and incoming dataframe into
    notification message. Then it sends it to AMQ end-point provided
    by credentials file.
    """
    if not StompAMQ:
        return
    # main function logic
    with open(SparkFiles.get('amq_broker.json')) as istream:
        creds = json.load(istream)
        host, port = creds['host_and_ports'].split(':')
        port = int(port)
        amq = StompAMQ(username=creds['username'], password=creds['password'],
                       producer=creds['producer'], topic=creds['topic'],
                       validation_schema=None,
                       host_and_ports=[(host, port)])
        arr = []
        for idx, row in enumerate(data):
            #            if  not idx:
            #                print("### row", row, type(row))
            doc = json.loads(row)
            doc["rec_tsmp"] = int(time.time()) * 1000
            hid = doc.get("hash", 1)
            arr.append(amq.make_notification(doc, hid))
        amq.send(arr)
        print("### Send %s docs to CERN MONIT" % len(arr))


def run(path, amq, stomp, yarn=None, aggregation_schema=False, verbose=False):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    if stomp and os.path.isfile(stomp):
        ctx.addPyFile(stomp)
    else:
        raise Exception('No stomp module egg is provided')
    if amq and os.path.isfile(amq):
        if amq.split('/')[-1] == 'amq_broker.json':
            ctx.addFile(amq)
        else:
            raise Exception('Wrong AMQ broker file name, please name it as amq_broker.json')
    else:
        raise Exception('No AMQ credential file is provided')
    sql_context = SQLContext(ctx)

    awk = "awk '{print $8}'"
    hpath = f"hadoop fs -ls {path} | {awk}"
    if verbose:
        print("### Read files: %s" % hpath)
    stream = os.popen(hpath)
    pfiles = [f for f in stream.read().splitlines() if f.find('part-') != -1]

    if aggregation_schema:
        df = union_all([sql_context.read.format('com.databricks.spark.csv')
                       .options(treatEmptyValuesAsNulls='true', nullValue='null', header='true')
                       .load(fname, schema=aggregated_data_schema()) for fname in pfiles])
    else:
        df = union_all([sql_context.read.format('com.databricks.spark.csv')
                       .options(treatEmptyValuesAsNulls='true', nullValue='null', header='true')
                       .load(fname) for fname in pfiles])

    # Register temporary tables to be able to use sqlContext.sql
    df.registerTempTable('df')
    print_rows(df, "DataFrame", verbose)

    print('Schema:')
    df.printSchema()

    # for testing uncomment line below
    # df.toJSON().foreachPartition(print_data)
    # send data to CERN MONIT via stomp AMQ, see send2monit function
    df.toJSON().foreachPartition(send2monit)

    ctx.stop()


@info
@click.command()
@c.common_options(c.ARG_HDIR, c.ARG_YARN, c.ARG_VERBOSE)
# Custom options
@click.option("--stomp", default="", help="Full path to stomp python module egg")
@click.option("--amq", default="amq_broker.json", help="AMQ credentials JSON file (should be named as amq_broker.json")
@click.option("--aggregation_schema", default=False, is_flag=True,
              help="use aggregation schema for data upload (needed for correct var types)")
def main(hdir, yarn, verbose, stomp, amq, aggregation_schema):
    """Main function"""
    click.echo('cern_monit')
    click.echo(f'Input Arguments: hdir:{hdir}, yarn:{yarn}, verbose:{verbose}, '
               f'stomp:{stomp}, amq:{amq}, aggregation_schema:{aggregation_schema}')
    run(hdir, amq, stomp, yarn, aggregation_schema, verbose)


if __name__ == '__main__':
    main()
