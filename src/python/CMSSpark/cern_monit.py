#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark script to read data from HDFS location and send them to CERN MONIT system.
"""

# system modules
import os
import re
import sys
import time
import json
from subprocess import Popen, PIPE

# pyspark modules
from pyspark import SparkContext, SparkFiles
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark.spark_utils import spark_context, print_rows, unionAll
from CMSSpark.utils import info
from CMSSpark.conf import OptionParser
from CMSSpark.schemas import aggregated_data_schema

# CMSMonitoring modules
try:
    from CMSMonitoring.StompAMQ import StompAMQ
except ImportError:
    StompAMQ = False

def print_data(data):
    "Helper function for testing purposes"
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
        amq = StompAMQ(creds['username'], creds['password'], \
            creds['producer'], creds['topic'], \
            validation_schema=None, \
            host_and_ports=[(host, port)])
        arr = []
        for idx, row in enumerate(data):
#            if  not idx:
#                print("### row", row, type(row))
            doc = json.loads(row)
            doc["rec_tsmp"] = int(time.time())*1000
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
    if  stomp and os.path.isfile(stomp):
        ctx.addPyFile(stomp)
    else:
        raise Exception('No stomp module egg is provided')
    if  amq and os.path.isfile(amq):
        if  amq.split('/')[-1] == 'amq_broker.json':
            ctx.addFile(amq)
        else:
            raise Exception('Wrong AMQ broker file name, please name it as amq_broker.json')
    else:
        raise Exception('No AMQ credential file is provided')
    sqlContext = SQLContext(ctx)

    hpath = "hadoop fs -ls %s | awk '{print $8}'" % path
    if  verbose:
        print("### Read files: %s" % hpath)
    pipe = Popen(hpath, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    pipe.wait()
    pfiles = [f for f in pipe.stdout.read().split('\n') if f.find('part-') != -1]
    df = []

    if aggregation_schema:
        df = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                    .options(treatEmptyValuesAsNulls='true', nullValue='null', header='true') \
                    .load(fname, schema=aggregated_data_schema()) for fname in pfiles])
    else:
        df = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                    .options(treatEmptyValuesAsNulls='true', nullValue='null', header='true') \
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
def main():
    "Main function"
    optmgr = OptionParser('cern_monit')
    msg = 'Full path to stomp python module egg'
    optmgr.parser.add_argument("--stomp", action="store",
        dest="stomp", default='', help=msg)
    msg = "AMQ credentials JSON file (should be named as amq_broker.json)"
    optmgr.parser.add_argument("--amq", action="store",
        dest="amq", default="amq_broker.json", help=msg)
    optmgr.parser.add_argument("--aggregation_schema", action="store_true",
            dest="aggregation_schema", default=False, help="use aggregation schema for data upload (needed for correct var types)")
    opts = optmgr.parser.parse_args()
    run(opts.hdir, opts.amq, opts.stomp, opts.yarn, opts.aggregation_schema, opts.verbose)

if __name__ == '__main__':
    main()
