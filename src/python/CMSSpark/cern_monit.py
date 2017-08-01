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
import argparse
from subprocess import Popen, PIPE

# pyspark modules
from pyspark import SparkContext, SparkFiles
from pyspark.sql import HiveContext

# CMSSpark modules
from CMSSpark.spark_utils import spark_context, print_rows, unionAll
from CMSSpark.utils import elapsed_time
from CMSSpark.schemas import aggregated_data_schema

def send2monit(data):
    """
    Helper function which wraps StompAMQ and incoming dataframe into
    notification message. Then it sends it to AMQ end-point provided
    by credentials file.
    """
    import os
    import stomp
    import time
    import uuid
    import logging
    class StompyListener(object):
        """
        Auxiliar listener class to fetch all possible states in the Stomp
        connection.
        """
        def __init__(self):
            self.logr = logging.getLogger(__name__)

        def on_connecting(self, host_and_port):
            self.logr.info('on_connecting %s', str(host_and_port))

        def on_error(self, headers, message):
            self.logr.info('received an error %s %s', str(headers), str(message))

        def on_message(self, headers, body):
            self.logr.info('on_message %s %s', str(headers), str(body))

        def on_heartbeat(self):
            self.logr.info('on_heartbeat')

        def on_send(self, frame):
            self.logr.info('on_send HEADERS: %s, BODY: %s ...', str(frame.headers), str(frame.body)[:160])

        def on_connected(self, headers, body):
            self.logr.info('on_connected %s %s', str(headers), str(body))

        def on_disconnected(self):
            self.logr.info('on_disconnected')

        def on_heartbeat_timeout(self):
            self.logr.info('on_heartbeat_timeout')

        def on_before_message(self, headers, body):
            self.logr.info('on_before_message %s %s', str(headers), str(body))

            return (headers, body)


    class StompAMQ(object):
        """
        Class to generate and send notifications to a given Stomp broker
        and a given topic.

        :param username: The username to connect to the broker.
        :param password: The password to connect to the broker.
        :param producer: The 'producer' field in the notification header
        :param topic: The topic to be used on the broker
        :param host_and_ports: The hosts and ports list of the brokers.
            E.g.: [('agileinf-mb.cern.ch', 61213)]
        """

        # Version number to be added in header
        _version = '0.1'

        def __init__(self, username, password,
                     producer='CMS_WMCore_StompAMQ',
                     topic='/topic/cms.jobmon.wmagent',
                     host_and_ports=None, verbose=0):
            self._host_and_ports = host_and_ports or [('agileinf-mb.cern.ch', 61213)]
            self._username = username
            self._password = password
            self._producer = producer
            self._topic = topic
            self.verbose = verbose

        def send(self, data):
            """
            Connect to the stomp host and send a single notification
            (or a list of notifications).

            :param data: Either a single notification (as returned by
                `make_notification`) or a list of such.

            :return: a list of successfully sent notification bodies
            """

            conn = stomp.Connection(host_and_ports=self._host_and_ports)
            conn.set_listener('StompyListener', StompyListener())
            try:
                conn.start()
                conn.connect(username=self._username, passcode=self._password, wait=True)
            except stomp.exception.ConnectFailedException as exc:
                print("ERROR: Connection to %s failed %s" % (repr(self._host_and_ports), str(exc)))
                return []

            # If only a single notification, put it in a list
            if isinstance(data, dict) and 'topic' in data:
                data = [data]

            successfully_sent = []
            for notification in data:
                body = self._send_single(conn, notification)
                if body:
                    successfully_sent.append(body)

            if conn.is_connected():
                conn.disconnect()

            print('Sent %d docs to %s' % (len(successfully_sent), repr(self._host_and_ports)))
            return successfully_sent

        def _send_single(self, conn, notification):
            """
            Send a single notification to `conn`

            :param conn: An already connected stomp.Connection
            :param notification: A dictionary as returned by `make_notification`

            :return: The notification body in case of success, or else None
            """
            try:
                body = notification.pop('body')
                destination = notification.pop('topic')
                conn.send(destination=destination,
                          headers=notification,
                          body=json.dumps(body),
                          ack='auto')
                if  self.verbose:
                    print('Notification %s sent' % str(notification))
                return body
            except Exception as exc:
                print('ERROR: Notification: %s not send, error: %s' % \
                              (str(notification), str(exc)))
                return None


        def make_notification(self, payload, id_, producer=None):
            """
            Generate a notification with the specified data

            :param payload: Actual notification data.
            :param id_: Id representing the notification.
            :param producer: The notification producer.
                Default: StompAMQ._producer

            :return: the generated notification
            """
            producer = producer or self._producer

            notification = {}
            notification['topic'] = self._topic

            # Add headers
            headers = {
                       'type': 'cms_wmagent_info',
                       'version': self._version,
                       'producer': producer
            }

            notification.update(headers)

            # Add body consisting of the payload and metadata
            body = {
                'payload': payload,
                'metadata': {
                    'timestamp': int(time.time()),
                    'id': id_,
                    'uuid': str(uuid.uuid1()),
                }
            }
            notification['body'] = body
            return notification

    # main function logic
    with open(SparkFiles.get('amq_broker.json')) as istream:
        creds = json.load(istream)
        host, port = creds['host_and_ports'].split(':')
        port = int(port)
        amq = StompAMQ(creds['username'], creds['password'], \
            creds['producer'], creds['topic'], [(host, port)])
        arr = []
        for idx, row in enumerate(data):
            if  not idx:
                print("### row", row, type(row))
            doc = json.loads(row)
            hid = doc.get("hash", 1)
            arr.append(amq.make_notification(doc, hid))
        amq.send(arr)
        print("### Send %s docs to CERN MONIT" % len(arr))

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to send data from HDFS area to CERN MONIT"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        msg = 'Location of data on HDFS in CSV data-format'
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default='', help=msg)
        msg = 'Full path to stomp python module egg'
        self.parser.add_argument("--stomp", action="store",
            dest="stomp", default='', help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")
        msg = "AMQ credentials JSON file (should be named as amq_broker.json)"
        self.parser.add_argument("--amq", action="store",
            dest="amq", default="amq_broker.json", help=msg)

def run(path, amq, stomp, yarn=None, verbose=False):
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
    sqlContext = HiveContext(ctx)

    hpath = "hadoop fs -ls %s | awk '{print $8}'" % path
    if  verbose:
        print("### Read files: %s" % hpath)
    pipe = Popen(hpath, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    pipe.wait()
    pfiles = [f for f in pipe.stdout.read().split('\n') if f.find('part-') != -1]
    df = unionAll([sqlContext.read.format('com.databricks.spark.csv')\
                    .options(treatEmptyValuesAsNulls='true', nullValue='null', header='true')\
                    .load(fname) for fname in pfiles])

    # Register temporary tables to be able to use sqlContext.sql
    df.registerTempTable('df')
    print_rows(df, "DataFrame", verbose)

    print 'Schema:'
    df.printSchema()

    # send data to CERN MONIT via stomp AMQ, see send2monit function
    df.toJSON().foreachPartition(send2monit)

    ctx.stop()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    run(opts.hdir, opts.amq, opts.stomp, opts.yarn, opts.verbose)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
