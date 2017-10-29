#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
import os
import re
import sys
import time
import json
import argparse
import datetime
import calendar
from types import NoneType

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext

# CMSSpark modules
from CMSSpark.spark_utils import avro_rdd, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import elapsed_time

class OptionParser():
    def __init__(self):
        "User based option parser"
        desc = "Spark script to process DBS+PhEDEx metadata"
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///cms/wmarchive/avro'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = ''
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        msg = 'Date timestamp (YYYYMMDD) or range YYYYMMDD-YYYYMMDD'
        self.parser.add_argument("--date", action="store",
            dest="date", default='', help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

# global patterns
PAT_YYYYMMDD = re.compile(r'^20[0-9][0-9][0-1][0-9][0-3][0-9]$')
PAT_YYYY = re.compile(r'^20[0-9][0-9]$')
PAT_MM = re.compile(r'^(0[1-9]|1[012])$')
PAT_DD = re.compile(r'^(0[1-9]|[12][0-9]|3[01])$')

def dateformat(value):
    """Return seconds since epoch for provided YYYYMMDD or number with suffix 'd' for days"""
    msg  = 'Unacceptable date format, value=%s, type=%s,' \
            % (value, type(value))
    msg += " supported format is YYYYMMDD or number with suffix 'd' for days"
    value = str(value).lower()
    if  PAT_YYYYMMDD.match(value): # we accept YYYYMMDD
        if  len(value) == 8: # YYYYMMDD
            year = value[0:4]
            if  not PAT_YYYY.match(year):
                raise Exception(msg + ', fail to parse the year part, %s' % year)
            month = value[4:6]
            date = value[6:8]
            ddd = datetime.date(int(year), int(month), int(date))
        else:
            raise Exception(msg)
        return calendar.timegm((ddd.timetuple()))
    elif value.endswith('d'):
        try:
            days = int(value[:-1])
        except ValueError:
            raise Exception(msg)
        return time.time()-days*24*60*60
    else:
        raise Exception(msg)

def hdate(date):
    "Transform given YYYYMMDD date into HDFS dir structure YYYY/MM/DD"
    date = str(date)
    return '%s/%s/%s' % (date[0:4], date[4:6], date[6:8])

def range_dates(trange):
    "Provides dates range in HDFS format from given list"
    out = [hdate(str(trange[0]))]
    if  trange[0] == trange[1]:
        return out
    tst = dateformat(trange[0])
    while True:
        tst += 24*60*60
        tdate = time.strftime("%Y%m%d", time.gmtime(tst))
        out.append(hdate(tdate))
        if  str(tdate) == str(trange[1]):
            break
    return out

def hdfs_path(hdir, dateinput):
    "Construct HDFS path for WMArchive data"
    dates = dateinput.split('-')
    if  len(dates) == 2:
        return ['%s/%s' % (hdir, d) for d in range_dates(dates)]
    dates = dateinput.split(',')
    if  len(dates) > 1:
        return ['%s/%s' % (hdir, hdate(d)) for d in dates]
    return ['%s/%s' % (hdir, hdate(dateinput))]

def run(fout, hdir, date, yarn=None, verbose=None):
    """
    Main function to run pyspark job.
    """
    if  not date:
        raise Exception("Not date is provided")

    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    # read DBS and Phedex tables
    # construct here hdfs path and pass empty string as a date
    rdd = avro_rdd(ctx, sqlContext, hdfs_path(hdir, date), date='', verbose=verbose)

    def getdata(row):
        """
        Helper function to extract useful data from WMArchive records.
        You may adjust it to your needs. Given row is a dict object.
        """
        meta = row.get('meta_data', {})
        sites = []
        out = {'host': meta.get('host', ''), 'task': row.get('task', '')}
        for step in row['steps']:
            if step['name'].lower().startswith('cmsrun'):
                site = step.get('site', '')
                output = step.get('output', [])
                perf = step.get('performance', {})
                cpu = perf.get('cpu', {})
                mem = perf.get('memory', {})
                storage = perf.get('storage', {})
                out['ncores'] = cpu['NumberOfStreams']
                out['nthreads'] = cpu['NumberOfThreads']
                out['site'] = site
                out['jobCPU'] = cpu['TotalJobCPU']
                out['jobTime'] = cpu['TotalJobTime']
                out['evtCPU'] = cpu['TotalEventCPU']
                out['evtThroughput'] = cpu['EventThroughput']
                if output:
                    output = output[0]
                    out['appName'] = output.get('applicationName', '')
                    out['appVer'] = output.get('applicationName', '')
                    out['globalTag'] = output.get('globalTag', '')
                    out['era'] = output.get('acquisitionEra', '')
                else:
                    out['appName'] = ''
                    out['appVer'] = ''
                    out['globalTag'] = ''
                    out['era'] = ''
                break
        return out

    out = rdd.map(lambda r: getdata(r))
    if  verbose:
        print(out.take(1)) # out here is RDD object

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        # output will be saved as-is, in this case the out is an RDD which
        # contains json records, therefore the output will be records
        # coming out from getdata helper function above.
        out.saveAsTextFile(fout)

    ctx.stop()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    run(opts.fout, opts.hdir, opts.date, opts.yarn, opts.verbose)
    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
