#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : utils.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Set of utililities
"""

# system modules
import os
import gzip
import time

# WMCore modules
try:
    # stopmAMQ API
    from WMCore.Services.StompAMQ.StompAMQ import StompAMQ
except ImportError:
    StompAMQ = None

def credentials(fname=None):
    "Read credentials from PBR_BROKER environment"
    if  not fname:
        fname = os.environ.get('PBR_BROKER', '')
    if  not os.path.isfile(fname):
        return {}
    with open(fname, 'r') as istream:
        data = json.load(istream)
    return data

def cern_monit(res, chunk=1000):
    "Send results to CERN MONIT system"
    creds = credentials(amq)
    host, port = creds['host_and_ports'].split(':')
    port = int(port)
    if  creds and StompAMQ:
        print("### Send %s docs via StompAMQ" % len(res))
        amq = StompAMQ(creds['username'], creds['password'], \
            creds['producer'], creds['topic'], [(host, port)])
        data = []
        for doc in res:
            hid = doc.get("hash", 1)
            data.append(amq.make_notification(doc, hid))
        results = amq.send(data)
        print("### results sent by AMQ", len(results))

class GzipFile(gzip.GzipFile):
    def __enter__(self):
        "Context manager enter method"
        if self.fileobj is None:
            raise ValueError("I/O operation on closed GzipFile object")
        return self

    def __exit__(self, *args):
        "Context manager exit method"
        self.close()

def fopen(fin, mode='r'):
    "Return file descriptor for given file"
    if  fin.endswith('.gz'):
        stream = gzip.open(fin, mode)
        # if we use old python we switch to custom gzip class to support
        # context manager and with statements
        if  not hasattr(stream, "__exit__"):
            stream = GzipFile(fin, mode)
    else:
        stream = open(fin, mode)
    return stream

def htime(seconds):
    "Convert given seconds into human readable form of N hour(s), N minute(s), N second(s)"
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    def htimeformat(msg, key, val):
        "Helper function to proper format given message/key/val"
        if  val:
            if  msg:
                msg += ', '
            msg += '%d %s' % (val, key)
            if  val > 1:
                msg += 's'
        return msg

    out = ''
    out = htimeformat(out, 'day', days)
    out = htimeformat(out, 'hour', hours)
    out = htimeformat(out, 'minute', minutes)
    out = htimeformat(out, 'second', secs)
    return out

def elapsed_time(time0):
    "Return elapsed time from given time stamp"
    return htime(time.time()-time0)

def unix_tstamp(date):
    "Convert given date into unix seconds since epoch"
    if  not isinstance(date, str):
        raise NotImplementedError('Given date %s is not in string YYYYMMDD format' % date)
    if  len(date) == 8:
        return int(time.mktime(time.strptime(date, '%Y%m%d')))
    elif len(date) == 10: # seconds since epoch
        return int(date)
    else:
        raise NotImplementedError('Given date %s is not in string YYYYMMDD format' % date)


