#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
Set of utililities
"""

# system modules
import os
import gzip
import time

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

def split_date(date):
    "Split given YYYYMMDD into pieces"
    val = str(date)
    return val[:4], val[4:6], val[6:]

def info(func):
    "decorator to spark workflow"
    def wrapper():
        time0 = time.time()
        func()
        print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
        print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
        print('Elapsed time: %s sec' % elapsed_time(time0))
    wrapper.__name__ = func.__name__
    return wrapper
