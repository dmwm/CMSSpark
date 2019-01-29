#!/usr/bin/env python
#-*- coding: utf-8 -*-
# pylint: disable=
"""
File       : process.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: script to parse phedex data produce on HDFS and create a new dataframe
with the following attributes
site,dateset,min_date,max_date,min_rdate,max_rdate,min_size,max_size,days
Here is an example how to produce it:
mergePhedex.py --idir=$PWD/phedex --dates=20170101-20171206 --fout=phedex.csv
"""

# system modules
import os
import sys
import time
import argparse
import subprocess

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--idir", action="store",
                                 dest="idir", default="", help="Input directory to read")
        self.parser.add_argument("--dates", action="store",
                                 dest="dates", default="", help="date range to read, e.g. YYYYMMDD or YYYYMMDD-YYYYMMDD")
        self.parser.add_argument("--fout", action="store",
                                 dest="fout", default="cmsagg", help="Output file")
        self.parser.add_argument("--verbose", action="store_true",
                                 dest="verbose", default=False, help="verbose output")


def date_stamp(date):
    "Convert YYYYMMDD into sec since epoch"
    sec = time.mktime(time.strptime(date, '%Y%m%d'))
    return sec


def days_present(max_d, rd):
    "Find total number of days"
    if max_d and rd:
        secs = date_stamp(str(int(max_d))) - date_stamp(str(int(rd)))
        secd = 60*60*24
        return int(round(secs/secd))
    return -1


def update(rdict, giddict, line):
    # split date,site,dataset,size,replica_date
    date, site, dataset, size, rdate, gid = line.split(',')
    date = int(date)
    size = int(size)
    rdate = int(rdate)
    if gid == "null" or gid == '':
        gid = "-1"
    gid = int(gid)
    key = (site, dataset, rdate, gid)
    keyGid = (site, dataset, rdate)

    if keyGid in giddict:
        # look for a convertion from valid->-1 or -1->valid
        lastGid = giddict[keyGid]
        if lastGid != gid:
            if gid == -1 and lastGid != -1:
                gid = lastGid
                key = (site, dataset, rdate, gid)
            if lastGid == -1 and gid != -1:
                # fix the old dictionary first
                keyDel = (site, dataset, rdate, lastGid)
                if key in rdict:
                    print 'surprising', key, keyDel
                rdict[key] = rdict[keyDel]
                del rdict[keyDel]

    if key in rdict:
        _min_date, _max_date, _ave_size, _max_size, _nom_days, _last_size  = rdict[key]

        if date != _max_date:
            # this will miss the last day in the average - that will be fixed in the next step
            if _nom_days == 1:
                _ave_size = _last_size
            else:
                _ave_size = int( (_ave_size*_nom_days + _last_size) / float(_nom_days+1))

            days = _nom_days+1
            _last_size = 0
        else:
            days = _nom_days
        min_date = min(date, _min_date)
        max_date = max(date, _max_date)
        last_size = _last_size+size
        ave_size = _ave_size
        max_size = _max_size
        if last_size > _max_size:
            max_size = last_size

        rdict[key] = [min_date, max_date, ave_size, max_size, days, last_size]
        giddict[keyGid] = gid
    else:
        days = 1
        rdict[key] = [date, date, size, size, days, size]
        giddict[keyGid] = gid
    return

def updateAveSize(rdict):

    for key in rdict:
        _min_date, _max_date, _ave_size, _max_size, _nom_days, _last_size  = rdict[key]
        if _nom_days == 1:
            _ave_size = _last_size
        else:
            _ave_size = int( (_ave_size*_nom_days + _last_size) / float(_nom_days+1))

        rdict[key][2] = _ave_size



def process(idir, dates, fout):
    "read data from HDFS and create CSV files from it"
    rdict = {}
    giddict = {}
    for date in range(dates[0], dates[-1]+1):  # include the last date in the range
        for path, dirs, files in os.walk(idir):
            if path.endswith(str(date)):
                time0 = time.time()
                arr = path.split('/')
                for ifile in files:
                    if 'part-' not in ifile:
                        continue
                    iname = os.path.join(path, ifile)
                    with open(iname) as istream:
                        while True:
                            line = istream.readline().replace('\n', '')
                            if not line:
                                break
                            update(rdict, giddict, line)
                print('%s %s, #keys %s' %
                      (path, time.time()-time0, len(rdict.keys())))
        sys.__stdout__.flush()
        sys.__stderr__.flush()
    updateAveSize(rdict)
    with open(fout, 'w') as ostream:
        ostream.write(
            'site,dataset,rdate,gid,min_date,max_date,ave_size,max_size,days\n')
        for key, val in rdict.iteritems():
            vals = [str(k) for k in list(key)+list(val)]
            ostream.write(','.join(vals[:-1]) + '\n')

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    dates = opts.dates.split('-')
    idates = [int(d) for d in dates]
    process(opts.idir, idates, opts.fout)

if __name__ == '__main__':
    main()
