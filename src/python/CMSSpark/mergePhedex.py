#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : process.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: script to fetch data from HDFS and create local CSV file(s)
./getCSV.py --idir=hdfs:///cms/users/vk/dbs_condor --date=20171121 --fout=dbs_condor
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

def days_present(min_d, max_d, min_rd, max_rd):
    "Find total number of days"
    if min_d and min_rd and max_d and max_rd:
        minv = min(int(min_d), int(min_rd))
        maxv = max(int(max_d), int(max_rd))
        secs = date_stamp(str(maxv)) - date_stamp(str(minv))
        secd = 60*60*24
        return int(round(secs/secd))
    return -1

def update(rdict, line):
    # split date,site,dataset,size,replica_date
    date, site, dataset, size, rdate = line.split(',')
    date = int(date)
    size = int(size)
    rdate = int(rdate)
    key = (site, dataset)
    if key in rdict:
        _min_date, _max_date, _min_rdate, _max_rdate, _min_size, _max_size, _ = rdict[key]
        min_date = min(date, _min_date)
        max_date = max(date, _max_date)
        min_rdate = min(rdate, _min_rdate)
        max_rdate = max(rdate, _max_rdate)
        min_size = min(size, _min_size)
        max_size = max(size, _max_size)
        days = days_present(min_date, max_date, min_rdate, max_rdate)
        rdict[key] = [min_date, max_date, min_rdate, max_rdate, min_size, max_size, days]
    else:
        rdict[key] = [date, date, rdate, rdate, size, size, -1]
    return

def process(idir, dates, fout):
    "read data from HDFS and create CSV files from it"
    rdict = {}
    for path, dirs, files in os.walk(idir):
        for date in range(dates[0], dates[-1]):
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
                            update(rdict, line)
                print('%s %s, #keys %s' % (path, time.time()-time0, len(rdict.keys())))
        sys.__stdout__.flush()
        sys.__stderr__.flush()
    with open(fout, 'w') as ostream:
        ostream.write('size,dataset,min_date,max_date,min_rdate,max_rdate,min_size,max_size,days\n')
        for key, val in rdict.iteritems():
            vals = [str(k) for k in list(key)+list(val)]
            ostream.write(','.join(vals) + '\n')

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    dates = opts.dates.split('-')
    idates = [int(d) for d in dates]
    process(opts.idir, idates, opts.fout)

if __name__ == '__main__':
    main()
