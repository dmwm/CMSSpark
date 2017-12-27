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
import datetime


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


def update(rdict, line):
    # split date,site,dataset,size,replica_date
    date, site, dataset, size, rdate, gid = line.split(',')
    date = int(date)
    size = int(size)
    rdate = int(rdate)
    if gid == "null":
        gid = "-1"
    gid = int(gid)
    key = (site, dataset, rdate, gid)
    if key in rdict:
        _min_date, _max_date, _ave_size, _last_size, _nom_days = rdict[key]
        if date != _max_date:
            # this will miss the last day in the average - but its what can be done easily
            _ave_size = int(
                (_ave_size*_nom_days + _last_size) / float(_nom_days+1))
            days = _nom_days+1
            _last_size = 0
        else:
            days = _nom_days
        min_date = min(date, _min_date)
        max_date = max(date, _max_date)
        last_size = _last_size+size
        ave_size = _ave_size
        rdict[key] = [min_date, max_date, ave_size, last_size, days]
    else:
        days = 1
        rdict[key] = [date, date, size, size, days]
    return


def clean(rdict):
    rdict3 = {}
    for key, val in rdict.iteritems():
        t = list(key)[0:3]  # all but gid
        key3 = (t[0], t[1], t[2])
        if key3 not in rdict3:
            rdict3[key3] = []
        rdict3[key3].append(list(key)+list(val))

    minDateCol = 0
    maxDateCol = 1
    maxSizeCol = 2
    daysCol = 4
    aveSizeCol = 3

    key3s = rdict3.keys()
    for k in key3s:
        foundIt = True
        while foundIt:
            foundIt = False
            vals = rdict3[k]
            n = len(vals)
            if n == 1:
                continue
            for i in range(n):
                if foundIt:
                    break
                gid1 = vals[i][3]
                for j in range(n):
                    if foundIt:
                        break
                    gid2 = vals[j][3]

                    if gid1 != -1 and gid2 != -1:
                        continue
                    key1 = k+(gid1,)
                    key2 = k+(gid2,)

            # maybe these are related.. look for neighboring dates
                    dMin1 = datetime.datetime.strptime(
                        str(rdict[key1][minDateCol]), "%Y%m%d").date()
                    dMax2 = datetime.datetime.strptime(
                        str(rdict[key2][maxDateCol]), "%Y%m%d").date()

                    if ((dMin1-dMax2).days == 1):
                        # make up the new entry by merging the two and add it to the dict
                        gid = gid1
                        key = key1
                        if gid == -1:
                            gid = gid2
                            key = key2
                        min_date = rdict[key2][minDateCol]
                        max_date = rdict[key1][maxDateCol]
                        maxSize1 = rdict[key1][maxSizeCol]
                        maxSize2 = rdict[key2][maxSizeCol]
                        max_size = maxSize1
                        if maxSize2 > maxSize1:
                            max_size = maxSize2
                        days1 = rdict[key1][daysCol]
                        days2 = rdict[key2][daysCol]
                        days = days1+days2

                        ave_size = 1.0/(float(days)) * (days1*float(rdict[key1][aveSizeCol]) +
                                                        days2*float(rdict[key2][aveSizeCol]))
                        ave_size = int(ave_size)
                        site = k[0]
                        dataset = k[1]
                        rdate = k[2]

                        del rdict[key1]
                        del rdict[key2]

                        tArr = [site, dataset, rdate,
                                gid, min_date, max_date,
                                ave_size, max_size, days]

                        rdict[key] = tArr[4:]
                        new3 = [tArr]
                        for l in range(n):
                            if l == i:
                                continue
                            if l == j:
                                continue
                            new3.append(vals[l])
                        rdict3[k] = new3
                        foundIt = True
    return


def process(idir, dates, fout):
    "read data from HDFS and create CSV files from it"
    rdict = {}
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
                            update(rdict, line)
                print('%s %s, #keys %s' %
                      (path, time.time()-time0, len(rdict.keys())))
        sys.__stdout__.flush()
        sys.__stderr__.flush()
    clean(rdict)
    with open(fout, 'w') as ostream:
        ostream.write(
            'site,dataset,rdate,gid,min_date,max_date,ave_size,max_size,days\n')
        for key, val in rdict.iteritems():
            vals = [str(k) for k in list(key)+list(val)]
            ostream.write(','.join(vals) + '\n')


def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    dates = opts.dates.split('-')
    idates = [int(d) for d in dates]
    process(opts.idir, idates, opts.fout)


if __name__ == '__main__':
    main()
