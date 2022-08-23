#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : dates.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : ...
"""

# system modules
import argparse
import datetime


class OptionParser:
    def __init__(self):
        """User based option parser"""
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--start", action="store",
                                 dest="start", default="", help="start date (YYYYMMDD)")
        self.parser.add_argument("--ndays", action="store",
                                 dest="ndays", default=30, help="Number of days, default 30")
        iformat = '%Y-%m-%d'
        self.parser.add_argument("--format", action="store",
                                 dest="format", default=iformat, help="date format, e.g. %%Y%%m%%d")
        self.parser.add_argument("--range", action="store_true",
                                 dest="range", default=False, help="show full range")


def dates(start, numdays):
    base = datetime.datetime.today()
    if start:
        year = int(start[:4])
        month = int(start[4:6])
        day = int(start[6:])
        base = datetime.datetime(year, month, day)
    date_list = [base - datetime.timedelta(days=x) for x in range(0, numdays)]
    return date_list


def range_dates(start, numdays):
    base = datetime.datetime.today()
    if start:
        year = int(start[:4])
        month = int(start[4:6])
        day = int(start[6:])
        base = datetime.datetime(year, month, day)
    for step in range(0, numdays):
        yield base - datetime.timedelta(days=step)


def dformat(date, iformat):
    return date.strftime(iformat)


def main():
    """Main function"""
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    if opts.range:
        for date in range_dates(opts.start, int(opts.ndays)):
            print(dformat(date, opts.format))
    else:
        date_list = dates(opts.start, int(opts.ndays))
        min_date, max_date = date_list[-1], date_list[0]
        print('%s %s' % (dformat(min_date, opts.format), dformat(max_date, opts.format)))


if __name__ == '__main__':
    main()
