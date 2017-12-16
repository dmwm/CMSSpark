#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : makeCSV.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: script to fetch data from HDFS and create local CSV file(s)
#./getCSV.py --hdir=hdfs:///cms/users/vk/dbs_condor --date=20171121 --odir=dbs_condor
./getCSV.py --idir=$PWD/dbs_condor --dates=20171121-20171128
"""

# system modules
import os
import sys
import argparse
import subprocess

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--idir", action="store",
            dest="idir", default="", help="Input data path")
        self.parser.add_argument("--dates", action="store",
            dest="dates", default="", help="dates or dates-rante to read, e.g. YYYYMMDD-YYYYMMDD")
#        self.parser.add_argument("--hdir", action="store",
#            dest="hdir", default="", help="Input HDFS path")
#        self.parser.add_argument("--odir", action="store",
#            dest="odir", default="cmsagg", help="Output file")
#        self.parser.add_argument("--verbose", action="store_true",
#            dest="verbose", default=False, help="verbose output")

#def makeCSV(hdir, dates, odir):
#    "read data from HDFS and create CSV files from it"
#    cmd = "hadoop fs -get %s %s" % (hdir, odir)
#    print("run %s" % cmd)
#    proc = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr)
#    proc.wait()
#    if proc.returncode:
#        print("Fail to read, return code %s" % proc.returncode)
#        os.exit(proc.returncode)
def makeCSV(idir, dates):
#    "read data from HDFS and create CSV files from it"
    for path, dirs, files in os.walk(idir):
        for date in dates:
            # first loop over output dir
            if not path.endswith(str(date)):
                continue
            arr = path.split('/')
            oname = '%s-%s.csv' % (arr[-2], arr[-1])
            print("write %s" % oname)
            with open(oname, 'w') as ostream:
                headers = None
                for ifile in files:
                    if 'part-' not in ifile:
                        continue
                    iname = os.path.join(path, ifile)
                    with open(iname) as istream:
                        first_line = istream.readline()
                        if not headers:
                            headers = first_line
                            ostream.write(headers)
                        while True:
                            line = istream.readline().replace('"', '')
                            if not line:
                                break
                            ostream.write(line)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    dates = opts.dates.split('-')
    idates = [int(d) for d in dates]
    pdates = [d for d in range(idates[0], idates[-1])]
    makeCSV(opts.idir, pdates)
#    makeCSV(opts.hdir, idates, opts.odir)

if __name__ == '__main__':
    main()
