#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
# Author: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
"""
module which holds configuration options
"""
import argparse

class OptionParser():
    def __init__(self, name=''):
        "User based option parser"
        desc = "Spark %s workflow" % name
        self.parser = argparse.ArgumentParser(prog='PROG', description=desc)
        msg = 'Location of CMS folders on HDFS'
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default="", help=msg)
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help='Output file name')
        self.parser.add_argument("--date", action="store",
            dest="date", default="", help='Select CMSSW data for specific date (YYYYMMDD)')
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytix cluster via yarn resource manager")
        self.parser.add_argument("--cvmfs", action="store_true",
            dest="cvmfs", default=False, help="run job on analytix cluster via lxplus7 cvmfs environment")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")
