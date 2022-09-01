#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : conf.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : module which holds configuration options
"""

import click


def common_options(*options):
    """Common user arguments wrapper"""

    def wrapper(function):
        for option in reversed(options):
            function = option(function)
        return function

    return wrapper


ARG_HDIR = click.option("--hdir", default="", help="Location of CMS folders on HDFS")
ARG_FOUT = click.option("--fout", default="", help="Output file name")
ARG_FIN = click.option("--fin", default="", help="Input file name")
ARG_DATE = click.option("--date", default="", help="Select CMSSW data for specific date (YYYYMMDD)")
ARG_NO_LOG4J = click.option("--no-log4j", default=False, is_flag=True, help="Disable spark log4j messages")
ARG_YARN = click.option("--yarn", default=False, is_flag=True,
                        help="run job on analytix cluster via yarn resource manager")
ARG_CVMFS = click.option("--cvmfs", default=False, is_flag=True,
                         help="un job on analytix cluster via lxplus7 cvmfs environment")
ARG_VERBOSE = click.option("--verbose", default=False, is_flag=True, help="verbose output")
