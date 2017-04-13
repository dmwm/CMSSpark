#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : dbs_phedex.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Spark script to parse DBS and PhEDEx content on HDFS
"""

# system modules
import os
import re
import sys
import time
import json
import argparse
from types import NoneType

from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, print_rows, spark_context
from CMSSpark.utils import elapsed_time, cern_monit

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        year = time.strftime("%Y", time.localtime())
        hdir = 'hdfs:///project/awg/cms'
        msg = 'Location of CMS folders on HDFS, default %s' % hdir
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        fout = 'dbs_datasets.csv'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default=fout, help='Output file name, default %s' % fout)
        self.parser.add_argument("--tier", action="store",
            dest="tier", default="", help='Select datasets for given data-tier, use comma-separated list if you want to handle multiple data-tiers')
        self.parser.add_argument("--era", action="store",
            dest="era", default="", help='Select datasets for given acquisition era')
        self.parser.add_argument("--release", action="store",
            dest="release", default="", help='Select datasets for given CMSSW release')
        self.parser.add_argument("--cdate", action="store",
            dest="cdate", default="", help='Select datasets starting given creation date in YYYYMMDD format')
        self.parser.add_argument("--patterns", action="store",
            dest="patterns", default="", help='Select datasets patterns')
        self.parser.add_argument("--antipatterns", action="store",
            dest="antipatterns", default="", help='Select datasets antipatterns')
        msg = 'Perform action over DBS info on HDFS: tier_stats, dataset_stats'
        self.parser.add_argument("--action", action="store",
            dest="action", default="tier_stats", help=msg)
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")
        msg = "Send results via StompAMQ to a broker, provide broker credentials in JSON file"
        self.parser.add_argument("--amq", action="store",
            dest="amq", default="", help=msg)

def run(fout, verbose=None, yarn=None, tier=None, era=None,
        release=None, cdate=None, patterns=[], antipatterns=[]):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    time0 = time.time()

    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sqlContext = HiveContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sqlContext, verbose=verbose))
    tables.update(phedex_tables(sqlContext, verbose=verbose))
    phedex_df = tables['phedex_df']
    daf = tables['daf']
    ddf = tables['ddf']
    bdf = tables['bdf']
    fdf = tables['fdf']
    aef = tables['aef']
    pef = tables['pef']
    mcf = tables['mcf']
    ocf = tables['ocf']
    rvf = tables['rvf']

    print("### ddf from main", ddf)

    # aggregate phedex info into dataframe
    phedex_cols = ['node_name', 'dataset_name', 'dataset_is_open', 'block_bytes', 'replica_time_create']
    newpdf = phedex_df.select(phedex_cols).groupBy(['node_name', 'dataset_name', 'dataset_is_open'])\
            .agg({'block_bytes':'sum', 'replica_time_create':'max'})\
            .withColumnRenamed('sum(block_bytes)', 'pbr_size')\
            .withColumnRenamed('max(replica_time_create)', 'max_replica_time')
    newpdf.registerTempTable('newpdf')
    print_rows(newpdf, 'newpdf', verbose)
    newpdf.persist(StorageLevel.MEMORY_AND_DISK)

    # join tables
    cols = ['*'] # to select all fields from table
    cols = ['d_dataset_id', 'd_dataset','d_creation_date','d_is_dataset_valid','f_event_count','f_file_size','dataset_access_type','acquisition_era_name','processing_version']

    # join tables
    stmt = 'SELECT %s FROM ddf JOIN fdf on ddf.d_dataset_id = fdf.f_dataset_id JOIN daf ON ddf.d_dataset_access_type_id = daf.dataset_access_type_id JOIN aef ON ddf.d_acquisition_era_id = aef.acquisition_era_id JOIN pef ON ddf.d_processing_era_id = pef.processing_era_id' % ','.join(cols)
    print(stmt)
    joins = sqlContext.sql(stmt)
    print_rows(joins, 'joins', verbose)

    # keep joins table around
    joins.persist(StorageLevel.MEMORY_AND_DISK)

    # construct conditions
    cond = 'dataset_access_type = "VALID" AND d_is_dataset_valid = 1'
    if  era:
        cond += ' AND acquisition_era_name like "%s"' % era.replace('*', '%')
    cond_pat = []
    for name in patterns:
        if  name.find('*') != -1:
            cond_pat.append('d_dataset LIKE "%s"' % name.replace('*', '%'))
        else:
            cond_pat.append('d_dataset="%s"' % name)
    if  cond_pat:
        cond += ' AND (%s)' % ' OR '.join(cond_pat)
    for name in antipatterns:
        if  name.find('*') != -1:
            cond += ' AND d_dataset NOT LIKE "%s"' % name.replace('*', '%')
        else:
            cond += ' AND d_dataset!="%s"' % name
    if  cdate:
        dates = cdate.split('-')
        if  len(dates) == 2:
            cond += ' AND d_creation_date > %s AND d_creation_date < %s' \
                    % (unix_tstamp(dates[0]), unix_tstamp(dates[1]))
            joins = joins.where(joins.d_creation_date>unix_tstamp(dates[0]))
        elif len(dates) == 1:
            cond += ' AND d_creation_date > %s' % unix_tstamp(dates[0])
        else:
            raise NotImplementedError("Given dates are not supported, please either provide YYYYMMDD date or use dash to define a dates range.")

    print("Applied condition %s" % cond)

    if  tier:
        if isinstance(tier, list):
            tiers = tier
        else:
            tiers = tier.split(',')
        gen_cond = cond
        for tier in tiers:
            cond = gen_cond + ' AND d_dataset like "%%/%s"' % tier
            fjoin = joins.where(cond).distinct().select(cols)
    else:
        fjoin = joins.where(cond).distinct().select(cols)

    # at this step we have fjoin table with Row(d_dataset_id=9413359, d_dataset=u'/SingleMu/CMSSW_7_1_0_pre9-GR_R_71_V4_RelVal_mu2012D_TEST-v6000/DQM', d_creation_date=1406060166.0, d_is_dataset_valid=1, f_event_count=5318, f_file_size=21132638.0, dataset_access_type=u'DELETED', acquisition_era_name=u'CMSSW_7_1_0_pre9', processing_version=u'6000'))

    newdf = fjoin\
            .groupBy(['d_dataset','d_dataset_id','dataset_access_type','acquisition_era_name','processing_version'])\
            .agg({'f_event_count':'sum', 'f_file_size':'sum', 'd_creation_date':'max'})\
            .withColumnRenamed('sum(f_event_count)', 'evts')\
            .withColumnRenamed('sum(f_file_size)', 'size')\
            .withColumnRenamed('max(d_creation_date)', 'date')


    # at this point we have ndf dataframe with our collected stats for every dataset
    # let's join it with release info
    newdf.registerTempTable('newdf')
    cols = ['d_dataset_id','d_dataset','evts','size','date','dataset_access_type','acquisition_era_name','processing_version','r_release_version']
    stmt = 'SELECT %s FROM newdf JOIN mcf ON newdf.d_dataset_id = mcf.mc_dataset_id JOIN ocf ON mcf.mc_output_mod_config_id = ocf.oc_output_mod_config_id JOIN rvf ON ocf.oc_release_version_id = rvf.r_release_version_id' % ','.join(cols)
    agg_dbs_df = sqlContext.sql(stmt)
    agg_dbs_df.registerTempTable('agg_dbs_df')
    print_rows(agg_dbs_df, 'agg_dbs_df', verbose)

    # keep agg_dbs_df table around
    agg_dbs_df.persist(StorageLevel.MEMORY_AND_DISK)

    # join dbs and phedex tables
    cols = ['d_dataset_id','d_dataset','evts','size','date','dataset_access_type','acquisition_era_name','processing_version','r_release_version','dataset_name','node_name','pbr_size','dataset_is_open','max_replica_time']
    stmt = 'SELECT %s FROM agg_dbs_df JOIN newpdf ON agg_dbs_df.d_dataset = newpdf.dataset_name' % ','.join(cols)
    finaldf = sqlContext.sql(stmt)
    print_rows(finaldf, 'finaldf', verbose)

    # collect results and perform re-mapping
    out = []
    idx = 0
    site = '' # will fill it out when process Phedex data
    naccess = 0 # will fill it out when process Phedex+DBS data and calc naccess
    njobs = 0 # will fill it out when process JobMonitoring data
    cpu = 0 # will fill it out when process JobMonitoring data
    atype = '' # will fill out when process DBS data
    pver = '' # will fill out when process DBS data
    rver = '' # will fill out when process DBS data
    drop_cols = ['d_dataset_id','d_dataset','dataset_access_type','acquisition_era_name','processing_version','r_release_version', 'node_name', 'dataset_name']
    for row in finaldf.collect():
        rdict = row.asDict()
        _, primds, procds, tier = rdict['d_dataset'].split('/')
        rdict['primds'] = primds
        rdict['procds'] = procds
        rdict['tier'] = tier
        rdict['era'] = rdict.get('acquisition_era_name', era)
        rdict['atype'] = rdict.get('dataset_access_type', atype)
        rdict['pver'] = rdict.get('processing_version', pver)
        rdict['release'] = rdict.get('r_release_version', rver)
        node = rdict.get('node_name', site).split('_')
        if len(node) == 4:
            node_name = '_'.join(node[:3])
            node_type = node[-1]
        else:
            node_name = rdict.get('node_name', site)
            node_type = 'NA'
        rdict['site'] = node_name
        rdict['node_type'] = node_type
        rdict['dataset'] = rdict['dataset_name']
        rdict['naccess'] = naccess
        rdict['njobs'] = njobs
        rdict['cpu'] = cpu
        for key in drop_cols:
            if  key in rdict:
                del rdict[key]
        out.append(rdict)
        if  verbose and idx < 5:
            print(rdict)
        idx += 1

    # write out output
    if  fout:
        with open(fout, 'w') as ostream:
            headers = sorted(out[0].keys())
            ostream.write(','.join(headers)+'\n')
            for rdict in out:
                arr = []
                for key in headers:
                    arr.append(str(rdict[key]))
                ostream.write(','.join(arr)+'\n')

    ctx.stop()
    if  verbose:
        print("Elapsed time %s" % elapsed_time(time0))
    return out

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    time0 = time.time()
    fout = opts.fout
    verbose = opts.verbose
    yarn = opts.yarn
    tier = opts.tier
    era = opts.era
    release = opts.release
    cdate = opts.cdate
    patterns = opts.patterns.split(',') if opts.patterns else []
    antipatterns = opts.antipatterns.split(',') if opts.antipatterns else []
    res = run(fout, verbose, yarn, tier, era, release, cdate, patterns, antipatterns)
    cern_monit(res, opts.amq)

    if  verbose:
        print("### Collected", len(res), "results")
        if  len(res)>0:
            print(res[0])

    print('Start time  : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time0)))
    print('End time    : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(time.time())))
    print('Elapsed time: %s sec' % elapsed_time(time0))

if __name__ == '__main__':
    main()
