#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : dbs_phedex.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : Spark script to parse and aggregate DBS and PhEDEx records on HDFS.
"""

# system modules
from pyspark.sql import SQLContext

# CMSSpark modules
from CMSSpark.spark_utils import dbs_tables, phedex_tables, spark_context, split_dataset
from CMSSpark.utils import info
from CMSSpark.conf import OptionParser


def run(fout, yarn=None, verbose=None, patterns=None, antipatterns=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sql_context = SQLContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    tables.update(dbs_tables(sql_context, inst=inst, verbose=verbose))
    tables.update(phedex_tables(sql_context, verbose=verbose))
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
    newpdf = phedex_df.select(phedex_cols).groupBy(['node_name', 'dataset_name', 'dataset_is_open']) \
        .agg({'block_bytes': 'sum', 'replica_time_create': 'max'}) \
        .withColumnRenamed('sum(block_bytes)', 'pbr_size') \
        .withColumnRenamed('max(replica_time_create)', 'max_replica_time')
    newpdf.registerTempTable('newpdf')
    #    print_rows(newpdf, 'newpdf', verbose)
    #    newpdf.persist(StorageLevel.MEMORY_AND_DISK)

    # join tables
    cols = ['*']  # to select all fields from table
    cols = ['d_dataset_id', 'd_dataset', 'd_creation_date', 'd_is_dataset_valid', 'f_event_count', 'f_file_size',
            'dataset_access_type', 'acquisition_era_name', 'processing_version']

    # join tables
    stmt = 'SELECT %s FROM ddf JOIN fdf on ddf.d_dataset_id = fdf.f_dataset_id JOIN daf ON ddf.d_dataset_access_type_id = daf.dataset_access_type_id JOIN aef ON ddf.d_acquisition_era_id = aef.acquisition_era_id JOIN pef ON ddf.d_processing_era_id = pef.processing_era_id' % ','.join(cols)
    print(stmt)
    joins = sql_context.sql(stmt)
    #    print_rows(joins, 'joins', verbose)

    # keep joins table around
    #    joins.persist(StorageLevel.MEMORY_AND_DISK)

    # construct conditions
    cond = 'dataset_access_type = "VALID" AND d_is_dataset_valid = 1'
    fjoin = joins.where(cond).distinct().select(cols)

    # at this step we have fjoin table with Row(d_dataset_id=9413359, d_dataset=u'/SingleMu/CMSSW_7_1_0_pre9-GR_R_71_V4_RelVal_mu2012D_TEST-v6000/DQM', d_creation_date=1406060166.0, d_is_dataset_valid=1, f_event_count=5318, f_file_size=21132638.0, dataset_access_type=u'DELETED', acquisition_era_name=u'CMSSW_7_1_0_pre9', processing_version=u'6000'))

    newdf = fjoin \
        .groupBy(['d_dataset', 'd_dataset_id', 'dataset_access_type', 'acquisition_era_name', 'processing_version']) \
        .agg({'f_event_count': 'sum', 'f_file_size': 'sum', 'd_creation_date': 'max'}) \
        .withColumnRenamed('sum(f_event_count)', 'evts') \
        .withColumnRenamed('sum(f_file_size)', 'size') \
        .withColumnRenamed('max(d_creation_date)', 'date')

    # at this point we have ndf dataframe with our collected stats for every dataset
    # let's join it with release info
    newdf.registerTempTable('newdf')
    cols = ['d_dataset_id', 'd_dataset', 'evts', 'size', 'date', 'dataset_access_type', 'acquisition_era_name',
            'processing_version', 'r_release_version']
    stmt = 'SELECT %s FROM newdf JOIN mcf ON newdf.d_dataset_id = mcf.mc_dataset_id JOIN ocf ON mcf.mc_output_mod_config_id = ocf.oc_output_mod_config_id JOIN rvf ON ocf.oc_release_version_id = rvf.r_release_version_id' % ','.join(cols)
    agg_dbs_df = sql_context.sql(stmt)
    agg_dbs_df.registerTempTable('agg_dbs_df')
    #    print_rows(agg_dbs_df, 'agg_dbs_df', verbose)

    # keep agg_dbs_df table around
    #    agg_dbs_df.persist(StorageLevel.MEMORY_AND_DISK)

    # join dbs and phedex tables
    #    cols = ['d_dataset_id','d_dataset','evts','size','date','dataset_access_type','acquisition_era_name','processing_version','r_release_version','dataset_name','node_name','pbr_size','dataset_is_open','max_replica_time']
    cols = ['d_dataset', 'evts', 'size', 'date', 'dataset_access_type', 'acquisition_era_name', 'r_release_version',
            'node_name', 'pbr_size', 'dataset_is_open', 'max_replica_time']
    stmt = 'SELECT %s FROM agg_dbs_df JOIN newpdf ON agg_dbs_df.d_dataset = newpdf.dataset_name' % ','.join(cols)
    finaldf = sql_context.sql(stmt)

    # keep agg_dbs_df table around
    #    finaldf.persist(StorageLevel.MEMORY_AND_DISK)
    #    print_rows(finaldf, stmt, verbose)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        ndf = split_dataset(finaldf, 'd_dataset')
        ndf.write.format("com.databricks.spark.csv") \
            .option("header", "true").save(fout)

    ctx.stop()


@info
def main():
    """Main function"""
    optmgr = OptionParser('dbs_phedex')
    optmgr.parser.add_argument("--patterns", action="store",
                               dest="patterns", default="", help='Select datasets patterns')
    optmgr.parser.add_argument("--antipatterns", action="store",
                               dest="antipatterns", default="", help='Select datasets antipatterns')
    msg = 'DBS instance on HDFS: global (default), phys01, phys02, phys03'
    optmgr.parser.add_argument("--inst", action="store",
                               dest="inst", default="global", help=msg)
    opts = optmgr.parser.parse_args()
    print("Input arguments: %s" % opts)
    inst = opts.inst
    if inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    patterns = opts.patterns.split(',') if opts.patterns else []
    antipatterns = opts.antipatterns.split(',') if opts.antipatterns else []
    run(opts.fout, opts.yarn, opts.verbose, patterns, antipatterns, inst)


if __name__ == '__main__':
    main()
