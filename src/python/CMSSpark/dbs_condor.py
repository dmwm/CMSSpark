#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : dbs_condor.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : Spark script to parse DBS and Condor records on HDFS.
"""

# system modules
import click
import time

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, lit as _lit, sum as _sum, col as _col, split as _split
from pyspark.sql.types import DoubleType

# CMSSpark modules
from CMSSpark import conf as c
from CMSSpark.spark_utils import dbs_tables, print_rows, union_all
from CMSSpark.spark_utils import spark_context, condor_tables
from CMSSpark.utils import info, split_date


def condor_date(date):
    """Convert given date into AAA date format"""
    if not date:
        date = time.strftime("%Y/%m/%d", time.gmtime(time.time() - 60 * 60 * 24))
        return date
    if len(date) != 8:
        raise Exception("Given date %s is not in YYYYMMDD format" % date)
    year = date[:4]
    month = date[4:6]
    day = date[6:]
    return '%s/%s/%s' % (year, month, day)


def condor_date_unix(date):
    """Convert AAA date into UNIX timestamp"""
    return time.mktime(time.strptime(date, '%Y/%m/%d'))


def run(date, fout, yarn=None, verbose=None, inst='GLOBAL'):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)
    sql_context = SQLContext(ctx)

    # read DBS and Phedex tables
    tables = {}
    dtables = ['daf', 'ddf', 'bdf', 'fdf', 'aef', 'pef', 'mcf', 'ocf', 'rvf']
    tables.update(dbs_tables(sql_context, inst=inst, verbose=verbose, tables=dtables))
    #    tables.update(phedex_tables(sql_context, verbose=verbose))
    #    phedex_df = tables['phedex_df']
    daf = tables['daf']  # dataset access table
    ddf = tables['ddf']  # dataset table
    bdf = tables['bdf']  # block table
    fdf = tables['fdf']  # file table
    aef = tables['aef']  # acquisition era
    pef = tables['pef']  # processing era table
    mcf = tables['mcf']  # output mod config table
    ocf = tables['ocf']  # output module table
    rvf = tables['rvf']  # release version table

    # read Condor rdd
    #    tables.update(condor_tables(sql_context, hdir='hdfs:///cms/users/vk/condor', date=condor_date(date), verbose=verbose))
    tables.update(condor_tables(sql_context, date=condor_date(date), verbose=verbose))
    condor_df = tables['condor_df']  # aaa table

    # aggregate dbs info into dataframe
    cols = ['d_dataset_id', 'd_dataset', 'd_creation_date', 'd_is_dataset_valid', 'f_event_count', 'f_file_size',
            'dataset_access_type', 'acquisition_era_name', 'processing_version']
    stmt = 'SELECT %s FROM ddf JOIN fdf on ddf.d_dataset_id = fdf.f_dataset_id JOIN daf ON ddf.d_dataset_access_type_id = daf.dataset_access_type_id JOIN aef ON ddf.d_acquisition_era_id = aef.acquisition_era_id JOIN pef ON ddf.d_processing_era_id = pef.processing_era_id' % ','.join(
        cols)
    print(stmt)
    joins = sql_context.sql(stmt)

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
    stmt = 'SELECT %s FROM newdf JOIN mcf ON newdf.d_dataset_id = mcf.mc_dataset_id JOIN ocf ON mcf.mc_output_mod_config_id = ocf.oc_output_mod_config_id JOIN rvf ON ocf.oc_release_version_id = rvf.r_release_version_id' % ','.join(
        cols)
    agg_dbs_df = sql_context.sql(stmt)
    agg_dbs_df.registerTempTable('agg_dbs_df')

    # merge dbs+phedex and Condor data
    cols = ['d_dataset', 'evts', 'size', 'date', 'dataset_access_type', 'acquisition_era_name', 'r_release_version']
    cols = cols + ['data.KEvents', 'data.CMSSWKLumis', 'data.CMSSWWallHrs', 'data.Campaign', 'data.Workflow',
                   'data.CpuEff', 'data.CoreHr', 'data.QueueHrs', 'data.CRAB_UserHN', 'data.Type', 'data.ExitCode',
                   'data.TaskType', 'data.RecordTime']
    stmt = 'SELECT %s FROM condor_df JOIN agg_dbs_df ON agg_dbs_df.d_dataset = condor_df.data.DESIRED_CMSDataset WHERE condor_df.data.KEvents > 0' % ','.join(
        cols)
    #     stmt = 'SELECT %s FROM condor_df JOIN dbs_phedex_df ON dbs_phedex_df.d_dataset = condor_df.data.DESIRED_CMSDataset WHERE condor_df.data.KEvents > 0' % ','.join(cols)

    final_df = sql_context.sql(stmt)
    print_rows(final_df, stmt, verbose)

    # keep table around
    final_df.persist(StorageLevel.MEMORY_AND_DISK)

    # user defined function
    def rate(evts, cores):
        """Calculate the rate of events vs cores, if they're not defineed return -1"""
        if evts and cores:
            return float(evts) / float(cores)
        return -1.

    func_rate = udf(rate, DoubleType())

    # our output
    store = {}

    # conditions

    # here we split dataframe based on exitcode conditions to reduce dimentionality
    # of the input, otherwise job crashes with Integer.MAX_VALUE exception which
    # basically tells that input dataframe exceed number of available partitions
    for ecode in [0, 1]:
        if ecode == 0:
            refdf = final_df.where(_col('ExitCode') == 0)
            condf = condor_df.where(_col('data.ExitCode') == 0)
        else:
            refdf = final_df.where(_col('ExitCode') != 0)
            condf = condor_df.where(_col('data.ExitCode') != 0)
        refdf.persist(StorageLevel.MEMORY_AND_DISK)
        condf.persist(StorageLevel.MEMORY_AND_DISK)

        # aggregate CMS datasets
        cols = ['data.DESIRED_CMSDataset', 'data.CRAB_UserHN', 'data.ExitCode', 'data.Type', 'data.TaskType',
                'data.RecordTime']
        xdf = condf.groupBy(cols) \
            .agg(_sum('data.KEvents').alias('sum_evts'), _sum('data.CoreHr').alias('sum_chr')) \
            .withColumn('date', _lit(date)) \
            .withColumn('rate', func_rate(_col('sum_evts'), _col('sum_chr'))) \
            .withColumn("tier", _split(_col('DESIRED_CMSDataset'), "/").alias('tier').getItem(3)) \
            .withColumnRenamed('CRAB_UserHN', 'user') \
            .withColumnRenamed('RecordTime', 'rec_time') \
            .withColumnRenamed('DESIRED_CMSDataset', 'dataset')
        store.setdefault('dataset', []).append(xdf)

        # aggregate across campaign
        cols = ['data.Campaign', 'data.CRAB_UserHN', 'data.ExitCode', 'data.Type', 'data.TaskType', 'data.RecordTime']
        xdf = condf.groupBy(cols) \
            .agg(_sum('data.KEvents').alias('sum_evts'), _sum('data.CoreHr').alias('sum_chr')) \
            .withColumn('date', _lit(date)) \
            .withColumn('rate', func_rate(_col('sum_evts'), _col('sum_chr'))) \
            .withColumnRenamed('CRAB_UserHN', 'user') \
            .withColumnRenamed('RecordTime', 'rec_time') \
            .withColumnRenamed('Campaign', 'campaign')
        store.setdefault('campaign', []).append(xdf)

        # aggregate across DBS releases
        cols = ['r_release_version', 'CRAB_UserHN', 'ExitCode', 'Type', 'TaskType', 'RecordTime']
        xdf = refdf.groupBy(cols) \
            .agg(_sum('KEvents').alias('sum_evts'), _sum('CoreHr').alias('sum_chr')) \
            .withColumn('date', _lit(date)) \
            .withColumn('rate', func_rate(_col('sum_evts'), _col('sum_chr'))) \
            .withColumnRenamed('CRAB_UserHN', 'user') \
            .withColumnRenamed('RecordTime', 'rec_time') \
            .withColumnRenamed('r_release_version', 'release')
        store.setdefault('release', []).append(xdf)

        # aggregate across DBS eras
        cols = ['acquisition_era_name', 'CRAB_UserHN', 'ExitCode', 'Type', 'TaskType', 'RecordTime']
        xdf = refdf.groupBy(cols) \
            .agg(_sum('KEvents').alias('sum_evts'), _sum('CoreHr').alias('sum_chr')) \
            .withColumn('date', _lit(date)) \
            .withColumn('rate', func_rate(_col('sum_evts'), _col('sum_chr'))) \
            .withColumnRenamed('CRAB_UserHN', 'user') \
            .withColumnRenamed('RecordTime', 'rec_time') \
            .withColumnRenamed('acquisition_era_name', 'era')
        store.setdefault('era', []).append(xdf)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if fout:
        year, month, day = split_date(date)
        for col in store.keys():
            out = '%s/%s/%s/%s/%s' % (fout, col, year, month, day)
            print("output: %s" % out)
            odf = union_all(store[col])
            try:
                print("%s rows: %s" % (col, odf.count()))
                print_rows(odf, col, verbose=1)
                odf.write.format("com.databricks.spark.csv") \
                    .option("header", "true").save(out)
            except Exception as exp:
                print("FAIL to write %s, exception %s" % (out, str(exp)))

    ctx.stop()


@info
@click.command()
@c.common_options(c.ARG_DATE, c.ARG_YARN, c.ARG_FOUT, c.ARG_VERBOSE)
# Custom options
@click.option("--inst", default="global", help="DBS instance on HDFS: global (default), phys01, phys02, phys03")
def main(date, yarn, fout, verbose, inst):
    """Main function"""
    click.echo('dbs_condor')
    click.echo(f'Input Arguments: date:{date}, yarn:{yarn}, fout:{fout}, verbose:{verbose}, inst:{inst}')
    if inst in ['global', 'phys01', 'phys02', 'phys03']:
        inst = inst.upper()
    else:
        raise Exception('Unsupported DBS instance "%s"' % inst)
    run(date, fout, yarn, verbose, inst)


if __name__ == '__main__':
    main()
