# System modules
import os
import time

# pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.functions import lit, sum, count, col, split
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# CMSSpark modules
from CMSSpark.spark_utils import phedex_tables, print_rows
from CMSSpark.spark_utils import spark_context, split_dataset
from CMSSpark.utils import info_save, split_date
from CMSSpark.conf import OptionParser

PHEDEX_TIME_DATA_FILE = 'spark_exec_time_tier_phedex.txt'

def get_options():
    opts = OptionParser('PhEDEx')
    return opts.parser.parse_args()

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def get_destination_dir():
    return '%s/../../../bash/report_tiers' % get_script_dir()

def site_filter(site):
    "Filter site names without _MSS or _Buffer or _Export"
    if site.endswith('_MSS') or site.endswith('_Buffer') or site.endswith('_Export'):
        return 0
    return 1

def unix2human(tstamp):
    "Convert unix time stamp into human readable format"
    return time.strftime('%Y%m%d', time.gmtime(tstamp))

def quiet_logs(sc):
    """
    Sets logger's level to ERROR so INFO logs would not show up.
    """
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def run(date, fout, yarn=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    # define spark context, it's main object which allow to communicate with spark
    ctx = spark_context('cms', yarn, verbose)

    quiet_logs(ctx)
    
    sqlContext = HiveContext(ctx)

    fromdate = '%s-%s-%s' % (date[:4], date[4:6], date[6:])
    todate = fromdate
    # read Phedex tables
    tables = {}
    tables.update(phedex_tables(sqlContext, verbose=verbose, fromdate=fromdate, todate=todate))
    phedex_df = tables['phedex_df']

    # register user defined function
    unix2date = udf(unix2human, StringType())
    siteFilter = udf(site_filter, IntegerType())

    # aggregate phedex info into dataframe
    cols = ['node_name', 'dataset_name', 'block_bytes', 'replica_time_create', 'br_user_group_id']
    pdf = phedex_df.select(cols).where(siteFilter(col('node_name')) == 1)\
            .groupBy(['node_name', 'dataset_name', 'replica_time_create', 'br_user_group_id'])\
            .agg({'block_bytes':'sum'})\
            .withColumn('date', lit(date))\
            .withColumn('replica_date', unix2date(col('replica_time_create')))\
            .withColumnRenamed('sum(block_bytes)', 'size')\
            .withColumnRenamed('dataset_name', 'dataset')\
            .withColumnRenamed('node_name', 'site')\
            .withColumnRenamed('br_user_group_id', 'groupid')
    pdf.registerTempTable('pdf')
    pdf.persist(StorageLevel.MEMORY_AND_DISK)

    # write out results back to HDFS, the fout parameter defines area on HDFS
    # it is either absolute path or area under /user/USERNAME
    if  fout:
        cols = ['date','site','dataset','size','replica_date', 'groupid']
        # don't write header since when we'll read back the data it will
        # mismatch the data types, i.e. headers are string and rows
        # may be different data types
        pdf.select(cols)\
            .write.format("com.databricks.spark.csv")\
            .option("header", "true").save(fout)

    ctx.stop()

@info_save('%s/%s' % (get_destination_dir(), PHEDEX_TIME_DATA_FILE))
def main():
    "Main function"
    opts = get_options()
    print("Input arguments: %s" % opts)
    
    fout = opts.fout
    date = opts.date
    verbose = opts.verbose
    yarn = opts.yarn

    run(date, fout, yarn, verbose)

if __name__ == '__main__':
    main()
