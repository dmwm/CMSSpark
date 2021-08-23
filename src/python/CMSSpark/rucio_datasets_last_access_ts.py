#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
"""Get last access timeS of datasets by joining Rucio's REPLICAS, DIDS and CONTENTS tables"""

import pandas as pd
import pickle
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    hex as _hex,
    lower,
    max as _max,
    when,
)

pd.options.display.float_format = "{:,.2f}".format
pd.set_option("display.max_colwidth", None)

HDFS_RUCIO_CONTENTS = "/project/awg/cms/rucio_contents/*/part*.avro"
HDFS_RUCIO_DIDS = "/project/awg/cms/rucio_dids/*/part*.avro"
HDFS_RUCIO_REPLICAS = f"/project/awg/cms/rucio/{datetime.today().strftime('%Y-%m-%d')}/replicas/part*.avro"


def get_ts_thresholds():
    """Returns unix timestamps of 3, 6 and 12 months ago"""
    timestamps = {}
    for num_month in [3, 6, 12]:
        dt = datetime.today() + relativedelta(months=-num_month)
        timestamps[num_month] = int(datetime(dt.year, dt.month, dt.day).timestamp()) * 1000
    return timestamps


def get_disk_rse_ids():
    """Get rse:rse_id map from pickle file

    TODO: Get rse:rse_id map via Rucio python library. I could not run Rucio python library unfortunately.


    Used code in LxPlus (author: David Lange):
    ```py
#!/usr/bin/env python
from subprocess import Popen,PIPE
import os,sys,pickle
def runCommand(comm):
    p = Popen(comm,stdout=PIPE,stderr=PIPE,shell=True)
    pipe=p.stdout.read()
    errpipe=p.stderr.read()
    tupleP=os.waitpid(p.pid,0)
    eC=tupleP[1]
    return eC,pipe.decode(encoding='UTF-8'),errpipe.decode(encoding='UTF-8')
comm="rucio list-rses"
ec,cOut,cErr = runCommand(comm)
rses={}
for l in cOut.split():
    rse=str(l.strip())
    print(rse)
    comm="rucio-admin rse info "+rse
    ec2,cOut2,cErr2 = runCommand(comm)
    id=None
    for l2 in cOut2.split('\n'):
        if "id: " in l2:
            id=l2.split()[1]
            break
    print(id)
    rses[rse]=id
with open("rses.pickle", "wb+") as f:
  pickle.dump(rses, f)
    ```
    """
    with open("rses.pickle", "rb+") as f:
        rses = pickle.load(f)
    return list(
        dict(
            [(k, v) for k, v in rses.items() if not any(tmp in k for tmp in ["Tape", "Test", "Temp"])]
        ).values()
    )


def get_spark_session(yarn=True, verbose=False):
    """
    Get or create the spark context and session.
    """
    sc = SparkContext(appName="cms-monitoring-rucio-last_access-ts")
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def main():
    """
    TODO: Send resulting datasets in threshold groups with their sizes to ElasticSearch or Prometheus ..
    TODO: .. because resulting number of datasets are ~700K. It cannot be served in html table.

    Logic:
        - Get all files on disk by joining "replicas" and "dids" table.
        - "dids" table contains all files, but "replicas" table contains only files on disk.
        - After finding files on disk, get the parent datasets of those files by joining "contents" table.
        - "contents" table provides parent dataset of a file.
        - Dumped "contents" table in HDFS includes only `did_type='D' AND child_type='F'`
        - Then group-by by "dataset" and calculate last accessed_at of dataset by getting "max" accessed_at of files
        which belongs to same dataset.
        - Now we have last access time of dataset which is extracted from its files.
        - Finally, join the result with df_dids_datasets to get size of each dataset from "dids" table.

    Access time filter logic:
        - If "last_access_ts" is less than 3 months ago, then set "months_old" as 3,
        - If "last_access_ts" is less than 3 monthsa ago, then set "months_old" as 6,
        - If "last_access_ts" is less than 3 months ago, then set "months_old" as 12

    The result includes only the datasets whose last access time is 3 months ago.
    And the dataset whose last access time is less than 12 months or 6 months is NOT included in 3 months flag
    which means a dataset can only 1 flag(3, 6 or 12).

    """
    spark = get_spark_session()
    disk_rse_ids = get_disk_rse_ids()
    df_replicas = spark.read.format("avro").options(inferschema='true') \
        .load(HDFS_RUCIO_REPLICAS) \
        .withColumn("RSE_ID", lower(_hex(col("RSE_ID")))) \
        .filter(col("RSE_ID").isin(disk_rse_ids)) \
        .filter(col("SCOPE") == "cms") \
        .select(["NAME", "RSE_ID"]) \
        .cache()
    df_dids_files = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_DIDS) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "F") \
        .select(["NAME", "ACCESSED_AT", "BYTES"]) \
        .cache()
    df_dids_datasets = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_DIDS) \
        .filter(col("SCOPE") == "cms") \
        .filter(col("DID_TYPE") == "D") \
        .withColumnRenamed("NAME", "DS_NAME") \
        .select(["DS_NAME", "BYTES"]) \
        .cache()
    df_contents = spark.read.format("com.databricks.spark.avro").load(HDFS_RUCIO_CONTENTS) \
        .withColumnRenamed("NAME", "DS_NAME") \
        .withColumnRenamed("CHILD_NAME", "NAME") \
        .select(["DS_NAME", "NAME"]) \
        .cache()
    df_files_on_disk = df_replicas.join(df_dids_files, ["NAME"], "left").filter(col('BYTES').isNotNull()).cache()
    ts_thresholds = get_ts_thresholds()

    # Exclusive. Datasets whose last access time is 12 months ago are not in 6 or 3 months ones.
    df_without_bytes = df_contents.join(df_files_on_disk, ["NAME"]) \
        .groupby(["DS_NAME"]) \
        .agg(_max(col("ACCESSED_AT")).alias("last_access_ts")) \
        .withColumn('last_access_at_least_n_months_ago',
                    when(col('last_access_ts') < ts_thresholds[12], 12)
                    .when(col('last_access_ts') < ts_thresholds[6], 6)
                    .when(col('last_access_ts') < ts_thresholds[3], 3)
                    .otherwise(None)
                    ) \
        .filter(col('last_access_at_least_n_months_ago').isNotNull()) \
        .cache()
    df = df_without_bytes.join(df_dids_datasets, ["DS_NAME"])
    return df
