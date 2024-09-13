#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : wma_agent_count_to_opensearch.py
Author      : Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : Sends historical hourly aggregated WMArchive data to os-cms test tenant for debugging purpose
              Hourly aggregated agent count per host with following fields:
              "wmats_midday": aggregation is daily, so we use midday timestamp of wmats
              "day": string yyyy-mm-dd
              "host": WMA agent
              "count": count of "['day', 'host']" aggregation, number of wmarchive input(job) per host per day
              "avg_steps_count": average step cont for that day for the agent
              "sites": collected set of sites that host reported

Requirements:
    - opensearch-py~=2.1
    - SWAN: !pip install --user opensearch-py~=2.1
"""
import time
from datetime import timezone

import click as click
import pandas as pd
from pyspark.sql.functions import (
    avg as _avg, col, collect_set as _collect_set, count as _count, first as _first, from_unixtime, lit,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from CMSSpark.osearch import osearch
from CMSSpark.spark_utils import get_spark_session, get_candidate_files

pd.options.display.float_format = "{:,.2f}".format
pd.set_option("display.max_colwidth", None)

# global variables
_DEFAULT_HDFS_FOLDER = "/project/monitoring/archive/wmarchive/raw/metric"
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]


def get_rdd_schema():
    """Final schema of steps"""
    return StructType(
        [
            StructField('ts', LongType(), nullable=False),
            StructField('wmats', LongType(), nullable=False),
            StructField('site', StringType(), nullable=True),
            StructField('host', StringType(), nullable=False),
            StructField('number_of_steps', IntegerType(), nullable=False),
            StructField('wmaid', StringType(), nullable=False),
        ]
    )


def udf_step_extract(row):
    """
    Borrowed from wmarchive.py

    Helper function to extract useful data from WMArchive records. Returns list of step_res
    """
    wmaid, wmats, host, ts = row['wmaid'], row['wmats'], row['meta_data']['host'], row['meta_data']['ts']
    site_name, count = "UNKNOWN", 0

    result = {'host': host, 'ts': ts, 'wmaid': wmaid, 'wmats': wmats}
    if 'steps' in row and row['steps']:
        for step in row['steps']:
            count += 1
            if step['site'] is not None:
                site_name = step['site']
    result['site'] = site_name
    result['number_of_steps'] = count
    return [result]


def get_index_schema():
    """
    Creates mapping dictionary for the unified-logs monthly index
    """
    # !PLEASE! Modify # of shards and replicas according to your data size.
    #             For 30GB, 1 shard 1 replica can be enough.
    return {
        "settings": {"index": {"number_of_shards": "2", "number_of_replicas": "1"}},
        "mappings": {
            "properties": {
                "wmats_midday": {"format": "epoch_second", "type": "date"},
                "day": {"ignore_above": 32, "type": "keyword"},
                "host": {"ignore_above": 256, "type": "keyword"},
                "count": {"type": "long"},
                "avg_steps_count": {"type": "long"},
                "sites": {"type": "text"},
            }
        }
    }


def drop_nulls_in_dict(d):  # d: dict
    """Drops the dict key if the value is None

    ES mapping does not allow None values and drops the document completely.
    """
    return {k: v for k, v in d.items() if v is not None}  # dict


def send(part, opensearch_host, es_secret_file, es_index_template):
    """Send given data to OpenSearch"""
    client = osearch.get_es_client(opensearch_host, es_secret_file, get_index_schema())
    # Monthly index format: index_mod="M"
    idx = client.get_or_create_index(timestamp=time.time(), index_template=es_index_template, index_mod="M")
    client.send(idx, part, metadata=None, batch_size=10000, drop_nulls=False)


@click.command()
@click.option("--start_date", required=False, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--end_date", required=False, type=click.DateTime(_VALID_DATE_FORMATS))
@click.option('--es_host', required=True, default=None, type=str,
              help='OpenSearch host name without port: os-cms.cern.ch/os')
@click.option('--es_secret_file', required=True, default=None, type=str,
              help='OpenSearch secret file that contains "user:pass" only')
@click.option('--es_index', required=True, default=None, type=str,
              help='OpenSearch index template (prefix), i.e.: "test-wmarchive-agent-count"')
def main(start_date, end_date, es_host, es_secret_file, es_index):
    click.echo(f'input args: start_date:{start_date}, end_date:{end_date}, es_host:{es_host}, es_index:{es_index}')
    spark = get_spark_session(app_name='cmsmonit-wma-agent-count')
    start_date = start_date.replace(tzinfo=timezone.utc)
    end_date = end_date.replace(tzinfo=timezone.utc)
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df_raw = (
        spark.read.option("basePath", _DEFAULT_HDFS_FOLDER).json(
            get_candidate_files(start_date, end_date, spark, base=_DEFAULT_HDFS_FOLDER, day_delta=2)
        )
        .select(["data.*", "metadata.timestamp"])
        .filter(f"""data.wmats >= {start_date.timestamp()} AND data.wmats < {end_date.timestamp()} """)
    )

    df_rdd = df_raw.rdd.flatMap(lambda r: udf_step_extract(r))
    df = spark.createDataFrame(df_rdd, schema=get_rdd_schema()).dropDuplicates(['wmaid'])
    df = df.withColumn('day', from_unixtime(col('wmats'), 'yyyy-MM-dd'))
    df = df.groupby(['day', 'host']).agg(
        _count(lit(1)).alias('count'),  # no duplicate wmaid
        _avg(col('number_of_steps')).alias('avg_steps_count'),
        _collect_set(col('site')).alias('sites'),
        (_first(col('wmats')) - (_first(col('wmats')) % 86400) + 43200).alias('wmats_midday')
    )
    for part in df.rdd.mapPartitions(lambda p: [[drop_nulls_in_dict(x.asDict()) for x in p]]).toLocalIterator():
        part_size = len(part)
        print(f"Length of partition: {part_size}")
        send(part, opensearch_host=es_host, es_secret_file=es_secret_file, es_index_template=es_index)


if __name__ == "__main__":
    main()
