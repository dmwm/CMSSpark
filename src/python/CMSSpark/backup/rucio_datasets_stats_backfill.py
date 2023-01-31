#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : rucio_datasets_stats_backfill.py
Author      : Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : Fills weekly and monthly indexes using HDFS project/monitoring/archive/cms_rucio/raw/daily_stats.
              No aggregation is applied, only selected day of each week(Thursday) and month(3rd) HDFS data send to
                weekly and monthly indexes.

References:
  - rucio_datasets_stats.py
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
import subprocess

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session

# CMSMonitoring modules
try:
    from CMSMonitoring.StompAMQ7 import StompAMQ7
except ImportError:
    print("ERROR: Could not import StompAMQ")
    sys.exit(1)

# UTC timestamp of start hour of spark job
TSAMP_CURRENT_HOUR = int(datetime.utcnow().replace(minute=0, second=0, tzinfo=timezone.utc).timestamp()) * 1000
BASE_HDFS_READ_PATH = 'hdfs:///project/monitoring/archive/cms_rucio/raw/daily_stats'
LOCAL_DIRECTORY = '/data'
NTH_DAY_OF_MONTH = '03'


def run_cmd(args_list):
    """run linux commands"""
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def get_files_in_daily_directory(spark, hdfs_dir):
    """Returns files of the HDFS directory"""
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfs_dir))
    return [f.getPath().toString() for f in list_status]


def get_weekly_date_tuples(start_date, end_date):
    """Get week tuples (year, month, day)"""
    # We select Tuesday as the start day so each week's Tuesday will be sent
    weeks = []
    for n in range(0, int((end_date - start_date).days), 7):
        d = start_date + timedelta(n)
        weeks.append(d.timetuple()[:3])
    return weeks


def get_monthly_date_tuples(weeks):
    """Get months date tuples (YEAR, MONTH) from weeks"""
    return sorted(list(set([d[:2] for d in weeks])))


# ---------------------------------------------------------------------------------------------------------------------
#                                     Process monthly
# ---------------------------------------------------------------------------------------------------------------------
def get_files_of_nth_day_of_each_month(spark, start_date, end_date):
    """Get files of each month's one day, so returns list(each day) of lists(json.gz files of each day)"""
    # start_date defines the Nth day of each month.
    weeks = get_weekly_date_tuples(start_date, end_date)
    months = get_monthly_date_tuples(weeks)
    files = []
    for month in months:
        # hdfs:///project/monitoring/archive/cms_rucio/raw/daily_stats  /  YEAR  / MONTH  / 03
        directory = BASE_HDFS_READ_PATH + '/' + f'{month[0]:02}' + '/' + f'{month[1]:02}' + '/' + NTH_DAY_OF_MONTH
        print(directory)
        files.append(get_files_in_daily_directory(spark, directory))
    return files


def process_monthly(spark, start_date, end_date, confs, batch_size):
    """Copy HDFS json.gz to local directory, uncompress and send through StompAMQ"""
    topic = "/topic/cms.rucio.monthlystats"
    doc_type = "monthly_stats"
    month_directory_files = get_files_of_nth_day_of_each_month(spark, start_date, end_date)
    for day_directory_files in month_directory_files[1:]:
        print('--------------------------------------------------')
        for file_path_json_gz in day_directory_files:
            # hdfs:///project/.../part-xxx.json.gz -> /local_dir/part-xxx.json.gz
            file_json_gz = LOCAL_DIRECTORY + '/' + file_path_json_gz.split('/')[-1]
            # /local_dir/part-xxx.json.gz -> /local_dir/part-xxx.json
            file_json = file_json_gz[:-3]
            # copy one json to local dir
            (ret, out, err) = run_cmd(['hdfs', 'dfs', '-get', file_path_json_gz, LOCAL_DIRECTORY])
            print(ret, out, err)
            # uncompress json.gz in local dir
            (ret, out, err) = run_cmd(['gzip', '-d', file_json_gz])
            print(ret, out, err)
            # iterate json lines in batches
            with open(file_json) as f:
                counter = 0
                data = []
                for line in f:
                    # Get only data part from {'data': {}, 'metadata': {}}
                    data.append(json.loads(line)['data'])
                    counter += 1
                    if counter == 100000:
                        special_send_to_amq(data=data, confs=confs, batch_size=batch_size, topic=topic,
                                            doc_type=doc_type)
                        counter = 0
                        data = []
            # Delete local json file
            (ret, out, err) = run_cmd(['rm', file_json])
            print(ret, out, err)


# ---------------------------------------------------------------------------------------------------------------------
#                                     Process weekly
# ---------------------------------------------------------------------------------------------------------------------
def get_files_of_day_of_each_week(spark, start_date, end_date):
    """Get files of each week's one day, so returns list(each day) of lists(json.gz files of each day)"""
    # start_date defines the Nth day of each week.
    weeks = get_weekly_date_tuples(start_date, end_date)
    files = []
    for week in weeks:
        directory = BASE_HDFS_READ_PATH + '/' + f'{week[0]:02}' + '/' + f'{week[1]:02}' + '/' + f'{week[2]:02}'
        print(directory)
        files.append(get_files_in_daily_directory(spark, directory))
    return files


def process_weekly(spark, start_date, end_date, confs, batch_size):
    """Copy HDFS json.gz to local directory, uncompress and send through StompAMQ"""
    topic = "/topic/cms.rucio.weeklystats"
    doc_type = "weekly_stats"
    week_directory_files = get_files_of_day_of_each_week(spark, start_date, end_date)
    for day_directory_files in week_directory_files:
        print('--------------------------------------------------')
        for file_path_json_gz in day_directory_files:
            # hdfs:///project/.../part-xxx.json.gz -> /local_dir/part-xxx.json.gz
            file_json_gz = LOCAL_DIRECTORY + '/' + file_path_json_gz.split('/')[-1]
            # /local_dir/part-xxx.json.gz -> /local_dir/part-xxx.json
            file_json = file_json_gz[:-3]
            # copy one json to local dir
            (ret, out, err) = run_cmd(['hdfs', 'dfs', '-get', file_path_json_gz, LOCAL_DIRECTORY])
            print(ret, out, err)
            # uncompress json.gz in local dir
            (ret, out, err) = run_cmd(['gzip', '-d', file_json_gz])
            print(ret, out, err)
            # iterate json lines in batches
            with open(file_json) as f:
                counter = 0
                data = []
                for line in f:
                    # Get only data part from {'data': {}, 'metadata': {}}
                    data.append(json.loads(line)['data'])
                    counter += 1
                    if counter == 200000:
                        special_send_to_amq(data=data, confs=confs, batch_size=batch_size, topic=topic,
                                            doc_type=doc_type)
                        counter = 0
                        data = []
            # Delete local json file
            (ret, out, err) = run_cmd(['rm', file_json])
            print(ret, out, err)


# =====================================================================================================================
#                     Send data with STOMP AMQ
# =====================================================================================================================
def credentials(f_name):
    if os.path.exists(f_name):
        return json.load(open(f_name))
    return {}


def drop_nulls_in_dict(d):  # d: dict
    """Drops the dict key if the value is None
    ES mapping does not allow None values and drops the document completely.
    """
    return {k: v for k, v in d.items() if v is not None}  # dict


def to_chunks(data, samples=1000):
    length = len(data)
    for i in range(0, length, samples):
        yield data[i:i + samples]


def special_send_to_amq(data, confs, batch_size, topic, doc_type):
    """Sends list of dictionary in chunks"""
    wait_seconds = 0.001
    if confs:
        username = confs.get('username', '')
        password = confs.get('password', '')
        producer = confs.get('producer')
        host = confs.get('host')
        port = int(confs.get('port'))
        cert = confs.get('cert', None)
        ckey = confs.get('ckey', None)
        for chunk in to_chunks(data, batch_size):
            # After each stomp_amq.send, we need to reconnect with this way.
            stomp_amq = StompAMQ7(username=username, password=password, producer=producer, topic=topic,
                                  key=ckey, cert=cert, validation_schema=None, host_and_ports=[(host, port)],
                                  loglevel=logging.WARNING)
            messages = []
            for msg in chunk:
                # Set metadata.timestamp as tstamp_hour of the old data
                notif, _, _ = stomp_amq.make_notification(payload=msg, doc_type=doc_type, producer=producer,
                                                          ts=msg['tstamp_hour'])
                messages.append(notif)
            if messages:
                stomp_amq.send(messages)
                time.sleep(wait_seconds)
        time.sleep(1)
        print("Message sending is finished")


def main():
    spark = get_spark_session("cms-monitoring-rucio-daily-stats")
    creds_json = credentials(f_name='/etc/secrets/amq_broker.json')

    process_monthly(spark, start_date=datetime(2022, 8, 3), end_date=datetime(2023, 1, 29), confs=creds_json,
                    batch_size=1000)
    process_weekly(spark, start_date=datetime(2022, 7, 14), end_date=datetime(2023, 1, 29), confs=creds_json,
                   batch_size=1000)

# HOW TO RUN
# -- In K8s, you can use cmsmon-rucio-ds service and run pyspark shell
# spark_submit_args=(
#     --master yarn --conf spark.ui.showConsoleProgress=false --conf spark.sql.session.timeZone=UTC
#     --driver-memory=8g --executor-memory=8g --executor-cores=4 --num-executors=30
#     --conf "spark.driver.bindAddress=0.0.0.0" --conf "spark.driver.host=${MY_NODE_NAME}"
#     --conf "spark.driver.port=31201" --conf "spark.driver.blockManager.port=31202"
#     --packages org.apache.spark:spark-avro_2.12:3.3.1 --py-files "/data/CMSMonitoring.zip,/data/stomp-v700.zip"
# )
#
#  -- run pyspark
# pyspark ${spark_submit_args[@]}
#
# -- paste all code except for main() function and run below
# creds_json = credentials(f_name='/etc/secrets/amq_broker.json')
# process_monthly(spark, start_date=datetime(2022, 8, 3), end_date=datetime(2022, 12, 4), confs=creds_json, batch_size=1000)
# process_weekly(spark, start_date=datetime(2022, 7, 14), end_date=datetime(2022, 12, 30), confs=creds_json, batch_size=1000)
