#!/usr/bin/env python
# coding: utf-8

# system modules
import re    
import hashlib
from functools import reduce

# third-party libs
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame, SparkSession

# CMSSpark options
from CMSSpark.spark_utils import spark_context, condor_tables, split_dataset
from CMSSpark.utils import info, split_date
from CMSSpark.conf import OptionParser

# Define cleanup methods
def hash_match(matchobj):
    match = matchobj.group(0)
    m = hashlib.md5()
    m.update(match.encode('utf-8'))
    return m.hexdigest()

def hash_match_partial(matchobj):
    match = matchobj.group(2)
    m = hashlib.md5()
    m.update(match.encode('utf-8'))
    return matchobj.group(1) + m.hexdigest()

def hash_private_info(message):
    # Hash DNs
    message = re.sub('(/DC=|/CN=|/OU=).*(?=(/DC=|/CN=|/OU=))(/DC=|/CN=|/OU=)\S*', hash_match, message)
    # Hash DNs without slashes
    message = re.sub('(DC=|CN=|OU=).*(?=(DC=|CN=|OU=))(DC=|CN=|OU=)\S*', hash_match, message)
    # Hash mail addresses
    message = re.sub('[\w\.-]+@[\w\.-]+(?:\.[\w]+)+', hash_match, message)
    # Hash IPs
    message = re.sub('[0-9]{1,3}(\.[0-9]{1,3}){3}', hash_match, message)
    # Hash /user/{username} directories
    message = re.sub('(/user/)(\w*)', hash_match_partial, message)
    # Hash /user={username} 
    message = re.sub('(user\=)(\w*)', hash_match_partial, message)
    # Hash user.{username} .
    message = re.sub('(user\.)(\w*)', hash_match_partial, message)
    # Hash bearer tokens
    message = re.sub('(Bearer\s*)([^\s]*)', hash_match_partial, message)
    # Hash macaroons
    message = re.sub('(macaroon\s*)([^\s]*)', hash_match_partial, message)

    return message

def run(fin, attrs, fout):
    # Setting up the Spark session
    spark = SparkSession.builder.master("local[*]").appName("Issues").getOrCreate()

    # Reading all the files in a directory
    paths = [fin]
    res = spark.read.json(paths)

    data = res.select("data.*")

    anonymize = udf(hash_private_info, returnType=StringType())

    # Use the above udf to anonymize data
    for attr in attrs:
        col = attr+'_hash'
        data = data.withColumn(col, anonymize(getattr(data, attr)))
#        data = data.withColumn(col, anonymize(data.user_dn))

    # drop user_dn
    data = reduce(DataFrame.drop, attrs, data)
#    data = reduce(DataFrame.drop, ['user_dn'], data)

    # Save to csv
    data.write.csv(fout)
    data.head()

def main():
    optmgr  = OptionParser('hdfs_app')
    msg = 'HDFS path to process'
#    optmgr.parser.add_argument("--fin", action="store",
#        dest="fin", default="", help=msg)
#    msg = 'HDFS path to write results to'
#    optmgr.parser.add_argument("--fout", action="store",
#        dest="fout", default="", help=msg)
    msg = 'Comma separated list of attributes to anonimise'
    optmgr.parser.add_argument("--attrs", action="store",
        dest="attrs", default="", help=msg)
    opts = optmgr.parser.parse_args()
    attrs = opts.attrs.split(',')
    run(opts.hdir, attrs, opts.fout)

if __name__ == '__main__':
    main()
