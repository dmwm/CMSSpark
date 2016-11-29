# dbs_spark

Set of spark scripts to parse and extract useful aggregated info from DBS dump
on HDFS. For example, we can get statistics for RAW data-tier as following:

```
nohup ./dbs_spark --fout=raw_datasets.csv --tier=RAW --yarn 2>&1 &>raw.log < /dev/null
```

This command will produce raw_datasets.csv file with all RAW datasets as well
as printout total number of events and dataset sizes.
