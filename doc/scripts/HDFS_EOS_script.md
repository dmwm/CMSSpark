# HDFS EOS script

In order to use the EOS HDFS data to generate data popularity data, we create an intermediate data source in parquet format. This script generates that intermediate dataset and allow us to query it to generate the report by application/dataset and by file/dataset. 
Location of the script: [src/python/CMSSpark/dbs_hdfs_eos.py](https://github.com/dmwm/CMSSpark/blob/master/src/python/CMSSpark/dbs_hdfs_eos.py)

The script does the following steps:
- reads json.gz files from HDFS (path: ```hdfs:///project/monitoring/archive/eos/logs/reports/cms/YYYY/MM/DD``` before 2020, and ```hdfs:///project/monitoring/archive/eos-report/logs/cms/YYYY/MM/DD``` after 2020), written by the EOS CERN-IT group;
- creates an intermediate parquet file at ```hdfs///cms/eos/full.parquet```, uniforming the different schemas in the input files (different schemas have been used at different times, due to historical reasons). The parquet file contains therefore data for all days and it's the base for creating the periodic reports;
- create reports from parquet, in csv and png form.

BE CAREFUL: if you are testing, please consider changing the intermediate parquet file [here](https://github.com/dmwm/CMSSpark/blob/master/src/python/CMSSpark/dbs_hdfs_eos.py), just in case you mess it up.

## How to run it

You can run any command with the --help option to get a description of subcomands/parameters. 

### To append a new day to the data source:

We can run the update command to add one specific date:

```bash
 bin/run_hdfs_eos.sh run_update "2019/09/19"
```

or we can use glob expressions to add more than one day at the time: 

```bash
bin/run_hdfs_eos.sh run_update "2019/09/1[0-9]"
```

If there are some changes in the data for a given date, we can override the partition: 

```bash
bin/run_hdfs_eos.sh run_update --mode overwrite "2019/09/1[0-9]"
```

### To create a report for a given period

Image and summary of totals by dataset:

```bash
bin/run_hdfs_eos.sh run_report_totals 20180101 20190101
```

This will create two files:

- `dataset_totals.csv`:  CSV file with the totals for the given period. [d_dataset, application, nevents, total_rb, total_wb, total_rt, total_wt, data_tier]
- `top_total_rb_<start>_<end>.png`: png image with the top 10 datasets by read bytes. 

Summary by file by day:

```bash
bin/run_hdfs_eos.sh get_filenames_per_day 20190101 20190103
```

- This will create a full report at filename/day granularity, it should only be use with short periods as it will result in a big file. [d_dataset, file_lfn,application, nevents, total_rb, total_wb, total_rt, total_wt, data_tier]

## Defaults

The default location for the parquet intermediate file will be: `hdfs:///cms/eos/full.parquet` and the default mode will be append, so it can add new partitions to the existing data without affecting it.

 *The override mode will use dynamic override mode*, affecting only the matched partitions. 

## Intermediate dataset

The generated intermediate parquet dataset will be partitioned by day and will have the following fields:

```reStructuredText
 |-- raw: string (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- rb_max: long (nullable = true)
 |-- session: string (nullable = true)
 |-- file_lfn: string (nullable = true)
 |-- application: string (nullable = true)
 |-- rt: long (nullable = true)
 |-- wt: long (nullable = true)
 |-- rb: long (nullable = true)
 |-- wb: long (nullable = true)
 |-- cts: long (nullable = true)
 |-- csize: long (nullable = true)
 |-- user: string (nullable = true)
 |-- user_dn: string (nullable = true)
 |-- day: integer (nullable = true)
```



## Requirements

This script uses EOS reports data from the ``analytix` cluster. They should be available at `hdfs:///project/monitoring/archive/eos/logs/reports/cms`

### How to run the script without LCG release environment

This instructions are not necessary in a lxplus-like environment.

All python packages are already available at the [LCG release]( http://lcginfo.cern.ch/release/96python3/ ). If you want to run it without an LCG environment, you will need this python 3 and this packages:

- pandas
- pyspark (2.4.x)
- matplotlib
- seaborn
- click

And you will need to setup the [environment for hadoop]( https://cern.service-now.com/service-portal/article.do?n=KB0004426 ). 

## Schema history

The schema changed twice within 2019 (details can be found in src/CMSSpark/spark_utils.py). 

In 2020 the EOS team anomymised sensitive GDPR information and all data are now in the new format:

```
root
 |-- data: struct (nullable = true)
 |    |-- csize: long (nullable = true)
 |    |-- ctms: long (nullable = true)
 |    |-- cts: long (nullable = true)
 |    |-- delete_on_close: long (nullable = true)
 |    |-- fid: long (nullable = true)
 |    |-- fsid: long (nullable = true)
 |    |-- fstpath: string (nullable = true)
 |    |-- host: string (nullable = true)
 |    |-- lid: long (nullable = true)
 |    |-- log: string (nullable = true)
 |    |-- nbwds: long (nullable = true)
 |    |-- nfwds: long (nullable = true)
 |    |-- nrc: long (nullable = true)
 |    |-- nwc: long (nullable = true)
 |    |-- nxlbwds: long (nullable = true)
 |    |-- nxlfwds: long (nullable = true)
 |    |-- osize: long (nullable = true)
 |    |-- otms: long (nullable = true)
 |    |-- ots: long (nullable = true)
 |    |-- path: string (nullable = true)
 |    |-- rb: long (nullable = true)
 |    |-- rb_max: long (nullable = true)
 |    |-- rb_min: long (nullable = true)
 |    |-- rb_sigma: double (nullable = true)
 |    |-- rc_max: long (nullable = true)
 |    |-- rc_min: long (nullable = true)
 |    |-- rc_sigma: double (nullable = true)
 |    |-- rc_sum: long (nullable = true)
 |    |-- rgid: long (nullable = true)
 |    |-- rs_op: long (nullable = true)
 |    |-- rsb_max: long (nullable = true)
 |    |-- rsb_min: long (nullable = true)
 |    |-- rsb_sigma: double (nullable = true)
 |    |-- rsb_sum: long (nullable = true)
 |    |-- rt: double (nullable = true)
 |    |-- ruid: string (nullable = true)
 |    |-- rv_op: long (nullable = true)
 |    |-- rvb_max: double (nullable = true)
 |    |-- rvb_min: long (nullable = true)
 |    |-- rvb_sigma: double (nullable = true)
 |    |-- rvb_sum: long (nullable = true)
 |    |-- rvt: double (nullable = true)
 |    |-- sbwdb: long (nullable = true)
 |    |-- sec.app: string (nullable = true)
 |    |-- sec.grps: string (nullable = true)
 |    |-- sec.host: string (nullable = true)
 |    |-- sec.info: string (nullable = true)
 |    |-- sec.name: string (nullable = true)
 |    |-- sec.prot: string (nullable = true)
 |    |-- sec.role: string (nullable = true)
 |    |-- sec.vorg: string (nullable = true)
 |    |-- sfwdb: long (nullable = true)
 |    |-- sxlbwdb: long (nullable = true)
 |    |-- sxlfwdb: long (nullable = true)
 |    |-- td: string (nullable = true)
 |    |-- tpc.dst: string (nullable = true)
 |    |-- tpc.src_lfn: string (nullable = true)
 |    |-- wb: long (nullable = true)
 |    |-- wb_max: long (nullable = true)
 |    |-- wb_min: long (nullable = true)
 |    |-- wb_sigma: double (nullable = true)
 |    |-- wt: double (nullable = true)
 |-- metadata: struct (nullable = true)
 |    |-- _id: string (nullable = true)
 |    |-- hostname: string (nullable = true)
 |    |-- json: string (nullable = true)
 |    |-- kafka_timestamp: long (nullable = true)
 |    |-- partition: string (nullable = true)
 |    |-- producer: string (nullable = true)
 |    |-- timestamp: long (nullable = true)
 |    |-- topic: string (nullable = true)
 |    |-- type: string (nullable = true)
 |    |-- type_prefix: string (nullable = true)
 ```
