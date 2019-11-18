# HDFS EOS script

In order to use the EOS HDFS data to generate data popularity data, we create an intermediate data source in parquet format. This script generates that intermediate dataset and allow us to query it to generate the report by application/dataset and by file/dataset. 

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

Summary by file by day:

```bash
bin/run_hdfs_eos.sh get_filenames_per_day 20180101 20190101
```

## Defaults

The default location for the parquet intermediate file will be: `hdfs:///cms/eos/full.parquet` and the default mode will be append, so it can add new partitions to the existing data without affecting it.

 *The override mode will use dynamic override mode*, affecting only the matched partitions. 



