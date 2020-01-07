# HDFS CRAB script

We use condor data in HDFS to generate statistics about data use in CRAB. We use the following attributes from the condor documents for completed jobs: 

- GlobalJobId - To identify the unique jobs
- RecordTime - To filter the job reports and to get the earliest and latest use date for a given datablock
- Status - To filter for only the completed/removed jobs
- ChirpCMSSWReadBytes - A proxy variable for the read bytes of the datablock
- CRAB_Workflow - To identify unique tasks
- CRAB_DataBlock - The primary datablock used in a job
- CMSPrimaryPrimaryDataset - The primary dataset used in a job

And produce a dataset with: 

- Datatier, generated from the datablock name. 
- PrimaryDataset (CMSPrimaryPrimaryDataset)
- Dataset, generated from the datablock name. 
- max(RecordTime)
- min(RecordTime)
  job_count: count of distinct completed or removed jobs. 
- workflow_count: count of distinct CRAB_Workflows. 
- sum(ChirpCMSSWReadBytes)

## Some context

CRAB sets the CRAB_DataBlock classad to the condor jobs. Using the cms-htcondor-spider and the MonIT flow that data is stored in HDFS as Json.

We also have the CMSPrimaryPrimaryDataset for all the jobs (depending on the submission tool). 
![Condor data flow](img/CondorJobsDataMonit.png)

The json documents have a flexible schema and currently they have more than 1000 different fields. We also have more than 300GB/day in data (Monit has a compaction method, so data older than a day is deduplicated and compressed, making the data weight around 80GB/day). 

## How to run it

You can run the python script directly, but it will require that you setup the environment first. The run_hdfs_crab.sh script will setup the environment in a lxplus-like machine and will run the python script. 

```
Usage: run_hdfs_crab.sh [OPTIONS] [%Y/%m/%d|%Y-%m-%d|%Y%m%d]
                        [%Y/%m/%d|%Y-%m-%d|%Y%m%d]

Options:
  --generate_plots      Additional to the csv, generate the plot(s)
  --output_folder TEXT  local output directory
  --help                Show this message and exit.

```

E.g. 

```bash
/bin/bash bin/run_hdfs_crab.sh --generate_plots --output_folder "./output" "2018-01-01" "2019-01-01"
```

This will generate a csv file `CRAB_popularity_20180101-20190101.csv` and a png file `CRAB_popularity_20180101-20190101_top_jc.png` with the top datablocks by job count. 

## Requirements

This script uses Condor data from  `analytix` cluster. They should be available at `/project/monitoring/archive/condor/raw/metric/`

### How to run the script without LCG release environment

This instructions are not necessary in a lxplus-like environment.

All python packages are already available at the [LCG release]( http://lcginfo.cern.ch/release/96python3/ ). If you want to run it without an LCG environment, you will need this python 3 and this packages:

- pandas
- pyspark (2.4.x)
- matplotlib
- seaborn
- click

And you will need to setup the [environment for hadoop]( https://cern.service-now.com/service-portal/article.do?n=KB0004426 ). 

