# HDFS HS06 CPU Time plot

We use condor data in HDFS to generate a  HS06 CPU Time plot in T2 non-CERN sites (by default, the sites to include/exclude can be adjusted via parameters). We use the following attributes from the condor documents for completed jobs: 

- GlobalJobId - To identify the unique jobs
- RecordTime - To filter the job reports and to get the earliest and latest use date for a given datablock
- Status - To filter for only the completed/removed jobs
- HS06CpuTimeHr - The HS06 CPU Time.
- Site - To filter for T2 sites which are not from CERN. 

And produce a dataset with: 

- year
- month/weekofyear: aggregation period.
- sum(HS06CpuTimeHr)

## How to run it

You can run the python script directly, but it will require that you setup the environment first. The `run_hs06cputime_plot.sh` script will setup the environment in a lxplus-like machine and will run the python script. 

```
Usage: condor_hs06coreHrPlot.py [OPTIONS] [%Y/%m/%d|%Y-%m-%d|%Y%m%d]
                                [%Y/%m/%d|%Y-%m-%d|%Y%m%d]

  This script will generate a dataset with the number of unique users of
  CRAB either by month or by weekofyear.

Options:
  --by [weekofyear|month]  Either weekofyear or month  [default: month]
  --include_re TEXT        Regular expression to select the sites to include
                           in the plot  [default: ^T2_.*$]
  --exclude_re TEXT        Regular expression to select the sites to exclude
                           of the plot  [default: .*_CERN.*]
  --generate_plots         Additional to the csv, generate the plot(s)
  --output_folder TEXT     local output directory
  --help                   Show this message and exit.


E.g. 

```bash
/bin/bash bin/run_hs06cputime_plot.sh --generate_plots --output_folder "./output" "2019-01-01" "2020-01-01" --by month
```

This will generate a csv file `HS06CpuTimeHr_month_20191001-20200101.csv` and a png file `HS06CpuTimeHr_month_20191001-20200101.png` with the HS06 CPU Time in Kdays for each month of the year for T2 non CERN sites. 

![img](./img/HS06CpuTimeHr_month_20191001-20200101.png) 


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

