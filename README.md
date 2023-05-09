# CMSSpark test1111111111111111

[![License:MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/vkuznet/LICENSE)
[![DOI](https://zenodo.org/badge/74044584.svg)](https://zenodo.org/badge/latestdoi/74044584)
[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=CMSSpark%20framework%20&url=https://github.com/vkuznet/CMSSpark&hashtags=cms,spark,BigData,lhc,physics)

Set of spark scripts to parse and extract useful aggregated info from various
CMS data streams on HDFS. So far, it supports DBS, PhEDEx, AAA, EOS, CMSSW
data parsing on HDFS.

Here are few examples how to get various stats:

```
# DBS+PhEDEx
apatterns="*BUNNIES*,*Commissioning*,*RelVal*"
hdir=hdfs:///cms/users/vk/datasets
run_spark dbs_phedex.py --fout=$hdir --antipatterns=$apatterns --yarn --verbose

# Send data to CERN MONIT, user must provide stomppy egg and AMQ JSON broker file
run_spark cern_monit.py --hdir=$hdir --stomp=static/stomp.py-4.1.15-py2.7.egg --amq=amq_broker.json

# DBS+CMSSW
run_spark dbs_cmssw.py --verbose --yarn --fout=hdfs:///cms/users/vk/cmssw --date=20170411

# DBS+AAA
run_spark dbs_aaa.py --verbose --yarn --fout=hdfs:///cms/users/vk/aaa --date=20170411

# DBS+EOS
run_spark dbs_eos.py --verbose --yarn --fout=hdfs:///cms/users/vk/eos --date=20170411

# WMArchive examples:
run_spark wmarchive.py --fout=hdfs:///cms/users/vk/wma --date=20170411
run_spark wmarchive.py --fout=hdfs:///cms/users/vk/wma --date=20170411,20170420 --yarn
run_spark wmarchive.py --fout=hdfs:///cms/users/vk/wma --date=20170411-20170420 --yarn
```

*Please note*: in order to run cern_monit.py script user must supply two
additional parameters. The StompAMQ library file and AMQ credentials.
The former is located in static are of this package. The later contains
CERN MONIT end-point parameters and should be individually obtained from CERN
MONIT team. For example

```
run_spark cern_monit.py --hdir=/cms/users/vk/datasets --amq=amq_broker.json --stomp=/path/stomp.py-4.1.15-py2.7.egg
```

### CMS metadata
CMS metadata are stored in the following location on HDFS and accessing from
analytix cluster:

- DBS: /project/awg/cms/CMS_DBS3_PROD_GLOBAL/ (full DB dump in CSV data-format)
- CMSSW: /project/awg/cms/cmssw-popularity (daily snapshots in avro data-format)
- JobMonitoring: /project/awg/cms/jm-data-popularity (daily snapshots in avro data-format)
- JobMonitoring: /project/awg/cms/job-monitoring (daily snapshots in avro data-format)
- PhedexReplicas: /project/awg/cms/phedex/block-replicas-snapshots (daily snapshots in CSV data-format)
- PhedexCatalog: /project/awg/cms/phedex/catalog (daily snapshots in CSV data-format)
- AAA: /project/monitoring/archive/xrootd (daily snapshots in JSON data-format)
- EOS: /project/monitoring/archive/eos (daily snapshots in JSON data-format)
- WMArchive: /cms/wmarchive/avro (daily snapshots in Avro data-format)

### Aggregating data and preparing reports
It is possible to aggregate PhEDEx and DBS data and prepare reports with tables and plots visualizing the data.

#### Initialization
Before running the scripts please run `./src/bash/reports_init` to clone wiki repository which contains reports. If you don't intend to submit reports to github wiki this step is not necessary.

#### Aggregating by data tier
Please run `./src/bash/report_tiers/aggregate` to download PhEDEx and DBS data aggregated by data tier.

Then run `python src/python/CMSSpark/reports/visualize_tiers.py` to generate the report which will be available here: `src/bash/CERNTasks.wiki/CMS_Tier_Reports.md`

Scripts should be ran from this directory: `src/bash/report_tiers`

#### Aggregating by campaign
Please run `./src/bash/report_campaigns/aggregate` download PhEDEx and DBS data aggregated by campaign.

Then run `python src/python/CMSSpark/reports/visualize_campaigns.py` to generate the report which will be available here: `src/bash/CERNTasks.wiki/CMS_Campaign_Reports.md`

Scripts should be ran from this directory: `src/bash/report_campaigns`

#### Aggregating leftover datasets
Please follow instructions [these instructions](README_Leftovers.md).

### Citation
Please use this paper for citation:
[https://arxiv.org/abs/1811.04785](https://arxiv.org/abs/1811.04785)


## Contribution suggestions

- Create virtualenv in the current directory with `venv` name which is in .gitignore: `python3 -m venv ./venv`
- Update pip to latest version: `python3 -m pip install --upgrade pip`
- Run `pip install -r requirements.txt`
- All set.
- Please obey the conventions in other scripts, python import conventions, bash script wrapper script, etc.
- All spark jobs use same Spark/Hadoop/Python versions. If a script requires update, please apply updates to all.
