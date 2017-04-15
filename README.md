# CMSSpark

Set of spark scripts to parse and extract useful aggregated info from various
CMS data streams on HDFS. So far, it supports DBS, PhEDEx, AAA, EOS, CMSSW
data parsing on HDFS.

Here are few examples how to get various stats:

```
# DBS+PhEDEx
dbs_phedex_stats.sh 2>&1 1>& dbs_phedex.log

# DBS+CMSSW
dbs_cmssw_spark --verbose --yarn --fout=hdfs:///cms/users/vk/cmssw --date=20170412 2>&1 &> cmssw.log

# DBS+AAA
dbs_aaa_spark --verbose --yarn --fout=hdfs:///cms/users/vk/aaa --date=20170411 2>&1 &> aaa.log

# DBS+EOS
dbs_eos_spark --verbose --yarn --fout=hdfs:///cms/users/vk/eos --date=20170412 2>&1 &> eos.log
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
