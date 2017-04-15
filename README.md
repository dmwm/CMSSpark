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
