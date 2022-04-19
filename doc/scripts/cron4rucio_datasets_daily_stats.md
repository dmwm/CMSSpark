## CMSSpark/bin/cron4rucio_datasets_daily_stats.sh

Dumps sqoop tables to temporary hdfs folder and then sends Spark job results to MONIT.

To run in LxPlus, for the time that this script is created, you need below settings:

```shell

# Before running Sqoop and spark job, set analytix cluster settings, use spark3
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix 3.2 spark3


# Below operations are required to run this script in LxPlus environment. 
# In K8s, we don't need since docker image provides all below.

# LCG_101 release setup gives py3.6 py3.9 driver-worker incompatibility error.
#   Virtual environment will be used until the issue is solved in upstream
#   Do not run `source /cvmfs/sft.cern.ch/lcg/views/LCG_101/x86_64-centos7-gcc8-opt/setup.sh`
/cvmfs/sft.cern.ch/lcg/releases/Python/3.6.5-f74f0/x86_64-centos7-gcc8-opt/bin/python3.6 -m venv venv_pyspark
source venv_pyspark/bin/activate
pip install --upgrade pip
pip install --no-cache-dir pandas click pyspark

# Create zip file of stomp.py 7.0.0
pip install --no-cache-dir -t stomp-v700 https://github.com/jasonrbriggs/stomp.py/archive/refs/tags/v7.0.0.zip
cd stomp-v700
zip -r ../stomp-v700.zip .
cd ..
rm -rf stomp-v700

# Zip file and directory of CMSMonitoring
svn export https://github.com/mrceyhun/CMSMonitoring.git/branches/f-stomp-v6+/src/python/CMSMonitoring
zip -r CMSMonitoring.zip CMSMonitoring/*
rm -rf CMSMonitoring

```
