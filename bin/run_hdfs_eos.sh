#!/bin/bash
# With this environment, It works in lxplus7, it can requiere modifications to run elsewhere.
source /cvmfs/sft.cern.ch/lcg/views/LCG_94a/x86_64-centos7-gcc8-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh analytix
currentDir=$(
  cd $(dirname "$0")
  pwd
)
export PYTHONPATH=$PYTHONPATH:"$currentDir/../src/python"
if ! klist -s
then
    echo "There is not valid ticket yet"
    kinit
fi
spark-submit  --master yarn \
--conf spark.driver.extraClassPath='/eos/project/s/swan/public/hadoop-mapreduce-client-core-2.6.0-cdh5.7.6.jar' \
--conf spark.executor.memory=4g --conf spark.executor.instances=60 --conf spark.driver.memory=2g \
"$currentDir/../src/python/CMSSpark/dbs_hdfs_eos.py" "$@"
