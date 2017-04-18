#!/bin/bash
#source /data/srv/current/apps/wmarchive/etc/profile.d/init.sh
#source /data/srv/current/apps/PhedexReplicaMonitoring/etc/profile.d/init.sh
#export PYTHONPATH=$PYTHONPATH:/usr/lib/spark/python

#export PYTHONUNBUFFERED=1
#export JAVA_JDK_ROOT
#export JAVA_HOME=$JAVA_JDK_ROOT

# GRID
#export X509_USER_PROXY=$STATEDIR/proxy/proxy.cert
#export X509_USER_CERT=$X509_USER_PROXY
#export X509_USER_KEY=$X509_USER_PROXY

apatterns="*BUNNIES*,*Commissioning*,*RelVal*"
# area on HDFS
fout=hdfs:///cms/users/vk/datasets
cmd="run_spark dbs_phedex.py --fout=$fout --antipatterns=$apatterns --yarn --verbose"
hadoop fs -rm -r -f $fout
$cmd
