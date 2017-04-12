#!/bin/bash
#source /data/srv/current/apps/wmarchive/etc/profile.d/init.sh
source /data/srv/current/apps/PhedexReplicaMonitoring/etc/profile.d/init.sh
export PYTHONPATH=$PYTHONPATH:/usr/lib/spark/python

export PYTHONUNBUFFERED=1
export JAVA_JDK_ROOT
export JAVA_HOME=$JAVA_JDK_ROOT

# kerberos
#export PRMKEYTAB=/data/wma/PhedexReplicaMonitoring/prm.keytab
#principal=`klist -k $PRMKEYTAB | tail -1 | awk '{print $2}'`
#echo "klist -k $PRMKEYTAB | tail -1 | awk '{print $2}'"
#kinit $principal -k -t $PRMKEYTAB

# GRID
export X509_USER_PROXY=$STATEDIR/proxy/proxy.cert
export X509_USER_CERT=$X509_USER_PROXY
export X509_USER_KEY=$X509_USER_PROXY

tiers="AOD MINIAOD GEN RAW RECO USER"
#tiers="USER"
apatterns="*BUNNIES*,*Commissioning*,*RelVal*"
hdir=hdfs:///project/awg/cms/CMS_DBS3_PROD_GLOBAL/current
amq=/data/wma/dbs/dbs_spark/phedex_dbs_broker.json
cmd="./dbs_spark --hdir=$hdir --fout=datasets.csv --antipatterns=$apatterns --yarn --verbose --amq=$amq"
#cmd="./dbs_spark --hdir=$hdir --fout=datasets.csv --antipatterns=$apatterns --yarn --verbose"
for tier in $tiers; do
    echo "$cmd --tier=$tier"
    $cmd --tier=$tier 2>&1 1>& ${tier}.log
done
#./dbs_spark --help
