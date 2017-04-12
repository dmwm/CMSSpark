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

# DBS data tiers, can be obtained from
# https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatiers
tiers="DQMIO FEVTDEBUGHLT AODSIM AOD USER ALCARECO RECO DIGI SIM GEN RAW RAW-RECO GEN-SIM-RECO GEN-SIM-RAW-RECO GEN-SIM-RAW-HLTDEBUG-RECO GEN-SIM-RAW-HLTDEBUG GEN-SIM-RAW GEN-SIM-DIGI-RECO GEN-SIM-DIGI-RAW-RECO GEN-SIM-DIGI-RAW-HLTDEBUG-RECO GEN-SIM-DIGI-RAW-HLTDEBUG GEN-SIM-DIGI-RAW GEN-SIM-DIGI-HLTDEBUG-RECO GEN-SIM-DIGI-HLTDEBUG GEN-SIM-DIGI GEN-SIM DIGI-RECO GEN-SIM-RAWDEBUG RAWDEBUG DQM ALCAPROMPT GEN-SIM-RECODEBUG RECODEBUG RAW-HLT RAW-HLY HLY GEN-SIM-DIGI-RECODEBUG FEVT HLTDEBUG RAWRECOSIMHLT RAW-RECOSIMHLT RECOSIMHLT LHE GEN-SIM-RAW-HLTDEBUG-RECODEBUG GEN-SIM-RAW-HLT HLT GEN-RAWDEBUG DQMROOT GEN-RAW FEVTHLTALL DAVE CRAP PREMIXRAW RAWAODSIM DBS3_DEPLOYMENT_TEST_TIER MINIAODSIM PREMIX-RAW MINIAOD"
#tiers="AOD MINIAOD GEN RAW RECO USER"
tiers="USER"
#apatterns="*BUNNIES*,*Commissioning*,*RelVal*"
apatterns="*BUNNIES*"
amq=/data/wma/dbs/dbs_spark/phedex_dbs_broker.json
fout=datasets.csv
#cmd="./dbs_phedex_spark --fout=$fout --antipatterns=$apatterns --yarn --verbose --amq=$amq"
cmd="./dbs_phedex_spark --fout=$fout --antipatterns=$apatterns --yarn --verbose"
for tier in $tiers; do
    echo "$cmd --tier=$tier"
    $cmd --tier=$tier 2>&1 1>& dp_${tier}.log
    mv $fout ${fout}.${tier}
done
#./dbs_phedex_spark --help
