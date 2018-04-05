#!/bin/bash
source /cvmfs/sft.cern.ch/lcg/views/LCG_93/x86_64-centos7-gcc62-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-setconf.sh analytix
export PATH=$PWD/bin:$PATH
export PYTHONPATH=$PWD/src/python:$PYTHONPATH
