#!/bin/bash
set -e
# Build help script for cmsmon-spark docker image

mkdir -p "$WDIR"/logs

# -- Get amtool
curl -ksLO https://github.com/prometheus/alertmanager/releases/download/v0.24.0/alertmanager-0.24.0.linux-amd64.tar.gz
tar xfz alertmanager-0.24.0.linux-amd64.tar.gz && mv alertmanager-0.24.0.linux-amd64/amtool "$WDIR/" && rm -rf alertmanager-0.24.0.linux-amd64*

# -- Install python modules
pip install --no-cache-dir click matplotlib numpy opensearch-py~=2.1 pandas plotly pyspark requests==2.29 schema seaborn stomp.py==7.0.0

echo "Info: CMSSPARK_TAG=${CMSSPARK_TAG} , CMSMON_TAG=${CMSMON_TAG}, HADOOP_CONF_DIR=${HADOOP_CONF_DIR}, PATH=${PATH}, PYTHONPATH=${PYTHONPATH}, PYSPARK_PYTHON=${PYSPARK_PYTHON}, PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON}"
