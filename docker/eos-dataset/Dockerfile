# cmsmon-eos-dataset
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:v1.0.0
MAINTAINER Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch 

ENV WDIR=/opt/spark-apps/eos-dataset
ENV OTEL_SERVICE_NAME=eos-dataset
WORKDIR $WDIR

COPY src/ $WDIR/

