# rucio-datasetmon
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:v1.0.0
LABEL maintainer="Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch"

ENV WDIR=/opt/spark-apps/rucio-datasetmon
ENV OTEL_SERVICE_NAME=rucio-datasetmon
WORKDIR $WDIR

COPY src/ $WDIR/
