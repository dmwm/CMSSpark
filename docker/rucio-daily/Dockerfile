# cmsmon-rucio-daily
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:test
MAINTAINER Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch 

ENV WDIR=/opt/spark-apps/rucio-daily
ENV OTEL_SERVICE_NAME=rucio-daily
WORKDIR $WDIR

COPY src/ $WDIR/

