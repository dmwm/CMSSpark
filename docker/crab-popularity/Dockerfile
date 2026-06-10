# cmsmon-crab-popularity
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:test
MAINTAINER Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch 

ENV WDIR=/opt/spark-apps/crab-popularity
ENV OTEL_SERVICE_NAME=crab-popularity
WORKDIR $WDIR

COPY src/ $WDIR/

