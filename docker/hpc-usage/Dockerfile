# cmsmon-hpc-usage
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:test
LABEL maintainer="Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch"

ENV WDIR=/opt/spark-apps/hpc-usage
ENV OTEL_SERVICE_NAME=hpc-usage
WORKDIR $WDIR

COPY src/ $WDIR/

