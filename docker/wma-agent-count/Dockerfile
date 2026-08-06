# wma-agent-count
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:v1.0.0
LABEL maintainer="Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch"

ENV WDIR=/opt/spark-apps/wma-agent-count
ENV OTEL_SERVICE_NAME=wma-agent-count
WORKDIR $WDIR

COPY src/ $WDIR/

