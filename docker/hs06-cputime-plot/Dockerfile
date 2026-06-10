# cmsmon-hs06-cputime-plot
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:test
LABEL maintainer="Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch"

ENV WDIR=/opt/spark-apps/hs06-cputime-plot
ENV OTEL_SERVICE_NAME=hs06-cputime-plot
WORKDIR $WDIR

COPY src/ $WDIR/

