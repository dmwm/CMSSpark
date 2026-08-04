# rucio-daily-stats
FROM registry.cern.ch/cmsmonitoring/cmsmon-spark:v1.0.0
LABEL maintainer Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch

RUN pip install --no-cache-dir CMSMonitoring==0.6.6

ENV WDIR=/opt/spark-apps/rucio-daily-stats
ENV OTEL_SERVICE_NAME=rucio-daily-stats
WORKDIR $WDIR

COPY src/ $WDIR/
