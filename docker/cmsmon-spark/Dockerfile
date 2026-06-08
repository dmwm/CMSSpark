# Image: cmsmon-spark
# Registry path: registry.cern.ch/cmsmonitoring/cmsmon-spark
FROM registry.cern.ch/cmsmonitoring/cmsmon-hadoop-base:latest
LABEL maintainer="Carlos Borrajo Gomez carlos.borrajo.gomez@cern.ch"

ENV WDIR=/data
WORKDIR $WDIR
ADD install.sh $WDIR/install.sh
ADD utils $WDIR/utils
ADD helpers $WDIR/helpers

ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV PATH="${PATH}:${WDIR}"
ENV PYTHONPATH="${PYTHONPATH}:${WDIR}"

RUN hadoop-setconf.sh hadoop-analytix

RUN chmod +x install.sh
RUN $WDIR/install.sh
