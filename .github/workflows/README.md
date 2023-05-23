## GitHub workflows

### 1 - build-docker-spark.yml

This wf builds `registry.cern.ch/cmsmonitoring/cmsmon-spark` docker image.

> cmsmon-spark image uses `cmsmon-hadoop-base:spark3-latest` as base image. Therefore, that image:tag should be updated regularly (i.e. in each quarter).

##### How it works:

Provided git tag will be used in the CMSSpark repository's git checkout.

---

### [Not in use] GH workflow to run multiple builds depending on tag message of same git tag

This option is removed because our GH action builds one docker image triggered by one git tag currently.

Please check this commit if you want to build multiple docker images by defining keywords in git tag message and trigger them with same git tag: https://github.com/dmwm/CMSSpark/blob/5cc37deccb68cb2595ec087f25c0a60cfc9ac610/.github/workflows/build-docker-spark.yml

Also, you can see a proper example in https://github.com/dmwm/CMSMonitoring/blob/585641fbfe010e27f27bf50de144d8f04018e7c3/.github/workflows/build-rucio-dataset-monitoring.yml
