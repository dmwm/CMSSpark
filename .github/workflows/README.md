## GitHub workflows

### 1 - build-docker-spark.yml

This wf builds docker images that use CMSSpark repository. It provides additional functionalities via Git tag messages.

#####How it works:

Provided git tag will be used in the CMSSpark repository checkout. Git tag message will provide more interactive management of GitHub actions.

###### There are 2 conventions currently:
1. **Builds all docker images**: `Build docker all.*spark3[_-]date=(.+)`
   - will build all docker images with provided "spark3[_-]date" cmsmon-hadoop-base image date tag
   - _Example_: `Build docker all with spark3-date=20220819`

2. **Builds individual docker images**: `Build docker [(](.+)[)].*spark3[_-]date=(.+)`
    - will build individual docker images with "spark3[_-]date" cmsmon-hadoop-base image date tag
    - docker image name(s) should be provided between parenthesis.
    - _Examples_:
      - `Build docker (cmsmon-rucio-ds) with spark3-date=20220819`
      - `Build docker (cmsmon-rucio-spark2mng, condor-cpu-eff) with spark3-date=20220819`

> spark3-date refers to YYYYMMDD suffix of **registry.cern.ch/cmsmonitoring/cmsmon-hadoop-base:spark3-YYYYMMDD** image
   which is used as base image in all docker images that we build in this workflow.
