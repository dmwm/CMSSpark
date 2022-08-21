## GitHub workflows

### 1 - build-docker-spark.yml

This wf builds docker images that use CMSSpark repository. It provides additional functionalities via Git tag messages.

##### How it works:

Provided git tag will be used in the CMSSpark repository checkout. Git tag message will provide more interactive
management of GitHub actions.

###### There are 3 conventions currently:

1. **Builds all docker images**: `Build docker all.*py=([.0-9]+).*spark3[_-]date=(.*[0-9]+)`
    - will build all docker images with given
        - `py`(PY_VERSION) which is build argument required for cmsmon-hadoop-base image
        - and `spark3[_-]date` cmsmon-hadoop-base image date tag
    - All other images depend on `cmsmon-hadoop-base`. Hence, firstly `cmsmon-hadoop-base` will be built. All other
      images will wait its built process to finish. Their build will run parallely. 
    - Docker image tag of `cmsmon-hadoop-base` will be given `spark3[_-]date` value. Other tags will use this value to
      get base image.
    - Other docker images will have "tag name" as docker image tag, i.e. : `v00.03.01`
    - _Example_: `Build docker all with py=3.9.12 and spark3-date=20220821`

2. **Builds docker hadoop base image**: `Build docker hadoop base.*py=([.0-9]+).*spark3[_-]date=(.*[0-9]+)`
    - will build `cmsmon-hadoop-base` docker image
        - `py`(PY_VERSION) which is build argument required for cmsmon-hadoop-base image
        - and `spark3[_-]date` cmsmon-hadoop-base image date tag
    - Docker image tag of `cmsmon-hadoop-base` will be given `spark3[_-]date` value.
    - _Example_: `Build docker hadoop base with py=3.9.12 and spark3-date=20220821`
    
3. **Builds individual docker images**: `Build docker [(](.+)[)].*spark3[_-]date=(.*[0-9]+)`
    - will build individual docker images with `spark3[_-]date` as `cmsmon-hadoop-base:spark3-YYYYMMDD` image date tag. There should be a `cmsmon-hadoop-base:spark3-YYYYMMDD` docker tag with given date in the registry. Because it is used as base image in the individual docker images.
    - docker image name(s) should be provided between parenthesis. 
    - _Examples_:
        - `Build docker (cmsmon-rucio-ds) with spark3-date=20220819`
        - `Build docker (cmsmon-rucio-spark2mng, condor-cpu-eff) with spark3-date=20220821`


> spark3-date refers to YYYYMMDD suffix of **registry.cern.ch/cmsmonitoring/cmsmon-hadoop-base:spark3-YYYYMMDD** image which is used as base image in all docker images that we build in this workflow.



###### Use cases:

- If we want to build all images, please run 1st option. When we should build all? Each 1-2 month, cern release new linux release and hadoop team use it as base image. That's why, if there is new release in https://gitlab.cern.ch/db/cerndb-infra-hadoop-conf `latest-hdp3`, we need to build all images.
- If we want to build only base image(`cmsmon-hadoop-base`), we can run 2nd option. Provided `py=` will be used as `PY_VERSION` build argument in the Dockerfile. Given date will be the "YYYYMMDD" suffix in the `cmsmon-hadoop-base:spark3-YYYYMMDD` docker image tag. If you want to test, you can provide prefix to date like `testYYYYMMDD`.
- If we want to build individual images, chose 3rd option. Be careful about given date value, there should be a docker release with given tag `cmsmon-hadoop-base:spark3-YYYYMMDD` in CERN registry.  
