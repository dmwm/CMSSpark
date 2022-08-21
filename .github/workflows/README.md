## GitHub workflows

### 1 - build-docker-spark.yml

This wf builds docker images that use CMSSpark repository. It provides additional functionalities via Git tag **messages**.

##### How it works:

Provided git tag will be used in the CMSSpark repository checkout. Git tag message will provide more interactive
management of GitHub actions.

###### There are 3 conventions currently:

1. **Builds all docker images**: `Build docker all`
    - Will build all docker images with given git tag verssion as docker tag.
    - _Example_: `Build docker all`

2. **Builds individual docker images**: `Build docker [(](.+)[)]`
    - Will build individual docker images which is provided in the git tag message.
    - Docker image name(s) should be provided between parenthesis. 
    - _Examples_:
        - `Build docker (cmsmon-rucio-ds)`
        - `Build docker (cmsmon-rucio-spark2mng, condor-cpu-eff)`


> Docker images use `cmsmon-hadoop-base:spark3-latest` as base image. Therefore, that image:tag should be updated regularly (i.e. in each quarter).
