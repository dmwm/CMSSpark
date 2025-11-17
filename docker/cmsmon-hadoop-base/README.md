# CMS Monitoring Hadoop base Docker image

This is the base image for any HDFS related workloads from the CMS Monitoring team. Importantly, it is used as the base for the `cmsmon-spark` image.

- Hadoop
- Spark CLI (submit, shell, etc.)
- HBase
- Sqoop

## Build and push

Use the shared helper script from `CMSSpark/docker`:

```shell
# Example
cd CMSSpark/docker/cmsmon-hadoop-base
../build-and-push.sh cmsmon-hadoop-base-spark3 latest
../build-and-push.sh cmsmon-hadoop-base-spark3 spark3-YYYYMMDD
```

Tags typically follow `spark(2|3)-YYYYMMDD` plus `spark(2|3)-latest`.

Keep in mind that for running the script one must be logged in the [CERN docker registry](https://registry.cern.ch/harbor) by running `docker login registry.cern.ch -u <username>`.
It will then prompt for a password and you must enter your CLI secret, NOT your CERN password (when using your personal account). This CLI secret can be found in your profile in Harbor.
