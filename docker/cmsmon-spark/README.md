# CMS Monitoring Spark base image

Base image for Spark-related Kubernetes CronJobs.

- Analytix 3.2 cluster, Spark 3, Python 3.9
- Includes sqoop, `stomp.py==7.0.0`, `CMSMonitoring/src/python/CMSMonitoring`, selected `CMSSpark` and `CMSMonitoring` trees, plus click, pyspark, pandas, numpy, seaborn, matplotlib, plotly, requests, amtool
- GitHub workflows build and publish the image
- For OpenSearch helper usage, see `helpers/osearch/README.md`

## Build and push

Use the shared helper script in `CMSSpark/docker`:

```shell
cd CMSSpark/docker
./build-and-push.sh -f ./cmsmon-spark cmsmon-spark v1.0.0
# Not specifying any tag defaults to `test`
./build-and-push.sh -f ./cmsmon-spark cmsmon-spark 
```

Run `./build-and-push.sh --help` for options such as custom Dockerfile paths or tags.

Keep in mind that for running the script one must be logged in the [CERN docker registry](https://registry.cern.ch/harbor) by running `docker login registry.cern.ch -u <username>`.
It will then prompt for a password and you must enter your CLI secret, NOT your CERN password (when using your personal account). This CLI secret can be found in your profile in Harbor.

## Versioning information

We have tagged the first version after the refactoring of all cron job images as `v1.0.0`, and that is the code hosted here. New versions will follow that numbering.

For information about earlier versions (`v0.5.0.12` and earlier), check out [this folder](https://github.com/dmwm/CMSKubernetes/tree/master/docker/cmsmon-spark) in the CMSKubernetes repository.
