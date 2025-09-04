# CMS Spark Docker Images

Docker images used for all of the Spark jobs used for data load pipelines in CMSMonitoring infrastructure.

All of the images are based in the `cmsmon-spark` one, which contains all the common packages and utilities required for the other services to function.

The script `build-and-push.sh` (in this same directory) can be used for updating each image directly in the cmsmonitoring docker registry. Run `./build-and-push.sh --help` for more information about its usage.

Keep in mind that for running the script one must be logged in the [CERN docker registry](https://registry.cern.ch/harbor) by running `docker login registry.cern.ch -u <username>`. It will then promt for a password and you must enter your CLI secret (when using your personal account). This can be found in your profile in Harbor.
