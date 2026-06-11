# CMS Monitoring Spark base image

Base image for Spark-related Kubernetes CronJobs.

- Analytix 3.2 cluster, Spark 3, Python 3.9
- Includes sqoop, `stomp.py==7.0.0`, `CMSMonitoring/src/python/CMSMonitoring`, selected `CMSSpark` and `CMSMonitoring` trees, plus click, pyspark, pandas, numpy, seaborn, matplotlib, plotly, requests, amtool
- GitHub workflows build and publish the image
- For OpenSearch helper usage, see `helpers/osearch/README.md`
- For OpenTelemetry helper usage, see below

## OpenTelemetry in child images

Child images can send metrics, traces, and logs to the in-cluster OpenTelemetry Collector via `helpers/otel_setup.py`. Instrumentation is opt-in: set `OTEL_ENABLED=true` and `OTEL_SERVICE_NAME` in the workload environment (and `OTEL_EXPORTER_OTLP_ENDPOINT` if not using the default `opentelemetry-collector.opentelemetry.svc.cluster.local:4317`).

```python
from helpers.otel_setup import global_meter, trace_span

@trace_span("my_job_step")
def run_job():
    counter = global_meter.create_counter("jobs_processed")
    counter.add(1)
```

See `helpers/otel_setup.py` for the full list of supported environment variables.

To forward Spark/YARN lines from `spark-submit` to OpenTelemetry, use `util_spark_submit_with_otel_logs` in the cron script (enabled when `OTEL_ENABLED=true`, disable with `OTEL_YARN_LOGS_ENABLED=false`).

StompAMQ `INFO` and below are filtered from logs by default (`stomp.py`, `StompAMQ`, `StompyListener`); filtered lines are not sent to OpenSearch or pod stdout. Disable with `OTEL_LOG_SUPPRESS_STOMP=false`, or override per logger via `OTEL_LOG_LOGGER_MIN_LEVELS` (e.g. `stomp.py:ERROR,StompAMQ:WARNING`).

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
