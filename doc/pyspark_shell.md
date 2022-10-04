## How to run PySpark shell for tests in Kubernetes pods or VMs

If SWAN.cern.ch is not working, you can use PySpark to run your PySpark code. It gives nice IPython shell depending on
your python environment.

- Kerberos authentication:

```
kinit $USER#CERN.CH
```

### LxPlus7

- You need to be in LxPlus7
- If you use additional Python repositories, please make sure that they are in `PYTHONPATH`
- `--py-files` is optional, just to show how you can add it

> Attention : do not use `LCG102` for now, it produces `ImportError: libffi.so.8` error in LxPlus7.
>
> For that reason, you need to provide Avro package like `org.apache.spark:spark-avro_2.12:3.1.2` with `3.1.2` version
> which is `3.2.1` in LCG102.
>
> In any case, please set avro version according to `spark-submit --version`

###### Run in LxPlus7

```
# Setup Analytix connection

source /cvmfs/sft.cern.ch/lcg/views/LCG_101/x86_64-centos7-gcc8-opt/setup.sh
source /cvmfs/sft.cern.ch/lcg/etc/hadoop-confext/hadoop-swan-setconf.sh analytix 3.2 spark3
export PATH="${PATH}:/usr/hdp/hadoop/bin/hadoop:/usr/hdp/spark3/bin:/usr/hdp/sqoop/bin"
export PYSPARK_DRIVER_PYTHON=ipython
# Set ipython as driver python

# Required Spark confs
spark_submit_args=(
  --master yarn 
  --conf spark.ui.showConsoleProgress=false 
  --driver-memory=8g --executor-memory=8g
  --packages org.apache.spark:spark-avro_2.12:3.1.2 
  --py-files "/data/CMSMonitoring.zip,/data/stomp-v700.zip"
)

# Run
pyspark ${spark_submit_args[@]}

# Now you are in IPyhton shell

# Check version
>>> spark.version

```

### Kubernetes

- You need to have specific docker image with access to Analytix cluster
- You need to define :`spark.driver.bindAddress, spark.driver.host, spark.driver.port, spark.driver.blockManager.port`
- Kubernetes ports should be open in both way In/Out like NodePort
- If you use additional Python repositories, please make sure that they are in `PYTHONPATH`
- `--py-files` is optional, just to show how you can add it

###### Run in Kubernetes Pod

```
# Set ipython as driver python
export PYSPARK_DRIVER_PYTHON=ipython

# Required Spark confs
spark_submit_args=(
  --master yarn 
  --conf spark.ui.showConsoleProgress=false 
  --conf "spark.driver.bindAddress=0.0.0.0" 
  --driver-memory=8g --executor-memory=8g
  --conf "spark.driver.host=${MY_NODE_NAME}" 
  --conf "spark.driver.port=31201" 
  --conf "spark.driver.blockManager.port=31202"
  --packages org.apache.spark:spark-avro_2.12:3.2.1 
  --py-files "/data/CMSMonitoring.zip,/data/stomp-v700.zip"
)

# Run
pyspark ${spark_submit_args[@]}

# Now you are in IPyhton shell

# Check version
>>> spark.version

```
