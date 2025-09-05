# EOS Popularity cron job

As a replacement for the xroot popularity service, we created an application that can produce a similar output based on the EOS reports data available in HDFS.

For more information about it, the EOS Popularity script and documentation is available in the [CMSSpark repository](https://github.com/dmwm/CMSSpark/blob/master/doc/scripts/HDFS_EOS_script.md).

Keep in mind that this script requires the following data available in the analytix cluster HDFS:
`/project/monitoring/archive/eos-report/logs/cms/`.
