# EOS Popularity cron job

As a replacement for the xroot popularity service, we created an application that can produce a similar output based on the EOS reports data available in HDFS.

For more information about it, the EOS Popularity script and documentation is available in the [CMSSpark repository](https://github.com/dmwm/CMSSpark/blob/master/doc/scripts/HDFS_EOS_script.md).

Keep in mind that this script requires the following data available in the analytix cluster HDFS:
`/project/monitoring/archive/eos-report/logs/cms/`.

The output of this cron job gets updated to its respective [CMSDataPop website](https://cmsdatapop.web.cern.ch/cmsdatapop/EOS/data/), and the data itself is used in the [EOS popularity panel](https://monit-grafana.cern.ch/goto/T-tkR0rNR?orgId=11) of the [Data Popularity dashboard](https://monit-grafana.cern.ch/goto/_JLiRA9NR?orgId=11).
