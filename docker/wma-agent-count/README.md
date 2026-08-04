# WMA Agent Count cronjob

Calculates daily agent count per host and sends it to OpenSearch os-cms test tenant weekly index.

The data is then used in the [WMAgent Job Count Agg dashboard](https://monit-grafana.cern.ch/goto/xbXrn99NR?orgId=11) in Grafana.
