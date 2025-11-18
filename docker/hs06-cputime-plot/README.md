# HS06 CPU Time Plot generation cron job

Generates datasets and plots for HS06 core hours either by week of year or by month for a given calendar year.

The cronjob in Kubernetes runs on the 19th of each month at 22:00 and outputs the results to the [HS06 CPU Time section](https://cmsdatapop.web.cern.ch/cmsdatapop/hs06cputime/) of the CMS Data Popularity website. The data itself is used in panels in the  [C-RSG Plots Dashboard](https://monit-grafana.cern.ch/goto/3H4qew9Hg?orgId=11) ([Fig 2](https://monit-grafana.cern.ch/goto/SW_9eQrNg?orgId=11) and [Fig 4](https://monit-grafana.cern.ch/goto/COTgR_9HR?orgId=11)).
