# PhEDEx and DBS leftover datasets

This document contains instructions on how to get a list of leftover datasets that are present on PhEDEx but either are not in DBS or are marked as not VALID.

#### Initialization
Before running the scripts please run `./src/bash/reports_init` to clone wiki repository which contains reports. If you don't intend to submit reports to github wiki, and you just want to inspect results, this step is **not necessary**.

#### Aggregating leftovers
Please run `./src/bash/report_leftovers/aggregate_leftovers 20170228 /location/on/hdfs` to aggregate and download leftovers. First argument is PhEDEx date in following format: YYYYMMDD. This argument is required. Seconds argument is location on hdfs where .csv files will be saved. This argument is optional. Default value is: `/cms/users/$USER/leftovers`. 

This script will download two .csv files to its own directory: `src/bash/report_leftovers/leftovers_all_df.csv` and `src/bash/report_leftovers/leftovers_orphans_df.csv`.

`leftovers_all_df.csv` contains all leftovers: datasets that are present in PhEDEx but are either not present in DBS or has status that is not VALID.

`leftovers_orphans_df.csv` contains orphan leftovers: datasets that are present in PhEDEx but are not present in DBS.

#### Creating a report
You can create a markdown report with the summary of downloaded data. For this first please aggregate and download leftovers (see [Aggregating leftovers](#aggregating-leftovers)) and then run `python src/python/CMSSpark/reports/visualize_leftovers.py`. Report will be created and placed here: `src/bash/CERNTasks.wiki/CMS_Leftovers_Report.md`.

Python script takes one optional argument `--commit`. If it is added, script will be committed to CERNTasks.wiki repository. This requires authentication to that repository. If you want to do this, please follow instructions in [Initialization](#initialization) section.
