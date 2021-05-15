#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Christian Ariza <christian.ariza AT gmail [DOT] com>
"""
Generate a static site with information about workflows/requests
cpu efficiency for the workflows/request matching the parameters.
"""
import os
import time
from datetime import datetime, date, timedelta
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    LongType,
    StringType,
    StructField,
    DoubleType,
    IntegerType,
)

from pyspark.sql.functions import (
    col,
    lit,
    concat,
    when,
    mean,
    sum as _sum,
    countDistinct,
    first,
)
import pandas as pd
import numpy as np
import click

_DEFAULT_DAYS = 30
_VALID_TYPES = ["analysis", "production", "folding@home", "test"]
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
_DEFAULT_HDFS_FOLDER = "/project/monitoring/archive/condor/raw/metric"

# Kibana links should be as below to be able to use pandas df functionalities
# by_workflow
kibana_by_wf_base_link_1 = (
    '''<a target="_blank" title="First click can be SSO redirection. ''' +
    '''If so, please click 2nd time" href="https://es-cms.cern.ch/kibana/app/kibana#/discover?_g=''' +
    '''(refreshInterval:(pause:!t,value:0),time:(from:'{START_DAY}',to:'{END_DAY}'))&_a=(columns:!(''' +
    '''RequestCpus,CpuEff,CpuTimeHr,WallClockHr,Site,RequestMemory,RequestMemory_Eval,CpuEffOutlier,Tier''' +
    '''),index:'cms-20*',interval:auto,query:(language:lucene,query:'Tier:%2FT1%7CT2%2F%20AND%20CpuEffOutlier:'''
)
# + CpuEffOutlier
kibana_by_wf_base_link_2 = '''%20AND%20Status:Completed%20AND%20JobFailed:0%20AND%20Workflow:%22'''
# + Workflow
kibana_by_wf_base_link_3 = '''%22%20AND%20WMAgent_RequestName:%22'''
# + WMAgent_RequestName
kibana_by_wf_base_link_4 = '''%22'),sort:!(RecordTime,desc))">@Kibana_t1t2</a>'''

# by_site
kibana_by_site_base_link_1 = (
    '''<a target="_blank" title="First click can be SSO redirection. ''' +
    '''If so, please click 2nd time" href="https://es-cms.cern.ch/kibana/app/kibana#/discover?_g=''' +
    '''(refreshInterval:(pause:!t,value:0),time:(from:'{START_DAY}',to:'{END_DAY}'))&_a=(columns:!(''' +
    '''RequestCpus,CpuEff,CpuTimeHr,WallClockHr,Site,RequestMemory,RequestMemory_Eval,CpuEffOutlier''' +
    '''),index:'cms-20*',interval:auto,query:(language:lucene,query:'CpuEffOutlier:'''
)
# + CpuEffOutlier
kibana_by_site_base_link_2 = '''%20AND%20Status:Completed%20AND%20JobFailed:0%20AND%20WMAgent_RequestName:%22'''
# + WMAgent_RequestName
kibana_by_site_base_link_3 = '''%22%20AND%20Workflow:%22'''  # + Workflow
kibana_by_site_base_link_4 = '''%22%20AND%20Site:%22'''  # + Site
kibana_by_site_base_link_5 = '''%22'),sort:!(RecordTime,desc))">@Kibana</a>'''


def get_spark_session(yarn=True, verbose=False):
    """
    Get or create the spark context and session.
    """
    sc = SparkContext(appName="cms-cpu-efficiency")
    return SparkSession.builder.config(conf=sc._conf).getOrCreate()


def format_df(df):
    """
    Set the presentation format for the dataframe
    """
    pd.set_option("display.max_colwidth", -1)  # never cut long columns
    pd.options.display.float_format = "{:,.2f}".format  # only 2 decimals
    df = df.rename(
        columns={
            "wf_cpueff": "CPU_eff",
            "wf_cpus": "CPUs",
            "wf_cputimehr": "CPU_time_hr",
            "wf_wallclockhr": "Wall_time_hr",
            "wf_cpueff_t1_t2": "CPU_eff_T1T2",
            "wf_cputimehr_t1_t2": "CPU_time_hr_T1T2",
            "wf_wallclockhr_t1_t2": "Wall_time_hr_T1T2"
        }
    )

    df["CPU_eff"] = df["CPU_eff"].map("{:,.1f}%".format)
    df["CPUs"] = df["CPUs"].map(int)
    df["CPU_time_hr"] = df["CPU_time_hr"].map(int)
    df["Wall_time_hr"] = df["Wall_time_hr"].map(int)
    df["CPU_eff_T1T2"] = df["CPU_eff_T1T2"].apply(lambda x: "-" if np.isnan(x) else "{:,.1f}%".format(x))
    df["CPU_time_hr_T1T2"] = df["CPU_time_hr_T1T2"].apply(lambda x: "-" if np.isnan(x) else int(x))
    df["Wall_time_hr_T1T2"] = df["Wall_time_hr_T1T2"].apply(lambda x: "-" if np.isnan(x) else int(x))
    return df


def get_tiers_html(grouped_tiers):
    """
    Arrange tiers html
    """
    grouped_tiers = grouped_tiers.sort_values("Tier")
    pd.set_option("display.max_colwidth", -1)  # never cut long columns
    pd.options.display.float_format = "{:,.2f}".format  # only 2 decimals
    grouped_tiers["tier_cpueff"] = grouped_tiers["tier_cpueff"].map("{:,.1f}%".format)
    grouped_tiers["tier_cpus"] = grouped_tiers["tier_cpus"].map(int)
    grouped_tiers["tier_cputimehr"] = grouped_tiers["tier_cputimehr"].map(int)
    grouped_tiers["tier_wallclockhr"] = grouped_tiers["tier_wallclockhr"].map(int)
    html_tiers = grouped_tiers.to_html(escape=False, index=False)
    html_tiers = html_tiers.replace('table border="1" class="dataframe"',
                                    'table id="dataframe-tiers" class="display compact" style="width:100%;"')
    html_tiers = html_tiers.replace('style="text-align: right;"', "")
    return html_tiers


def get_candidate_files(
    start_date, end_date, spark, base=_DEFAULT_HDFS_FOLDER,
):
    """
    Returns a list of hdfs folders that can contain data for the given dates.
    """
    st_date = start_date - timedelta(days=1)
    ed_date = end_date + timedelta(days=1)
    days = (ed_date - st_date).days
    pre_candidate_files = [
        "{base}/{day}{{,.tmp}}".format(
            base=base, day=(st_date + timedelta(days=i)).strftime("%Y/%m/%d")
        )
        for i in range(0, days)
    ]
    sc = spark.sparkContext
    # The candidate files are the folders to the specific dates,
    # but if we are looking at recent days the compaction procedure could
    # have not run yet so we will considerate also the .tmp folders.
    candidate_files = [
        f"{base}/{(st_date + timedelta(days=i)).strftime('%Y/%m/%d')}{{,.tmp}}"
        for i in range(0, days)
    ]
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = FileSystem.get(URI("hdfs:///"), sc._jsc.hadoopConfiguration())
    candidate_files = [url for url in candidate_files if fs.globStatus(Path(url))]
    return candidate_files


def _get_schema():
    return StructType(
        [
            StructField(
                "data",
                StructType(
                    [
                        StructField("GlobalJobId", StringType(), nullable=False),
                        StructField("Workflow", StringType(), nullable=False),
                        StructField("WMAgent_RequestName", StringType(), nullable=True),
                        StructField("ScheddName", StringType(), nullable=True),
                        StructField("WMAgent_JobID", StringType(), nullable=True),
                        StructField("RecordTime", LongType(), nullable=False),
                        StructField("JobFailed", LongType(), nullable=False),
                        StructField("Status", StringType(), nullable=True),
                        StructField("Site", StringType(), nullable=True),
                        StructField("Tier", StringType(), nullable=True),
                        StructField("Type", StringType(), nullable=True),
                        StructField("WallClockHr", DoubleType(), nullable=False),
                        StructField("CpuTimeHr", DoubleType(), nullable=True),
                        StructField("RequestCpus", DoubleType(), nullable=True),
                        StructField("CpuEff", DoubleType(), nullable=True),
                        StructField("CpuEffOutlier", IntegerType(), nullable=True),
                    ]
                ),
            ),
        ]
    )


def _generate_main_page(selected_pd,
                        grouped_tiers,
                        start_date,
                        end_date,
                        workflow_column=None,
                        filter_column=None,
                        cpu_eff_outlier=0):
    """Create HTML page

    Header
    Tiers table
    WF table with Site table selection
    Footer
    """
    workflow_column = (
        workflow_column if workflow_column is not None else pd["Workflow"].copy()
    )
    filter_column = filter_column if filter_column is not None else workflow_column
    is_wf = filter_column.name == "Workflow"
    selected_pd["Workflow"] = (
        f'<a class="wfname{" selname" if is_wf else ""}">'
        + workflow_column
        + '</a><br><a target="_blank" href="https://cms-pdmv.cern.ch/mcm/requests?prepid='
        + workflow_column
        + '">McM</a> '
          '<a target="_blank" href="https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=task_'
        + workflow_column
        + '">PMon</a> '
        + kibana_by_wf_base_link_1.format(START_DAY=(start_date + timedelta(seconds=time.altzone))
                                          .strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                                          END_DAY=(end_date + timedelta(seconds=time.altzone))
                                          .strftime('%Y-%m-%dT%H:%M:%S.000Z'))
        + str(cpu_eff_outlier)
        + kibana_by_wf_base_link_2
        + workflow_column
        + kibana_by_wf_base_link_3
        + selected_pd["WMAgent_RequestName"]
        + kibana_by_wf_base_link_4
    )
    if not is_wf:
        _fc = '<a class="selname">' + filter_column + "</a>"
        if filter_column.name == "WMAgent_RequestName":
            _fc += (
                '<br/><a href="https://cms-unified.web.cern.ch/cms-unified/report/'
                + filter_column
                + '">logs</a>'
            )
        selected_pd[filter_column.name] = _fc
    html = format_df(selected_pd).to_html(escape=False, index=False)
    html = html.replace(
        'table border="1" class="dataframe"',
        'table id="dataframe" class="display compact" style="width:100%;"',
    )
    html = html.replace('style="text-align: right;"', "")
    # cleanup of the default dump
    html = html.replace(
        'table border="1" class="dataframe"',
        'table id="dataframe" class="display compact" style="width:100%;"',
    )
    html = html.replace('style="text-align: right;"', "")

    html_header = f"""<!DOCTYPE html>
    <html>
    <head>
    <link rel="stylesheet" href="https://cdn.datatables.net/1.10.20/css/jquery.dataTables.min.css">
    <style>
    .dataTables_filter input {{
      border: 7px solid Tomato;
      width: 400px;
      font-size: 16px;
      font-weight: bold;
    }}
    table td {{
    word-break: break-all;
    }}
    #dataframe-tiers table {{
      font-family: arial, sans-serif;
      border-collapse: collapse;
      width: 100%;
    }}
    #dataframe-tiers td, th {{
      border: 1px solid #dddddd;
      text-align: left;
      padding: 8px;
    }}
    #dataframe-tiers tr:nth-child(even) {{
      background-color: #dddddd;
    }}
    </style>
    </head>
    <body>
    <h2>Dump of CMSSW Workflows and Their efficiencies
    from {start_date.strftime("%A %d. %B %Y")} to {end_date.strftime("%A %d. %B %Y")}</h2>
     <ul>
      <li>
        <b>Please see
            <a href="https://cmsdatapop.web.cern.ch/cmsdatapop/cpu_eff/">non-outlier</a>
            and
            <a href="https://cmsdatapop.web.cern.ch/cmsdatapop/cpu_eff_outlier/">outlier</a>
            efficiency tables
        </b>
      </li>
      <li><b>CPU_eff</b>: 100*sum(CpuTimeHr)/sum(CoreTime) ~~P.S.~~ CoreTime = WallClockHr * RequestCpus </li>
      <li>
        Ref1: <a href="https://github.com/dmwm/CMSSpark/blob/master/src/python/CMSSpark/condor_cpu_efficiency.py">
            Python script
        </a>
        &nbsp;
        Ref2: <a href="https://cmsmonit-docs.web.cern.ch/MONIT/cms-htcondor-es/">
            Documentation of used attributes
        </a>
      </li>
    </ul>
    <div class="tiers" style="display:block;">
    """
    # > Tiers table
    html_middle = (
        '''
        </div>
        <div class="container" style="display:block; width:100%">
    ''')
    # > WF table
    html_footer = (
        '''
    </div>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js">
    </script>
    <script type="text/javascript" src="https://cdn.datatables.net/1.10.20/js/jquery.dataTables.min.js">
    </script>
    <script>
        $(document).ready(function () {
        function toggleDetails(){
                var tr = $(this).closest("tr");
                sel_name = $(tr).find("td a.selname").text()
                wf_name = $(tr).find("td a.wfname").text()
                d_class="details-show"
                row = dt.row(tr)
                if(!row.child.isShown())
                {
                    console.log(wf_name)
                    $(tr).addClass(d_class)
                    row.child("<div id='details_"+sel_name+"'>loading</div>").show()
                    folder = "wfbysite"'''
        + ("" if is_wf else "+'/'+wf_name")
        + """
                    $.get(folder+"/CPU_Efficiency_bySite_"+sel_name+".html", function (response){
                        var html = response;
                        $("#details_"+sel_name).html(html);
                    });

                }else{
                    $(tr).removeClass(d_class)
                    row.child.hide()
                }

            }
            $('table#dataframe thead tr').append('<th>site details</th>');
            $('table#dataframe tbody tr').append('<td><button class="btn-details">+</button></td>');
            var dt = $('#dataframe').DataTable( {
                "order": [[ 4, "asc" ]],
                "scrollX": false,
                language: {
                    search: "_INPUT_",
                    searchPlaceholder: "--- Search Workflows ---",
                },
            });
            $('table#dataframe tbody tr').on('click','td button.btn-details',toggleDetails)
            dt.on('draw', function(){
            $('table#dataframe tbody tr').off('click').on('click','td button.btn-details',toggleDetails)
            })
        });
    </script></body></html>"""
    )
    html_tiers = get_tiers_html(grouped_tiers)
    return html_header + html_tiers + html_middle + html + html_footer


@click.command()
@click.option("--start_date", type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--end_date", type=click.DateTime(_VALID_DATE_FORMATS))
@click.option(
    "--cms_type",
    default="production",
    type=click.Choice(_VALID_TYPES),
    help=f"Workflow type to query {_VALID_TYPES}",
)
@click.option("--output_folder", default="./www/cpu_eff", help="local output directory")
@click.option("--offset_days", default=3, help="Offset to end_date")
@click.option("--cpu_eff_outlier", default=0, help="Filter by CpuEffOutlier")
def generate_cpu_eff_site(
    start_date=None,
    end_date=None,
    cms_type="production",
    output_folder="./www/cpu_eff",
    offset_days=3,
    cpu_eff_outlier=0,
):
    """
    """
    _offset_days = datetime.combine(date.today() - timedelta(days=offset_days), datetime.min.time())
    if not (start_date or end_date):
        # defaults to the last 30 days with 3 days offset.
        # Default: (today-33days to today-3days)
        end_date = _offset_days
        start_date = end_date - timedelta(days=_DEFAULT_DAYS)
    elif not start_date:
        start_date = end_date - timedelta(days=_DEFAULT_DAYS)
    elif not end_date:
        end_date = min(start_date + timedelta(days=_DEFAULT_DAYS), _offset_days)
    if start_date > end_date:
        raise ValueError(
            f"start date ({start_date}) should be earlier than end date({end_date})"
        )

    group_type_map = {
        "production": ["Workflow", "WMAgent_RequestName"],
        "analysis": ["Workflow"],
        "test": ["Workflow"],
        "folding@home": ["Workflow"],
    }
    group_by_col = group_type_map[cms_type]
    spark = get_spark_session()
    schema = _get_schema()
    raw_df = (
        spark.read.option("basePath", _DEFAULT_HDFS_FOLDER)
        .json(
            get_candidate_files(start_date, end_date, spark, base=_DEFAULT_HDFS_FOLDER),
            schema=schema,
        ).select("data.*")
        .filter(
            f"""Status='Completed'
          AND JobFailed=0
          AND RecordTime >= {start_date.timestamp() * 1000}
          AND RecordTime < {end_date.timestamp() * 1000}
          AND Type =  '{cms_type}'
          AND CpuEffOutlier = '{cpu_eff_outlier}'
          """
        )
        .drop_duplicates(["GlobalJobId"])
    )
    raw_df = (
        raw_df.withColumn(
            "RequestCpus",
            when(col("RequestCpus").isNotNull(), col("RequestCpus")).otherwise(lit(1)),
        ).withColumn("CoreTime", col("WallClockHr") * col("RequestCpus"))
    ).cache()

    grouped_tiers = raw_df.groupby("Tier", "Type", "CpuEffOutlier").agg(
        (100 * _sum("CpuTimeHr") / _sum("CoreTime")).alias("tier_cpueff"),
        _sum("RequestCpus").alias("tier_cpus"),
        _sum("CpuTimeHr").alias("tier_cputimehr"),
        _sum("WallClockHr").alias("tier_wallclockhr"),
    ).toPandas()
    grouped_wf = raw_df.groupby(*group_by_col, "Type").agg(
        (100 * _sum("CpuTimeHr") / _sum("CoreTime")).alias("wf_cpueff"),
        _sum("RequestCpus").alias("wf_cpus"),
        _sum("CpuTimeHr").alias("wf_cputimehr"),
        _sum("WallClockHr").alias("wf_wallclockhr"),
    )
    grouped_wf_t1_t2 = raw_df.filter("""Tier='T1' OR Tier='T2'""").groupby(*group_by_col, "Type").agg(
        (100 * _sum("CpuTimeHr") / _sum("CoreTime")).alias("wf_cpueff_t1_t2"),
        _sum("CpuTimeHr").alias("wf_cputimehr_t1_t2"),
        _sum("WallClockHr").alias("wf_wallclockhr_t1_t2"),
    )
    grouped_site_wf = raw_df.groupby(*group_by_col, "Site").agg(
        (100 * _sum("CpuTimeHr") / _sum("CoreTime")).alias("wf_site_cpueff"),
        _sum("RequestCpus").alias("wf_cpus"),
        _sum("CpuTimeHr").alias("wf_site_cputimehr"),
        _sum("WallClockHr").alias("wf_site_wallclockhr"),
        first("ScheddName").alias("schedd"),
        first("WMAgent_JobID").alias("wmagent_jobid"),
    )

    select_expr = f"""wf_wallclockhr > 100"""
    selected_df = grouped_wf.where(select_expr)
    selected_pd = selected_df.toPandas()
    grouped_wf_t1_t2 = grouped_wf_t1_t2.toPandas()
    grouped_wf_t1_t2.drop(['Type'], axis=1, inplace=True)
    # Merge grouped_wf and grouped_wf_t1_t2 to see cpueff, cputimehr and wallclockhr values of (T1-T2 sites only)
    selected_pd = pd.merge(selected_pd, grouped_wf_t1_t2,
                           how='left', left_on=['Workflow', 'WMAgent_RequestName'],
                           right_on=['Workflow', 'WMAgent_RequestName'])
    workflow_column = selected_pd["Workflow"].copy()
    filter_column = (
        workflow_column
        if group_by_col[-1] == "Workflow"
        else selected_pd[group_by_col[-1]].copy()
    )
    main_page = _generate_main_page(selected_pd, grouped_tiers, start_date, end_date, workflow_column, filter_column,
                                    cpu_eff_outlier)
    os.makedirs(output_folder, exist_ok=True)
    with open(f"{output_folder}/CPU_Efficiency_Table.html", "w") as ofile:
        ofile.write(main_page)
    # We are only interested on the selected workflows.
    site_wf = grouped_site_wf.where(
        col(filter_column.name).isin(filter_column.to_list())
    ).toPandas()
    if cms_type == "production":
        site_wf["log"] = (
            "<a href='https://cms-unified.web.cern.ch/cms-unified/logmapping/"
            + site_wf["WMAgent_RequestName"]
            + "/"
            + site_wf["schedd"]
            + "_"
            + site_wf["wmagent_jobid"]
            + ".tar.gz'>logs</a>"
        )
        site_wf.drop(columns="schedd")
        site_wf["@Kibana"] = (
            kibana_by_site_base_link_1.format(START_DAY=(start_date + timedelta(seconds=time.altzone))
                                              .strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                                              END_DAY=(end_date + timedelta(seconds=time.altzone))
                                              .strftime('%Y-%m-%dT%H:%M:%S.000Z'))
            + str(cpu_eff_outlier)
            + kibana_by_site_base_link_2
            + site_wf["WMAgent_RequestName"]
            + kibana_by_site_base_link_3
            + site_wf["Workflow"]
            + kibana_by_site_base_link_4
            + site_wf["Site"]
            + kibana_by_site_base_link_5
        )
    site_wf = site_wf.set_index([*group_by_col, "Site"]).sort_index()
    # Create one file per worflow, so we don't have a big file collapsing the browser.
    _folder = f"{output_folder}/wfbysite"
    os.makedirs(_folder, exist_ok=True)
    num_levels = len(group_by_col)
    for workflow, df in site_wf.groupby(filter_column.name):
        sublevels = ""
        if num_levels > 1:
            df_ni = df.reset_index()
            sublevels = (
                "/".join(df_ni[group_by_col[0:-1]].drop_duplicates().values[0].tolist())
                + "/"
            )
            os.makedirs(f"{_folder}/{sublevels}", exist_ok=True)
        df.droplevel(list(range(num_levels))).to_html(
            f"{_folder}/{sublevels}CPU_Efficiency_bySite_{workflow}.html", escape=False,
        )


if __name__ == "__main__":
    generate_cpu_eff_site()

