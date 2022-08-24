#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : stepchain_cpu_eff.py
Author      : Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : Generates StepChain tasks' CPU efficiency static web site
"""

# system modules
import os
import time
from datetime import date, datetime, timedelta

import click
import pandas as pd
from pyspark.sql.functions import (
    col as _col,
    collect_set as _collect_set,
    count as _count,
    lit,
    mean as _mean,
    sum as _sum,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    LongType,
)

# CMSSpark modules
from CMSSpark.spark_utils import get_spark_session, get_candidate_files

pd.options.display.float_format = "{:,.2f}".format
pd.set_option("display.max_colwidth", None)

# global variables
_DEFAULT_HDFS_FOLDER = "/project/monitoring/archive/wmarchive/raw/metric"
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]


def get_schema():
    """Final schema of steps"""
    return StructType(
        [
            StructField('ts', LongType(), nullable=False),
            StructField('task', StringType(), nullable=False),
            StructField('fwjr_id', StringType(), nullable=False),
            StructField('site', StringType(), nullable=False),
            StructField('acquisitionEra', StringType(), nullable=True),
            StructField('step_name', StringType(), nullable=False),
            StructField('jobCPU', DoubleType(), nullable=True),
            StructField('jobTime', DoubleType(), nullable=True),
            StructField('ncores', IntegerType(), nullable=True),
            StructField('nthreads', IntegerType(), nullable=True),
            StructField('era_len', IntegerType(), nullable=True),
            StructField('steps_len', IntegerType(), nullable=False),
            StructField('cpuEff', DoubleType(), nullable=True),
        ]
    )


def udf_step_extract(row):
    """
    Borrowed from wmarchive.py

    Helper function to extract useful data from WMArchive records.
    Returns list of step_res
    """
    meta = row.meta_data
    result = []
    count = 0
    task_name = row.task
    for step in row['steps']:
        if step['name'].lower().startswith('cmsrun'):
            step_res = {'ts': meta.ts, 'task': task_name, 'fwjr_id': meta.fwjr_id}
            count += 1
            step_res["step_name"] = step.name
            step_res['site'] = step.site
            step_res['ncores'] = step.performance.cpu.NumberOfStreams
            step_res['nthreads'] = step.performance.cpu.NumberOfThreads
            step_res['jobCPU'] = step.performance.cpu.TotalJobCPU
            step_res['jobTime'] = step.performance.cpu.TotalJobTime
            if step_res['jobCPU'] and step_res['nthreads'] and step_res['jobTime']:
                step_res['cpuEff'] = round(100 * (step_res['jobCPU'] / step_res['nthreads']) / step_res['jobTime'], 2)
            else:
                step_res['cpuEff'] = None
            step_res['acquisitionEra'] = set()
            if step.output:
                for outx in step.output:
                    step_res['acquisitionEra'].add(outx.acquisitionEra)
            if step_res['acquisitionEra']:
                step_res['era_len'] = len(step_res['acquisitionEra'])
                step_res['acquisitionEra'] = step_res['acquisitionEra'].pop()
            else:
                step_res['era_len'] = 0
                step_res['acquisitionEra'] = None
            result.append(step_res)
    if result:
        [r.setdefault("steps_len", count) for r in result]
        return result


def get_kibana_links():
    """Returns kibana link substrings as list.

    String formatting is not possible since they are used in pandas column/index aggregations"""
    kibana_link_0 = (
        '''<a target="_blank" title="First click can be SSO redirection. ''' +
        '''If so, please click 2nd time" href="''' +
        '''https://monit-kibana.cern.ch/kibana/app/kibana#/discover?_g=(filters:!(),refreshInterval:''' +
        '''(pause:!t,value:0),time:(from:'{START_DAY}',to:'{END_DAY}'))''' +
        '''&_a=(columns:!(_source),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index''' +
        ''':'60770470-8326-11ea-88fc-cfaa9841e350',key:data.steps.site,negate:!f,params:(query:'''
    )
    # + SITE_NAME
    kibana_link_1 = '''),type:phrase,value:'''
    # + SITE_NAME
    kibana_link_2 = '''),query:(match:(data.steps.site:(query:'''
    # + SITE_NAME
    kibana_link_3 = (
        ''',type:phrase))))),index:'60770470-8326-11ea-88fc-cfaa9841e350',interval:auto,query:''' +
        '''(language:lucene,query:'data.meta_data.jobstate:success%20AND%20data.meta_data.jobtype:''' +
        '''Production%20AND%20data.task:%22'''
    )
    # + TASK_NAME
    kibana_link_4 = '''%22'),sort:!(metadata.timestamp,desc))">@Kibana</a>'''
    return [kibana_link_0, kibana_link_1, kibana_link_2, kibana_link_3, kibana_link_4]


def get_req_mgr_links():
    link_0 = '''<a target="_blank" title="ReqMgr link of the task" href="https://cmsweb.cern.ch/reqmgr2/fetch?rid='''
    link_1 = '''">@ReqMgr</a>'''
    return [link_0, link_1]


def _generate_main_page(selected_pd, task_column, start_date, end_date):
    """Create HTML page

    Linux file name cannot contain slash character.
    Task names contains slash character and html file names consist of task names.
    Because of that, a workaround is applied: slash is replaced with "-_-" before writing htmls.
    Also this logic is used in JavaScript script to get html file content.
    """

    selected_pd["task"] = (
        f'<a class="taskname">'
        + task_column
        + '</a><br>'
    )
    _fc = '<a class="selname">' + task_column + "</a>"

    selected_pd[task_column.name] = _fc
    html = selected_pd.to_html(escape=False, index=False)
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
    <h2>Dump of CMSSW StepChain CPU Efficiencies
    from {start_date.strftime("%A %d. %B %Y")} to {end_date.strftime("%A %d. %B %Y")}</h2>
     <ul>
      <li>
        In main page, <strong>mean_cpueff</strong> is the average of all individual steps(cmsRun1,2,etc.) of a task.
      </li>
      <li>
        In detailed view, <strong>mean_cpueff</strong> is the average cpu eff of a specific step type in a site.
      </li>
      <li>Aggregations:</li>
        <ul>
          <li>
            <strong>Individual step cpu efficiency calculation</strong>: <code>cpuEff=
            (jobCPU / nthreads) / jobTime </code>
          </li>
          <li><strong>avg_jobCPU</strong>: <code>sum(jobCPU) / #jobs </code></li>
          <li><strong>avg_jobTime</strong>: <code>sum(jobTime) / #jobs </code></li>
        </ul>
      <li>Script that produces this table: <a href=
            "https://github.com/dmwm/CMSSpark/blob/master/src/python/CMSSpark/stepchain_cpu_eff.py"
          >stepchain_cpu_eff.py</a>
      </li>
    </ul>
    """
    # > Tiers table
    html_middle = (
        '''
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
                    task_name = $(tr).find("td a.task").text()
                    d_class="details-show"
                    row = dt.row(tr)
                    if(!row.child.isShown())
                    {
                        console.log(task_name)
                        // html file name of a task includes slash replaced with -_-, 
                        // so obey that rule to get html content                       
                        sel_name_replaced = sel_name.replaceAll(/\//g, "-_-");
                        $(tr).addClass(d_class)
                        row.child("<div id='details_"+sel_name_replaced+"'>loading</div>").show()
                        folder = "wfbytask"
                        console.log(folder+"/Cpu_Eff_byTask_"+sel_name_replaced+".html")
                        $.get(folder+"/Cpu_Eff_byTask_"+sel_name_replaced+".html", function (response){
                            var html = response;
                            $("#details_"+sel_name_replaced).html(html);
                        });
                    }else{
                        $(tr).removeClass(d_class)
                        row.child.hide()
                    }
                }
                $('table#dataframe thead tr').append('<th>task details</th>');
                $('table#dataframe tbody tr').append('<td><button class="btn-details">+</button></td>');
                var dt = $('#dataframe').DataTable( {
                    "order": [[ 4, "asc" ]],
                    "scrollX": false,
                    language: {
                        search: "_INPUT_",
                        searchPlaceholder: "--- Search Tasks ---",
                    },
                });
                $('table#dataframe tbody tr').on('click','td button.btn-details',toggleDetails)
                dt.on('draw', function(){
                $('table#dataframe tbody tr').off('click').on('click','td button.btn-details',toggleDetails)
                })
            });
        </script></body></html>'''
    )

    return html_header + html_middle + html + html_footer


def write_htmls(grouped_details, grouped_task, start_date, end_date, output_folder):
    """Write detailed dataframe and main page's task dataframe to html files

    Because of that Linux file name cannot contain slash,
    html file name of a task contains different characters(-_-) which are replaced with slash.
    This replacement trick is transparent, users see normal task names.
    """
    k_links = get_kibana_links()
    grouped_details = grouped_details.set_index(["task", "site", "step_name"]).sort_index()
    grouped_details["@Kibana"] = (
        k_links[0].format(
            START_DAY=(start_date + timedelta(seconds=time.altzone)).strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            END_DAY=(end_date + timedelta(seconds=time.altzone)).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        ) +
        grouped_details.index.get_level_values('site') +
        k_links[1] +
        grouped_details.index.get_level_values('site') +
        k_links[2] +
        grouped_details.index.get_level_values('site') +
        k_links[3] +
        grouped_details.index.get_level_values('task') +
        k_links[4]
    )
    # Create one file per worflow, so we don't have a big file collapsing the browser.
    _folder = f"{output_folder}/wfbytask"
    os.makedirs(_folder, exist_ok=True)
    for task_name, df_iter in grouped_details.groupby(["task"]):
        task_name = task_name.replace("/", "-_-")
        df_iter.droplevel(["task"]).to_html(
            f"{_folder}/Cpu_Eff_byTask_{task_name}.html", escape=False,
        )
    req_mgr_links = get_req_mgr_links()
    # ReqMgr website link
    grouped_task["@ReqMgr"] = (
        req_mgr_links[0] +
        # extract task_name from "/task_name/name_of_1st_step"
        grouped_task['task'].str.split("/").str[1] +
        req_mgr_links[1]
    )
    task_column = grouped_task["task"].copy()
    main_page = _generate_main_page(grouped_task, task_column, start_date, end_date)
    os.makedirs(output_folder, exist_ok=True)
    with open(f"{output_folder}/CPU_Efficiency_Table.html", "w") as ofile:
        ofile.write(main_page)


@click.command()
@click.option("--output_folder", default="./www/stepchain", help="local output directory")
@click.option("--start_date", type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--end_date", type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--last_n_days", type=int, default=15, help="Last n days data will be used")
def main(
    output_folder="./www/stepchain",
    start_date=None,
    end_date=None,
    last_n_days=15,
):
    """Get step data in wmarchive.

    Each step array contains multiple steps. Udf function returns each step as a separate row in a list.
    flatMap helps to flat list of steps to become individual rows in dataframe.
    """
    # Borrowed logic from condor_cpu_efficiency
    _yesterday = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
    if not (start_date or end_date):
        end_date = _yesterday
        start_date = end_date - timedelta(days=last_n_days)
    elif not start_date:
        start_date = end_date - timedelta(days=last_n_days)
    elif not end_date:
        end_date = min(start_date + timedelta(days=last_n_days), _yesterday)
    if start_date > end_date:
        raise ValueError(
            f"start date ({start_date}) should be earlier than end date({end_date})"
        )

    spark = get_spark_session(app_name='cms-stepchain-cpu-eff')
    df_raw = spark.read.option("basePath", _DEFAULT_HDFS_FOLDER).json(
        get_candidate_files(start_date, end_date, spark, base=_DEFAULT_HDFS_FOLDER, day_delta=1)
    ) \
        .select(["data.*", "metadata.timestamp"]) \
        .filter(
        f"""data.meta_data.jobstate='success'
                  AND data.meta_data.jobtype='Production'
                  AND data.wmats >= {start_date.timestamp()}
                  AND data.wmats < {end_date.timestamp()}
                  """
    )
    df_rdd = df_raw.rdd.flatMap(lambda r: udf_step_extract(r))
    df = spark.createDataFrame(df_rdd, schema=get_schema()).dropDuplicates().where(_col("ncores").isNotNull()).cache()
    df_details = df.groupby(["task", "site", "step_name"]).agg(
        (100 * (_sum("jobCPU") / _mean("nthreads")) / _sum("jobTime")).alias("avg_cpueff"),
        _count(lit(1)).alias("#jobs"),
        _mean("steps_len").alias("#steps"),
        _mean("nthreads").alias("#nthreads"),
        _mean("ncores").alias("#ncores"),
        (_sum("jobCPU") / _count(lit(1))).alias("avg_jobCPU"),
        (_sum("jobTime") / _count(lit(1))).alias("avg_jobTime"),
        _collect_set("acquisitionEra").alias("acquisitionEra"),
    ).withColumn("avg_cpueff", _col("avg_cpueff").cast(IntegerType())).toPandas()
    df_task = df.groupby(["task"]).agg(
        (100 * (_sum("jobCPU") / _mean("nthreads")) / _sum("jobTime")).alias("avg_cpueff"),
        _count(lit(1)).alias("#jobs"),
        _mean("steps_len").alias("#steps"),
        _mean("nthreads").alias("#nthreads"),
        _mean("ncores").alias("#ncores"),
        (_sum("jobCPU") / _count(lit(1))).alias("avg_jobCPU"),
        (_sum("jobTime") / _count(lit(1))).alias("avg_jobTime"),
    ).withColumn("avg_cpueff", _col("avg_cpueff").cast(IntegerType())).toPandas()
    write_htmls(df_details, df_task, start_date, end_date, output_folder)


if __name__ == "__main__":
    main()
