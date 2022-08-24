#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : hpc_at_cms.py
Author      : Author: Ceyhun Uzunoglu <ceyhunuzngl AT gmail [DOT] com>
Description : Generates HPC@CMS Running Cores Hourly plot for C-RSG
"""

# system modules
import os
from datetime import timedelta
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.types import (
    StructType,
    LongType,
    StringType,
    StructField,
    DoubleType,
)
import click

# global variables
from CMSSpark.spark_utils import get_candidate_files, get_spark_session

_BASE_HDFS_CONDOR = "/project/monitoring/archive/condor/raw/metric"
_VALID_DATE_FORMATS = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]


def _get_schema():
    return StructType(
        [
            StructField(
                "data",
                StructType(
                    [
                        StructField("GlobalJobId", StringType(), nullable=False),
                        StructField("RecordTime", LongType(), nullable=False),
                        StructField("JobFailed", LongType(), nullable=False),
                        StructField("Status", StringType(), nullable=True),
                        StructField("Site", StringType(), nullable=True),
                        StructField("GLIDEIN_Entry_Name", StringType(), nullable=True),
                        StructField("RequestCpus", DoubleType(), nullable=True),
                        StructField("RemoteHost", StringType(), nullable=True),
                    ]
                ),
            ),
        ]
    )


def get_hpc_at_cms(start_date, end_date, base=_BASE_HDFS_CONDOR):
    """Get data of required sites with their specific conditions defined in CMSMONIT-341 Jira ticket
    """
    schema = _get_schema()
    spark = get_spark_session(app_name="hpc-at-cms_plot")
    # start_date = datetime(2020, 11, 1)
    # end_date = datetime(2021, 1, 1)
    raw_df = (
        spark.read.option("basePath", base).json(
            get_candidate_files(start_date, end_date, spark, base, day_delta=3),
            schema=schema,
        ).select("data.*").filter(
            f"""Status='Completed'
              AND JobFailed=0
              AND RecordTime >= {start_date.timestamp() * 1000}
              AND RecordTime < {end_date.timestamp() * 1000}
              AND (
                      Site='T3_US_NERSC' OR
                      Site='T3_US_PSC' OR
                      Site='T3_US_SDSC' OR
                      Site='T3_US_TACC' OR
                      Site='T3_US_OSG' OR
                      Site='T1_IT_CNAF'
                  )
              """
        ).drop_duplicates(["GlobalJobId"])
    )
    df = raw_df.select("RecordTime", "RequestCpus", "RemoteHost", "Site", "GLIDEIN_Entry_Name").toPandas()

    # Pandas part
    # Condition list for each Site and its conditions defined in CMSMONIT-341 Jira ticket
    cond1 = ["CMSHTPC_T3_US_NERSC_Cori_SL7", "CMSHTPC_T3_US_NERSC_Cori", "CMSHTPC_T3_US_NERSC_Cori_SL7_PREM",
             "CMSHTPC_T3_US_NERSC_Cori_SL6_PREM"]
    cond2 = ["CMSHTPC_T3_US_NERSC_Cori_KNL_SL7", "CMSHTPC_T3_US_NERSC_Cori_KNL"]
    cond4 = "CMSHTPC_T3_US_SDSC_osg-comet"
    cond5 = "CMSHTPC_T3_US_SDSC-Expanse"
    cond6 = "OSG_US_TACC_JETSTREAM"
    cond7 = ["CMSHTPC_T3_US_TACC-Stampede2", "CMSHTPC_T3_US_TACC"]
    cond8 = "CMSHTPC_T3_US_TACC_FRONTERA"
    cond9 = "marconi.cineca.it"

    df.sort_values(by=['RecordTime'], inplace=True, ascending=True)
    df["RecordTimeDT"] = pd.to_datetime(df['RecordTime'], unit='ms', origin='unix')
    df.drop('RecordTime', axis=1, inplace=True)

    df1 = df[(df.Site == "T3_US_NERSC") & (df.GLIDEIN_Entry_Name.isin(cond1))][["RecordTimeDT", "RequestCpus"]]
    df2 = df[(df.Site == "T3_US_NERSC") & (df.GLIDEIN_Entry_Name.isin(cond2))][["RecordTimeDT", "RequestCpus"]]
    df3 = df[(df.Site == "T3_US_PSC")][["RecordTimeDT", "RequestCpus"]]
    df4 = df[(df.Site == "T3_US_SDSC") & (df.GLIDEIN_Entry_Name == cond4)][["RecordTimeDT", "RequestCpus"]]
    df5 = df[(df.Site == "T3_US_SDSC") & (df.GLIDEIN_Entry_Name == cond5)][["RecordTimeDT", "RequestCpus"]]
    df6 = df[((df.Site == "T3_US_TACC") | (df.Site == "T3_US_OSG")) & (df.GLIDEIN_Entry_Name == cond6)][
        ["RecordTimeDT", "RequestCpus"]]
    df7 = df[(df.Site == "T3_US_TACC") & (df.GLIDEIN_Entry_Name.isin(cond7))][["RecordTimeDT", "RequestCpus"]]
    df8 = df[(df.Site == "T3_US_TACC") & (df.GLIDEIN_Entry_Name == cond8)][["RecordTimeDT", "RequestCpus"]]
    df9 = df[((df.Site == "T1_IT_CNAF") & (df.RemoteHost.isna() | df.RemoteHost.str.endswith(cond9)))][
        ["RecordTimeDT", "RequestCpus"]]

    # Aggregations
    d1 = df1.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()
    d2 = df2.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()
    d3 = df3.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()
    d4 = df4.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()
    d5 = df5.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()
    d6 = df6.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()
    d7 = df7.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()
    d8 = df8.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()
    d9 = df9.groupby(pd.Grouper(key='RecordTimeDT', freq='1H')).sum()

    date_list = [start_date + timedelta(hours=x) for x in range(24 * (end_date - start_date).days)]
    dfy = pd.DataFrame(date_list, columns=["RecordTimeDT"])
    dfy.set_index("RecordTimeDT", inplace=True)
    dfy["tmp"] = 0
    dfy = dfy.groupby(pd.Grouper(freq='1H')).sum()
    dfy.drop('tmp', axis=1, inplace=True)

    d1 = pd.concat([dfy, d1], axis=1)
    d2 = pd.concat([dfy, d2], axis=1)
    d3 = pd.concat([dfy, d3], axis=1)
    d4 = pd.concat([dfy, d4], axis=1)
    d5 = pd.concat([dfy, d5], axis=1)
    d6 = pd.concat([dfy, d6], axis=1)
    d7 = pd.concat([dfy, d7], axis=1)
    d8 = pd.concat([dfy, d8], axis=1)
    d9 = pd.concat([dfy, d9], axis=1)

    d1.fillna(0, inplace=True)
    d2.fillna(0, inplace=True)
    d3.fillna(0, inplace=True)
    d4.fillna(0, inplace=True)
    d5.fillna(0, inplace=True)
    d6.fillna(0, inplace=True)
    d7.fillna(0, inplace=True)
    d8.fillna(0, inplace=True)
    d9.fillna(0, inplace=True)

    d1.rename(columns={'RequestCpus': 'T3_US_NERSC c1'}, inplace=True)
    d2.rename(columns={'RequestCpus': 'T3_US_NERSC c2'}, inplace=True)
    d3.rename(columns={'RequestCpus': 'T3_US_PSC c3'}, inplace=True)
    d4.rename(columns={'RequestCpus': 'T3_US_SDSC c4'}, inplace=True)
    d5.rename(columns={'RequestCpus': 'T3_US_SDSC c5'}, inplace=True)
    d6.rename(columns={'RequestCpus': 'T3_US_TACC OR T3_US_OSG c5'}, inplace=True)
    d7.rename(columns={'RequestCpus': 'T3_US_TACC c7'}, inplace=True)
    d8.rename(columns={'RequestCpus': 'T3_US_TACC c8'}, inplace=True)
    d9.rename(columns={'RequestCpus': 'T1_IT_CNAF c9'}, inplace=True)
    result = pd.concat([d1, d2, d3, d5, d6, d7, d8, d9], axis=1)
    return result


def generate_plot(result_df, output_folder, filename):
    """
        Generates the plot for given dataset
    """
    sns.set_style("darkgrid")
    sns.set_context("talk")
    plt.figure(figsize=(25, 20))
    plt.title(filename)
    plt.ylabel("Running Cores")
    fig = sns.lineplot(
        data=result_df,
        markers=True, dashes=False, legend="full", sizes=(.25, 2.5)
    ).get_figure()
    fig.savefig(os.path.join(output_folder, f"{filename}.png"))


@click.command()
@click.option("--start_date", type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--end_date", type=click.DateTime(_VALID_DATE_FORMATS))
@click.option("--output_folder", default="./output", help="local output directory")
def main(start_date, end_date, output_folder):
    """Main function
    """
    print("Start:", start_date, "End date:", end_date, "Output:", output_folder)
    result_df = get_hpc_at_cms(start_date, end_date)
    os.makedirs(output_folder, exist_ok=True)
    filename = f"HPC@CMS_Running_Cores_Hourly_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
    result_df.to_csv(os.path.join(output_folder, f"{filename}.csv"))
    generate_plot(result_df, output_folder, filename)


if __name__ == "__main__":
    main()
