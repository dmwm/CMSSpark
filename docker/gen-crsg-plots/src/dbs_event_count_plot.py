#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position,C0330

"""
File        : dbs_event_count_plot.py
Author      : Christian Ariza <christian.ariza AT gmail [DOT] com>
Description : Create the event count plot used, for example, in the C-RSG report.
                For additional documentation look at the notebook in the CMSSpark/src/notebooks folder.
Notes       : We disabled wrong-import-position because matplotlib needs to setup the backend before pyplot is imported.
                We disabled C0330 because pylint complains following the
                old recommendation. Black follows the new indentation recommendation.
"""

# system modules
import os
import click
import json
import logging
from datetime import timedelta, date, datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, concat, year, month, lpad

# Matplotlib needs to set the backend before pyplot is imported. That will
# cause pylint complain about the imports not being at top of file.
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from helpers import spark_utils

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
_VALID_DATE_FORMATS = ["%Y/%m"]
_VALID_TYPES = ["pdf", "png", "jpg", "svg"]


def plot_tiers_month(data, colors_file=None, attributes=None):
    """Create a stacked bar plot of events by data tier/month.

    args:
        - data: pandas dataframe with the month, data_tier_name and nevents columns.
    """
    data_by_month_tier = data
    data_by_month_tier.month = data.month.astype(int)
    fig, plot_ax = plt.subplots(figsize=(20, 7))
    months = pd.DataFrame(
        {
            "month": pd.Series(
                data_by_month_tier.month.unique().astype(int), name="month"
            )
        }
    )
    logger.debug(months.dtypes)
    tiers = data_by_month_tier.data_tier_name.unique()
    totals = (
        data_by_month_tier[["data_tier_name", "nevents"]]
        .groupby("data_tier_name")
        .sum()
        .reset_index()
    )
    totals["nevents"] = totals["nevents"].map("{:,d}".format)
    totals["new_label"] = totals["data_tier_name"].str.cat(
        totals["nevents"].values.astype(str), sep=" nevents: "
    )
    label_replacements = dict(zip(totals.data_tier_name, totals.new_label))
    data_by_month_tier = data_by_month_tier.replace(
        {"data_tier_name": label_replacements}
    )
    pivot_df = data_by_month_tier.pivot(
        index="month", columns="data_tier_name", values="nevents"
    )
    plt.title(
        f"Event count plot from {data_by_month_tier.month.min()} to {data_by_month_tier.month.max()}"
    )
    _default_colors = sns.color_palette("husl", len(tiers))
    try:
        colors = _default_colors if not colors_file else json.load(colors_file)
        logger.info("colors before replacements %s", colors)
        if isinstance(colors, dict):
            _c_r = {label_replacements[k]: colors.get(k) for k in label_replacements}
            colors = [_c_r[k] for k in pivot_df.columns]
        logger.info("colors after replacements %s", colors)
    except (json.JSONDecodeError, KeyError) as err:
        # If the file is not a valid json,
        # or the keys doesn't correspond to the tiernames,
        # use the random palette.
        logger.error(
            "There was a problem while reading the colors file. %s, %s",
            colors_file,
            err,
        )
        colors = _default_colors
    plt.xlabel("Month")
    plt.ylabel("Event count")
    # set matplotlib rcParams based on provided attributes for the plot
    # https://matplotlib.org/stable/api/matplotlib_configuration_api.html#matplotlib.rc
    if attributes:
        for key, kwds in attributes.items():
            matplotlib.rc(key, **kwds)
    pivot_df.plot.bar(stacked=True, color=colors, ax=plot_ax).legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
    return fig


def get_events_by_tier_month(spark, start_date, end_date,
                             tiers_raw=None, remove_raw=None, skims_raw=None, only_valid_files=False, verbose=False):
    """Generate a pandas dataframe containing data_tier_name, month, nevents for the given time period.

        It will add virtual tiers based on the skims.
        args:
            - spark: Spark session
            - start_date: String with the date y format yyyy/MM/dd
            - end_date: String with the date y format yyyy/MM/dd
            - tiers_raw: List of tiers
            - remove_raw: List of remove patterns
            - skims_raw: List of skim patterns
            - only_valid_files: True if you want to take into account only the valid files.
            - verbose: True if you want additional output messages, default False.
    """
    if tiers_raw is None:
        tiers_raw = [".*"]
    if skims_raw is None:
        skims_raw = []
    if remove_raw is None:
        remove_raw = []
    tiers = "^({})$".format("|".join(["{}".format(tier.strip()) for tier in tiers_raw]))
    skims_rlike = (
        ".*-({})-.*".format("|".join([elem.strip() for elem in skims_raw]))
        if skims_raw
        else "^$"
    )
    remove_rlike = (
        ".*({}).*".format("|".join([elem.strip().lower() for elem in remove_raw]))
        if remove_raw
        else "^$"
    )
    tables = spark_utils.dbs_tables(spark, tables=["ddf", "bdf", "fdf", "dtf"])
    if verbose:
        logger.info("remove %s", remove_rlike)
        logger.info("skims %s", skims_rlike)
        for k in tables:
            # tables[k].cache()
            logger.info(k)
            tables[k].printSchema()
            tables[k].show(5, truncate=False)
    datablocks_file_events_df = spark.sql(
        """SELECT sum(fdf.f_event_count) as f_event_count,
                             max(ddf.d_data_tier_id) as d_data_tier_id,
                             d_dataset,
                             b_block_name,
                             max(b_creation_date) as b_creation_date,
                             max(b_block_size) as size
                          FROM ddf JOIN bdf on ddf.d_dataset_id = bdf.b_dataset_id
                                   JOIN fdf on bdf.b_block_id = fdf.f_block_id
                          WHERE d_is_dataset_valid = 1
                          {}
                          group by d_dataset, b_block_name
                      """.format(
            "AND f_is_file_valid = 1" if only_valid_files else ""
        )
    )
    fiter_field = "b_creation_date"
    datablocks_file_events_df = (
        datablocks_file_events_df.withColumn(fiter_field, from_unixtime(fiter_field))
        .filter(
            fiter_field
            + " between '{}' AND '{}' ".format(
                start_date.replace("/", "-"), end_date.replace("/", "-")
            )
        )
        .withColumn(
            "month", concat(year(fiter_field), lpad(month(fiter_field), 2, "0"))
        )
    )

    datablocks_file_events_df.registerTempTable("dbfe_df")
    # Union of two queries:
    # - The first query will get all the selected data tiers,
    #   excluding the datasets who match the skims
    # - The second query will get all the selected data tiers,
    #   but only the dataset who match the skims.
    grouped = spark.sql(
        """
        select month, data_tier_name, sum(f_event_count) as nevents
        from dbfe_df join dtf on data_tier_id = d_data_tier_id
        where
            data_tier_name rlike '{tiers}'
            and lower(d_dataset) not rlike '{remove}'
            and d_dataset not rlike '{skims}'
            group by month, data_tier_name
        UNION
        select month,
             concat(data_tier_name, '/',regexp_extract(d_dataset,'{skims}',1)) AS data_tier_name,
             sum(f_event_count) as nevents
        from dbfe_df join dtf on dtf.data_tier_id = d_data_tier_id
        where
            data_tier_name rlike '{tiers}'
            and lower(d_dataset) not rlike '{remove}'
            and d_dataset rlike '{skims}'
            group by month, concat(data_tier_name, '/',regexp_extract(d_dataset,'{skims}',1))
        """.format(
            tiers=tiers, remove=remove_rlike, skims=skims_rlike
        )
    )
    return grouped.toPandas()


def event_count_plot(start_date, end_date, output_folder, output_formats, tiers, remove_patterns, skims,
                     colors_file=None, generate_csv=False, only_valid_files=False, attributes=None, verbose=False):
    """
    args:
        - start_date: String with the start date in format yyyy/MM/dd
        - end_date: String with the end date format yyyy/MM/dd
        - output_folder: Path of the output folder.
        - output_formats: list of formata like [png, pdf]
        - tiers: List of tiers
        - remove_patterns: List of remove patterns
        - skims: List of skim patterns
        - generate_csv: save the pandas dataframe to csv
        - only_valid_files: True if you want to take into account only the valid files.
        - verbose: True if you want additional output messages, default False.
    """
    spark = SparkSession.builder.appName("cms_dbs_event_count").getOrCreate()
    event_count_pdf = get_events_by_tier_month(
        spark,
        start_date,
        end_date,
        tiers,
        remove_patterns,
        skims,
        only_valid_files,
        verbose,
    )
    start_date_f = event_count_pdf.month.min()
    end_date_f = event_count_pdf.month.max()
    events_fig = plot_tiers_month(event_count_pdf, colors_file, attributes)

    os.makedirs(output_folder, exist_ok=True)
    if generate_csv:
        csv_filename = f"event_count_{start_date_f}-{end_date_f}.csv"
        event_count_pdf.to_csv(os.path.join(output_folder, csv_filename))

    image_paths = []
    for output_format in output_formats:
        image_filename = f"event_count_{start_date_f}-{end_date_f}.{output_format}"
        image_path = os.path.join(output_folder, image_filename)
        events_fig.savefig(image_path, format=output_format, bbox_inches='tight')
        image_paths.append(os.path.abspath(image_path))
    return image_paths


@click.command()
@click.option("--start_month", default=None, type=click.DateTime(_VALID_DATE_FORMATS),
              help="Start month in format yyyy/MM, defaults to: end_month - 11 months (i.e. one year period)")
@click.option("--end_month", default=None, type=click.DateTime(_VALID_DATE_FORMATS),
              help="End month (inclusive) in format yyyy/MM, defaults to previous month")
@click.option("--output_folder", default="./output", help="Output folder for the plots")
@click.option("--output_format", default="png", type=click.Choice(_VALID_TYPES), help="Output format for the plots")
@click.option("--colors_file", default=None, type=click.File('r'),
              help="A json file either with a list of colors (strings), or with a mapping of label and color. "
                   "If the file is not valid, or is not provided, a default palette will be generated.")
@click.option("--tiers", type=str,
              default="GEN,GEN-SIM,GEN-RAW,GEN-SIM-RECO,AODSIM,MINIAODSIM,"
                      "RAWAODSIM,NANOAODSIM,GEN-SIM-DIGI-RAW,GEN-SIM-RAW,GEN-SIM-DIGI-RECO",
              help="Comma separated list of tiers to consider. eg: GEN,GEN-SIM,GEN-RAW,GEN-SIM-RECO,AODSIM,MINIAODSIM")
@click.option("--remove", default="test,backfill,jobrobot,sam,bunnies,penguins",
              help="Comma separated list of case insensitive patterns. "
                   "The datasets which name match any of the patterns will be ignored.")
@click.option("--skims", default="",
              help="Comma separated list of skims. The skims are case sensitive. Datasets which match the given skims "
                   "will not be counted as part of the tier, but in a separated group named <tier>/<skim>.")
@click.option("--attributes", default=None, help="matplotlib rc params file (JSON format)")
@click.option("--generate_csv", is_flag=True, default=False, help="Create also a csv file with the plot data")
@click.option("--only_valid_files", is_flag=True, default=False, help="Only consider valid files, default False")
@click.option("--test", is_flag=True, default=False, help="Only consider valid files, default False")
@click.option("--verbose", is_flag=True, default=False, help="Prints additional logging info")
def main(start_month, end_month, output_folder, output_format, colors_file, tiers, remove, skims, generate_csv,
         only_valid_files, attributes, test, verbose):
    """Main function"""
    # This script create Event Count Plots based on the dbs data. It prints the path of the created image in std output.
    click.echo('--------------------------------------------------------------------------------------------')
    click.echo(f'Input Arguments: start_month:{start_month}, end_month:{end_month}, '
               f'output_folder:{output_folder}, output_format:{output_format}, colors_file:{colors_file}, '
               f'tiers:{tiers}, remove:{remove}, skims:{skims}, generate_csv:{generate_csv}, '
               f'only_valid_files:{only_valid_files}, attributes:{attributes}, verbose:{verbose}')
    click.echo('--------------------------------------------------------------------------------------------')
    if verbose:
        logger.setLevel(logging.INFO)
    if not end_month:
        previous_month = date.today().replace(day=1) - timedelta(days=1)
        end_month = previous_month.strftime("%Y/%m")
    if not start_month:
        _end_date = datetime.strptime(f"{end_month}/01", "%Y/%m/01")
        _start_date = _end_date - relativedelta(months=11)
        start_month = _start_date.strftime("%Y/%m")

    tiers = tiers.split(",")
    skims = skims.split(",")
    remove = remove.split(",")
    start_date = f"{start_month}/01"
    if test:
        # If test, give start_month as 1 month ago in bash script, this will handle the rest
        start_date = f"{start_month}/27"

    # The query to the data exclude the last day,
    # so we will query to the first day of the next month
    _end_date = datetime.strptime(f"{end_month}/01", "%Y/%m/01") + relativedelta(months=1)
    end_date = _end_date.strftime("%Y/%m/%d")
    # load rc param attributes from a given file
    if attributes:
        with open(attributes, 'r') as istream:
            attributes = json.load(istream)
    # always generate pdf by default in addition to given output format
    output_formats = ["pdf"]
    if output_format != "pdf":
        output_formats.append(output_format)
    image_file_names = event_count_plot(
        start_date,
        end_date,
        output_folder,
        output_formats,
        tiers,
        remove,
        skims,
        colors_file=colors_file,
        generate_csv=generate_csv,
        only_valid_files=only_valid_files,
        attributes=attributes,
        verbose=verbose,
    )
    for filename in image_file_names:
        print(filename)
        # Write filename to some generic file which will be written by bash script to create symbolic link
        with open(os.path.join(output_folder, output_format + "_output_path_for_ln.txt"), "w+") as f:
            f.write(filename)


if __name__ == "__main__":
    main()
