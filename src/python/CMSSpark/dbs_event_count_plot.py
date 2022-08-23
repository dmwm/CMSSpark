#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=wrong-import-position,C0330

"""
File        : dbs_event_count_plot.py
Author      : Christian Ariza <christian.ariza AT gmail [DOT] com>
Description : Create the event count plot used, for example, in the C-RSG report.
                For additional documentation look at the notebook in the CMSSpark/src/notebooks folder.
Notes       : We disabled wrong-import-position because matplotlib needs setup the backend before pyplot is imported.
                We disabled C0330 because pylint complains following the
                old recommendation. Black follows the new indentation recommendation.
"""

# system modules
import os
import argparse
import json
import logging
from datetime import timedelta, date, datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_unixtime,
    concat,
    year,
    month,
    lpad,
)

# Matplotlib needs to set the backend before pyplot is imported. That will
# cause pylint complain about the imports not being at top of file.
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

# CMSSpark modules
from CMSSpark import spark_utils

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


def valid_date(str_date):
    """Is the string a valid date in the desired format?"""
    try:
        datetime.strptime(f"{str_date}/01", "%Y/%m/%d")
        return str_date
    except ValueError:
        msg = "Not a valid month: '{0}'.".format(str_date)
        raise argparse.ArgumentTypeError(msg)


class OptionParser:
    """Custom option parser."""

    def __init__(self):
        """User based option parser"""
        desc = "This script create Event Count Plots based on the dbs data. " \
               "It prints the path of the created image in std output."
        self.parser = argparse.ArgumentParser(prog="DBS Event Count Plot", usage=desc)
        self.parser.add_argument(
            "--start_month",
            action="store",
            dest="start_month",
            default=None,
            help="""Start month in format yyyy/MM,
            defaults to: end_month - 11 months (i.e. one year period)""",
            type=valid_date,
        )
        self.parser.add_argument(
            "--end_month",
            action="store",
            dest="end_month",
            default=None,
            help="End month (inclusive) in format yyyy/MM, defaults to previous month",
            type=valid_date,
        )
        self.parser.add_argument(
            "--output_folder",
            action="store",
            dest="output_folder",
            default="./output",
            help="output folder for the plots",
        )
        self.parser.add_argument(
            "--output_format",
            action="store",
            dest="output_format",
            choices=["pdf", "png", "jpg", "svg"],
            default="png",
            help="output format for the plots",
        )
        self.parser.add_argument(
            "--colors_file",
            action="store",
            default=None,
            type=argparse.FileType("r"),
            help="""A json file either with a list of colors (strings), 
            or with a mapping of label and color. 
            If the file is not valid, or is not provided,
            a default palette will be generated.""",
        )
        self.parser.add_argument(
            "--tiers",
            action="store",
            nargs="*",
            dest="tiers",
            default=[
                "GEN",
                "GEN-SIM",
                "GEN-RAW",
                "GEN-SIM-RECO",
                "AODSIM",
                "MINIAODSIM",
                "RAWAODSIM",
                "NANOAODSIM",
                "GEN-SIM-DIGI-RAW",
                "GEN-SIM-RAW",
                "GEN-SIM-DIGI-RECO",
            ],
            help="""
            Space separated list of tiers to consider.
            eg:
            GEN GEN-SIM GEN-RAW GEN-SIM-RECO AODSIM MINIAODSIM RAWAODSIM NANOAODSIM GEN-SIM-DIGI-RAW GEN-SIM-RAW GEN-SIM-DIGI-RECO
            """,
        )
        self.parser.add_argument(
            "--remove",
            action="store",
            nargs="*",
            dest="remove",
            default="test,backfill,jobrobot,sam,bunnies,penguins".split(","),
            help="""
            Space separed list of case insensitive patterns.
            The datasets which name match any of the patterns will be ignored.
            """,
        )
        self.parser.add_argument(
            "--skims",
            action="store",
            nargs="*",
            dest="skims",
            default=[],
            help="""
            Space separated list of skims. The skims are case sensitive.
            Datasets which match the given skims will not be counted
            as part of the tier, but in a separated group named <tier>/<skim>.
            """,
        )
        self.parser.add_argument(
            "--generate_csv",
            action="store_true",
            help="Create also a csv file with the plot data",
            default=False,
        )
        self.parser.add_argument(
            "--only_valid_files",
            action="store_true",
            help="Only consider valid files, default False",
            default=False,
        )
        self.parser.add_argument(
            "--attributes",
            action="store",
            dest="attributes",
            help="matplotlib rc params file (JSON format)",
            default=None,
        )
        self.parser.add_argument(
            "--verbose",
            action="store_true",
            help="Prints additional logging info",
            default=False,
        )


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


def event_count_plot(start_date, end_date, output_folder, output_format, tiers, remove_patterns, skims,
                     colors_file=None, generate_csv=False, only_valid_files=False, attributes=None, verbose=False):
    """
    args:
        - start_date: String with the start date in format yyyy/MM/dd
        - end_date: String with the end date format yyyy/MM/dd
        - output_folder: Path of the output folder.
        - output_format: png/pdf
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
    image_filename = f"event_count_{start_date_f}-{end_date_f}.{output_format}"
    os.makedirs(output_folder, exist_ok=True)
    if generate_csv:
        csv_filename = f"event_count_{start_date_f}-{end_date_f}.csv"
        event_count_pdf.to_csv(os.path.join(output_folder, csv_filename))
    events_fig = plot_tiers_month(event_count_pdf, colors_file, attributes)
    image_path = os.path.join(output_folder, image_filename)
    events_fig.savefig(image_path, format=output_format, bbox_inches='tight')
    return os.path.abspath(image_path)


def main():
    """Main function"""
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    if opts.verbose:
        logger.setLevel(logging.INFO)
        logger.info("%s", opts)
    if not opts.end_month:
        previous_month = date.today().replace(day=1) - timedelta(days=1)
        opts.end_month = previous_month.strftime("%Y/%m")
    if not opts.start_month:
        _end_date = datetime.strptime(f"{opts.end_month}/01", "%Y/%m/01")
        _start_date = _end_date - relativedelta(months=11)
        opts.start_month = _start_date.strftime("%Y/%m")
    start_date = f"{opts.start_month}/01"
    # The query to the data exclude the last day,
    # so we will query to the first day of the next month
    _end_date = datetime.strptime(f"{opts.end_month}/01", "%Y/%m/01") + relativedelta(
        months=1
    )
    end_date = _end_date.strftime("%Y/%m/%d")
    # load rc param attributes from a given file
    attributes = None
    if opts.attributes:
        with open(opts.attributes, 'r') as istream:
            attributes = json.load(istream)
    # always generate pdf by default in addition to given output format
    output_formats = ["pdf"]
    if opts.output_format != "pdf":
        output_formats.append(opts.output_format)
    for output_format in output_formats:
        filename = event_count_plot(
            start_date,
            end_date,
            opts.output_folder,
            output_format,
            opts.tiers,
            opts.remove,
            opts.skims,
            colors_file=opts.colors_file,
            generate_csv=opts.generate_csv,
            only_valid_files=opts.only_valid_files,
            attributes=attributes,
            verbose=opts.verbose,
        )
        print(filename)


if __name__ == "__main__":
    main()
