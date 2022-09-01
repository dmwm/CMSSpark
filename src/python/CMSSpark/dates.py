#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : dates.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : ...
"""

# system modules
import click
import datetime


def dates(start, numdays):
    base = datetime.datetime.today()
    if start:
        year = int(start[:4])
        month = int(start[4:6])
        day = int(start[6:])
        base = datetime.datetime(year, month, day)
    date_list = [base - datetime.timedelta(days=x) for x in range(0, numdays)]
    return date_list


def range_dates(start, numdays):
    base = datetime.datetime.today()
    if start:
        year = int(start[:4])
        month = int(start[4:6])
        day = int(start[6:])
        base = datetime.datetime(year, month, day)
    for step in range(0, numdays):
        yield base - datetime.timedelta(days=step)


def dformat(date, iformat):
    return date.strftime(iformat)


@click.command()
@click.option("--start", default="", help="Dstart date (YYYYMMDD)")
@click.option("--ndays", default=30, help="Number of days, default 30")
@click.option("--format", "format_", default="%Y-%m-%d", help="date format, e.g. %%Y%%m%%d")
@click.option("--range", "range_", default=False, is_flag=True, help="show full range")
def main(start, ndays, format_, range_):
    """Main function"""
    click.echo("dates")
    click.echo(f'Input Arguments: start:{start}, ndays:{ndays}, format:{format_}, range:{range_}')
    if range_:
        for date in range_dates(start, int(ndays)):
            print(dformat(date, format_))
    else:
        date_list = dates(start, int(ndays))
        min_date, max_date = date_list[-1], date_list[0]
        print('%s %s' % (dformat(min_date, format_), dformat(max_date, format_)))


if __name__ == '__main__':
    main()
