#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : stats.py
Author      : Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
Description : ...
"""

# system modules
import argparse

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


class OptionParser:
    def __init__(self):
        """User based option parser"""
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
                                 dest="fin", default="", help="Input file")
        self.parser.add_argument("--agg", action="store",
                                 dest="agg", default="release",
                                 help="Aggregate by (release, era, tier, primds, procds)")
        self.parser.add_argument("--cols", action="store",
                                 dest="cols", default="evts,size,pbr_size", help="Output file")
        self.parser.add_argument("--sort_col", action="store",
                                 dest="sort_col", default="pbr_size", help="Output file")
        self.parser.add_argument("--verbose", action="store_true",
                                 dest="verbose", default=False, help="verbose output")


def pie(pdf, gb, data, agg, site):
    """Make a pie chart from groupby and data dataframes"""
    labels = []
    for name, group in gb:
        labels.append(name)

    # perform plotting
    plt.subplot()
    patches, texts, autotexts = \
        plt.pie(data['pbr_size'], labels=None, autopct='%1.1f%%', shadow=True)
    for t in texts:
        t.set_size('smaller')
    for t in autotexts:
        t.set_size('x-small')

    plt.title('Site: %s, replica size by %s aggregation' % (site, agg))
    pdf.savefig()
    plt.close()


def groupby(pdf, tdf, agg, site, cols, sort_col):
    """Group by given Dataframe by aggregation parameter"""
    pbytes = np.power(1024, 5)
    gb = tdf.groupby(agg)
    data = gb[cols].agg(np.sum)
    sorted_data = data.sort_values(sort_col, ascending=False)
    tot_evts = np.sum(data['evts'])
    tot_size = np.sum(data['size'])
    tot_pbsize = np.sum(data['pbr_size'])
    psize = '%7.3f PB' % (tot_size / pbytes,)
    pbsize = '%7.3f PB' % (tot_pbsize / pbytes,)
    msg = "\n%s, evts=%s, dataset size=%.3e (%s), replica size=%.3e (%s)" \
          % (site, tot_evts, tot_size, psize.strip(), tot_pbsize, pbsize.strip())
    print(msg)
    print('-' * (len(msg) - 1))
    with pd.option_context('display.max_rows', None):
        print(sorted_data)
    pie(pdf, gb, data, agg, site)


def summary(fname, agg, cols, sort_col):
    df = pd.read_csv(fname)
    sites = np.unique(df.node_name)
    ndf1 = df[df.node_name.str.contains('T1_')]
    ndf2 = df[df.node_name.str.contains('T2_')]
    ndf3 = df[df.node_name.str.contains('T3_')]

    with PdfPages('%s_all_sites.pdf' % agg) as pdf:
        for idx, tdf in enumerate([ndf1, ndf2, ndf3]):
            site = 'T%s-sites' % (idx + 1)
            groupby(pdf, tdf, agg, site, cols, sort_col)

    print("\n### Site breakdown ###\n")

    with PdfPages('%s.pdf' % agg) as pdf:
        for site in sorted(sites):
            ndf = df[df.node_name == site]
            groupby(pdf, ndf, agg, site, cols, sort_col)


def main():
    """Main function"""
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    agg = opts.agg
    if agg.find('release') != -1:
        agg = 'r_release_version'
    elif agg.find('era') != -1:
        agg = 'acquisition_era_name'
    elif agg.find('tier') != -1:
        agg = 'tier'
    else:
        raise Exception("not supported aggregation")
    cols = [c.strip() for c in opts.cols.split(',')]
    summary(opts.fin, agg, cols, opts.sort_col)


if __name__ == '__main__':
    main()
