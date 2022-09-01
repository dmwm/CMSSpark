#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File        : jm_stats.py
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
        self.parser.add_argument("--fout", action="store",
                                 dest="fout", default="", help="Output file")
        self.parser.add_argument("--agg", action="store",
                                 dest="agg", default="", help="Aggregate by (SiteName,JobExecExitCode,FileType,Type)")
        self.parser.add_argument("--verbose", action="store_true",
                                 dest="verbose", default=False, help="verbose output")


def summary(fname, agg):
    pbytes = np.power(1024, 5)
    df = pd.read_csv(fname)
    sites = np.unique(df.SiteName)
    with PdfPages('%s.pdf' % agg) as pdf:
        for site in sorted(sites):
            ndf = df[df.SiteName == site]

            gb = ndf.groupby(agg)
            data = gb['tot_cpu', 'ecode_count', 'tot_wc'].agg(np.sum)
            msg = "\n%s" % site
            print(msg)
            print('-' * (len(msg) - 1))
            with pd.option_context('display.max_rows', None):
                print(data)

            labels = []
            for name, group in gb:
                labels.append(name)

            plt.subplot()
            patches, texts, autotexts = \
                plt.pie(data['tot_cpu'], labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
            for t in texts:
                t.set_size('smaller')
            for t in autotexts:
                t.set_size('x-small')

            plt.title('Site: %s, tot_cpu %s aggregation' % (site, agg))
            pdf.savefig()
            plt.close()


def main():
    """Main function"""
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    agg = opts.agg
    summary(opts.fin, agg)


if __name__ == '__main__':
    main()
