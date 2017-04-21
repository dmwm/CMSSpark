#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : agg.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import sys
import argparse

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.gridspec import GridSpec

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--fin", action="store",
            dest="fin", default="", help="Input file")
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help="Output file")
        self.parser.add_argument("--agg", action="store",
            dest="agg", default="release", help="Aggregate by (release, era, tier, primds, procds)")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def summary(fname, agg):
    pbytes = np.power(1024, 5)
    df = pd.read_csv(fname)
    sites = np.unique(df.node_name)
    with PdfPages('%s.pdf' % agg) as pdf:
        for site in sorted(sites):
            ndf = df[df.node_name==site]
            gb = ndf.groupby(agg)
            data = gb['evts', 'size', 'pbr_size'].agg(np.sum)
            tot_evts = np.sum(data['evts'])
            tot_size = np.sum(data['size'])
            tot_pbsize = np.sum(data['pbr_size'])
            psize = '%7.3f PB' % (tot_size/pbytes,)
            pbsize = '%7.3f PB' % (tot_pbsize/pbytes,)
            msg = "\n%s, evts=%s, dataset size=%.3e (%s), replica size=%.3e (%s)" \
                    % (site, tot_evts, tot_size, psize.strip(), tot_pbsize, pbsize.strip())
            print(msg)
            print('-'*(len(msg)-1))
            with pd.option_context('display.max_rows', None):
                print(data)

            labels = []
            for name, group in gb:
                labels.append(name)

            # perform plotting
            plt.subplot()
            patches, texts, autotexts = \
                    plt.pie(data['pbr_size'], labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)
	    for t in texts:
		t.set_size('smaller')
	    for t in autotexts:
		t.set_size('x-small')

            plt.title('Site: %s, replica size by %s aggregation' % (site, agg))
            pdf.savefig()
            plt.close()

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    agg = opts.agg
    if  agg.find('release') != -1:
        agg = 'r_release_version'
    elif agg.find('era') != -1:
        agg = 'acquisition_era_name'
    elif agg.find('tier') != -1:
        agg = 'tier'
    else:
        raise Exception("not supported aggregation")
    summary(opts.fin, agg)

if __name__ == '__main__':
    main()
