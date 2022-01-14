#!/usr/bin/env python
# Main Author: David LANGE

"""Creates pickle file rse name and id map, to be used in rucio_datasets_last_access_ts.py to get only DISK rse ids"""

# Requirements:
#    - Rucio cli setup: https://twiki.cern.ch/twiki/bin/view/CMS/Rucio
#    - In most of the Virtual Machines we cannot run this file because it requires Rucio CLI setup.
#    - Should be run manually

import os
import pickle
from subprocess import Popen, PIPE


def runCommand(command):
    """Run subprocess"""
    p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    pipe = p.stdout.read()
    errpipe = p.stderr.read()
    tupleP = os.waitpid(p.pid, 0)
    eC = tupleP[1]
    return eC, pipe.decode(encoding='UTF-8'), errpipe.decode(encoding='UTF-8')


rucio_list_comm = "rucio list-rses"
ec, cOut, cErr = runCommand(rucio_list_comm)
rses = {}
for line in cOut.split():
    rse = str(line.strip())
    print(rse)
    comm = "rucio-admin rse info " + rse
    ec2, cOut2, cErr2 = runCommand(comm)
    rse_id = None
    for l2 in cOut2.split('\n'):
        if "id: " in l2:
            rse_id = l2.split()[1]
            break
    print(rse_id)
    rses[rse] = rse_id

# ATTENTION: Python2 do not support default protocol of Python3 which is 3
# Pickle file "rses.pickle" will be writtent to current directory
with open("rses.pickle", "wb+") as f:
    pickle.dump(rses, f, protocol=2)
