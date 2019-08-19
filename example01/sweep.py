#!/usr/bin/env python

import os
import sys
import time
import select
import subprocess

import classad
import htcondor
import dispatch

pairs = []
for x in range(0, 10):
    for y in range(0, 10):
        pairs.append("{0} {1}".format(x, y))

results = dispatch.sweep("./sweep-binary.sh", pairs, ["./sweep-binary.sh"], 8)

for pair in pairs:
    if results[pair] != pair:
        print("FAILURE: '{0}' => '{1}'".format(pair, results[pair]))
        break;
    else:
        print("SUCCESS")
        sys.exit(0)

sys.exit(1)
