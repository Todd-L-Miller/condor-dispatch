#!/usr/bin/env python

import os
import sys
import time
import select
import subprocess

import classad
import htcondor
import dispatch

jobhash = {
    "executable": "/bin/sleep",
    "arguments": "300",
    "transfer_input_files": "sweep-binary.sh",
    "transfer_executable": False,
    "should_transfer_files": True,
    "output": "sweep/$(PROCID).out",
    "error": "sweep/$(PROCID).err",
    "log": "sweep/log"
}

pairs = []
for x in range(0, 10):
    for y in range(0, 10):
        pairs.append("{0} {1}".format(x, y))

results = dispatch.sweep_with_job("./sweep-binary.sh", pairs, jobhash, 8)

for pair in pairs:
    if results[pair] != pair:
        print("FAILURE: '{0}' => '{1}'".format(pair, results[pair]))
        break;
    else:
        print("SUCCESS")
        sys.exit(0)

sys.exit(1)
