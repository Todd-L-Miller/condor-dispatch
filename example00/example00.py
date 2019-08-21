#!/usr/bin/env python

import sys
import time

import classad
import htcondor

logfile = "sweep/log"
sub = htcondor.Submit({
    "executable": "sweep-binary.sh",
    "should_transfer_files": True,
    "output": "sweep/$(PROCID).out",
    "log": logfile
})

pairs = []
for x in range(0, 10):
    for y in range(0, 10):
        pairs.append("{0} {1}".format(x, y))
arguments = [ { "arguments": pair } for pair in pairs ]

start = time.time()
schedd = htcondor.Schedd()
with schedd.transaction() as txn:
    sr = sub.queue_with_itemdata( txn, 1, arguments )
    print("Completed submit to cluster {0}".format(sr.cluster()))

jobTerminatedCount = 0
jel = htcondor.JobEventLog(logfile)
for event in jel.events(None):
    if event.cluster == sr.cluster():
        if event.type is htcondor.JobEventType.EXECUTE:
            print("Submit-to-startup time: {0} seconds".format(time.time()-start))
        if event.type is htcondor.JobEventType.JOB_TERMINATED:
            sys.stdout.write(".")
            sys.stdout.flush()
            jobTerminatedCount += 1
            if jobTerminatedCount == 100:
                end = time.time()
                print
                print("Last completed, in {0} seconds".format(end-start))
                sys.exit(0)

sys.exit(1)
