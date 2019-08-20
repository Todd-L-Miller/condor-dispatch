# condor-dispatch

A demonstration of using condor_ssh_to_job to dispatch work off a queue.  If you have a command-line you want to run many times, but each instance only takes a few seconds, this approach will probably be faster than submitting that many jobs.  Assumes your environment includes a running HTCondor pool and that you've included the HTCondor Python bindings in your PYTHONPATH.
