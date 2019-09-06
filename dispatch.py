import os
import sys
import time
import atexit
import select
import tempfile
import subprocess

import classad
import htcondor

"""Runs a lot of short jobs very quickly."""


def dispatch(commands, files, count=1):
    """Transfers each file in files to one or more sandboxes, then runs each
       command in commands in one such sandbox."""
    jobhash = _make_default_jobhash(files)
    return dispatch_with_job(commands, jobhash, count)


def dispatch_with_job(commands, jobhash, count=1):
    """Submits one or more jobs to HTCondor (which may transfer files), then
       runs each command in one of the jobs."""
    return _main_select_loop(None, commands[:], jobhash, count)


def sweep(command, arguments, files, count=1):
    """Transfers each file in files to one or more sandboxes, then runs
       command in one such sandbox once for each argument list in arguments."""
    jobhash = _make_default_jobhash(files)
    return sweep_with_job(command, arguments, jobhash, count)


def sweep_with_job(command, arguments, jobhash, count=1):
    """Submits one or more jobs to HTCondor (which may transfer files), then
       runs command in one of the jobs for each argument list in arguments."""
    return _main_select_loop(command, arguments[:], jobhash, count)


# ----------------------------------------------------------------------------


def _make_default_jobhash(files):
    jobhash = {
        "executable": "/bin/sleep",
        "arguments": "300",
        "transfer_executable": False,
        "should_transfer_files": True,
    }
    (fd, logfile) = tempfile.mkstemp()
    os.close(fd)
    atexit.register(os.unlink, logfile)
    jobhash["log"] = logfile
    jobhash["transfer_input_files"] = ",".join(files)
    return jobhash


def _open_ssh_pipes(cluster, proc):
    ssh = subprocess.Popen(
        ["condor_ssh_to_job", "{0}.{1}".format(cluster, proc)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    # We can't use communicate() because we need to do it more than once.
    return ssh.stdin, ssh.stdout


def _remove_job(schedd, cluster, message):
    schedd.act(htcondor.JobAction.Remove, "ClusterId == {0}".format(cluster), message)


def _main_select_loop(prefix, commands, jobhash, count):
    logfile = jobhash["log"]
    start = time.time()
    sub = htcondor.Submit(jobhash)
    schedd = htcondor.Schedd()
    with schedd.transaction() as txn:
        cluster = sub.queue(txn, count)
    ##
    print("Completed submit to cluster {0}".format(cluster))
    atexit.register(_remove_job, schedd, cluster, "dispatch cleaning up")
    jel = htcondor.JobEventLog(logfile)

    ready, pipesTo, pipesFro = [], [], []
    results, froToResultMap, correspondingToPipe = {}, {}, {}

    while True:
        for event in jel.events(0):
            if event.cluster == cluster:
                if event.type is htcondor.JobEventType.EXECUTE:
                    ##
                    print(
                        "Submit-to-startup time: {0} seconds".format(
                            time.time() - start
                        )
                    )
                    to, fro = _open_ssh_pipes(cluster, event.proc)
                    pipesTo.append(to)
                    pipesFro.append(fro)
                    correspondingToPipe[fro] = to

        # Check for pipe events.
        ready, _, _ = select.select(pipesFro, pipesTo, [], 0)

        for f in ready:
            result = f.readline()
            if result.startswith("Welcome to"):
                f.readline()
            else:
                result = result.strip()
                results[froToResultMap[f]] = result

            t = correspondingToPipe[f]
            if len(commands) != 0:
                froToResultMap[f] = commands.pop()
                message = froToResultMap[f]
                if not message.endswith("\n"):
                    message += "\n"
                if prefix is None:
                    t.write(message)
                else:
                    t.write("{0} {1}".format(prefix, message))
                t.flush()
            else:
                pipesTo.remove(t)
                pipesFro.remove(f)

            if len(pipesTo) == 0 or len(pipesFro) == 0:
                ##
                print("Jobs complete in {0} seconds".format(time.time() - start))
                _remove_job(schedd, cluster, "dispatch complete")
                return results

        time.sleep(0.01)


if __name__ == "__main__":
    # Tests not implemented.

    # We could also permit something like:
    #   $ script | condor_dispatch [submit-file]
    # where the script produces a newline-separated sequence of commands to
    # dispatch, but that may be better suited, and more idiomatic, as its
    # own wrapper script.
    pass
