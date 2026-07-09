"""Gunicorn hooks for prometheus_client multiprocess mode.

Enabled only when PROMETHEUS_MULTIPROC_DIR is set (see run_prod_gunicorn.sh).
The master wipes stale mmap files on start; dead workers' files are reaped so
their series stop contributing while live counters/gauges aggregate across
all workers and survive --max-requests recycles.
"""
import os
import glob

_mp = os.environ.get("PROMETHEUS_MULTIPROC_DIR")


def on_starting(server):  # master, before workers fork
    if _mp:
        os.makedirs(_mp, exist_ok=True)
        for f in glob.glob(os.path.join(_mp, "*.db")):
            os.unlink(f)


def child_exit(server, worker):  # reap dead worker's mmap files
    if _mp:
        from prometheus_client import multiprocess
        multiprocess.mark_process_dead(worker.pid)
