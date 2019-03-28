"""
helper code
"""
from filelock import FileLock
import logging
import os


def ACIDlog(msg, fname, mode="a", timeout=60, lock_log_lvl=logging.WARNING):
    """writes msg to the fname file with locking

    Parameters
    ----------
    msg : str
        message to write to log line
    fname : str
        location where we'll write the log
    mode : str
        model for writing
    timeout : scalar
        how long to wait for lock
    lock_log_lvl : logging lvl
        level for filelock logging, by default filelock sets this to DEBUG
        which throws out a massive number of logs which we generally don't
        want

    Notes
    -----
    The reason this exists is that logging with the Python std module
    and dask is painful/foobared.  This just works
    """

    # supress aggresive filelock logs
    logger = logging.getLogger("filelock")
    logger.setLevel(lock_log_lvl)

    # prep log entry
    log_entry = "{0}\n".format(msg)

    # grab lock and write entry
    lock = FileLock(fname + ".lock")
    with lock.acquire(timeout=timeout):
        with open(fname, mode) as handle:
            handle.write(log_entry)
    lock.release()
