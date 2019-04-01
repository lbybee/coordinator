"""
There is a growing ecosystem of tools out there to handle long-running,
distributed, "big-data" jobs.  The Coordinator class defined here tries
to pull some of these various tools into one object which can be dropped
into Python code to handle all the busy work.
"""
from dask_jobqueue import SLURMCluster, LSFCluster
from dask.distributed import Client, LocalCluster
from .utilities import ACIDlog, send_email
from datetime import datetime as dt
from functools import wraps
import logging
import inspect
import joblib
import toolz
import os


class Coordinator(Client):
    """handles the busy-work associated with computing over big-data

    Parameters
    ----------
    cluster : dask Cluster instance or None
        if provided, we use this as the cluster backend for the client
    cluster_type : str
        label for cluster type to use
    cluster_kwds : dict
        key words to initiate cluster
    n_workers : scalar
        number of workers for cluster scale
    cache_dir : str
        location to write cache
    clear : bool
        whether to clear the cache before running
    verbose : bool
        whether to get status updates from joblib Memory
    log_file : str
        default locaton to write log
    email_config_f : str
        config location for email log
    kwds : dict
        additional key-words to initialize Client.

    Attributes
    ----------
    memory : joblib Memory instance
        memory used for caching
    t0 : datetime
        launch time for Coordinator
    log_file : str
        location to write the log
    """

   def __init__(self, cluster=None, cluster_type="local",
                cluster_kwds={"silence_logs": logging.ERROR}, n_workers=1,
                cache_dir=".cache", log_file="coordinator.log",
                email_config_f="~/passepartout/files/config/emaildec.yaml",
                **kwds):

        # init Cluster
        if cluster_type.lower() == "local":
            Cluster = LocalCluster
            cluster_kwds["n_workers"] = n_workers
        elif cluster_type.lower() == "slurm":
            Cluster = SLURMCluster
        elif cluster_type.lower() == "lsf":
            Cluster = LSFCluster
        else:
            raise ValueError("Currently unsupported cluster_type: %s" %
                             cluster_type)

        if not cluster:
            cluster = Cluster(**cluster_kwds)
        if cluster_type.lower() != "local":
            cluster.scale(n_workers)

        # init logger inputs
        self.t0 = dt.now()
        self.log_file = log_file

        # init email
        self.email_config_f = email_config_f

        # init cache
        self.cache_dir = cache_dir

        # init Client
        super().__init__(cluster, **kwds)


    def map(self, func, *iterables, cache=False, overwrite=False, log=False,
            func_logger=None, email=False, serial=False, gather=False,
            pure=False, testing=0, enum=False, invert=False, **kwds):
        """map method with additional busy work handled

        Parameters
        ----------
        func : function
            method to run over map
        iterables : tuple
            Iterables, Iterators or Queues
        cache : bool
            whether to cache the function call
        overwrite : bool
            whether to overwrite the cache
        log : bool
            whether to log the function call
        func_logger : function or None
            if provided, we assume this is a user specified function
            which takes the same args/kwds as func but returns
            an additional message (based on these args/kwds) which
            can be added to the log message
        email : bool
            whether to send emails on internal failures
        serial : bool
            whether the map operation should use the default Python map
        gather : bool
            whether a gather operation should be run on the result of map
        pure : bool
            whether the function should be treated as pure
        testing : scalar
            if > 0, we take this many elements from the start of the iterables
            as our new iterables (useful for testing map operations)
        enum : bool
            whether to prepend an index to start of the iterable
        invert : bool
            whether to invert the iterables
        kwds : dict
            additional key words to pass to map/func

        Returns
        -------
        iterable
            list of futures or computed results
        """

        # apply decorators
        if cache:
            func = self.cache(func, self.cache_dir, overwrite)
        if log:
            func = self.log(func, self.t0, self.log_file, func_logger)
        if email:
            func = self.emailerror(func, self.email_config_f)

        # select mapfn
        mapfn = self.selectmap(serial, gather, pure)

        # prep iterable
        iterables = self.geniter(iterables, testing, enum, invert)

        # run map operation
        return mapfn(func, *iterables, **kwds)


    def cache(self, func, cache_dir, overwrite):
        """memoization decorator for function

        Parameters
        ----------
        func : function
            function to decorate
        cache_dir : str
            location where the cache will be written
        overwrite : bool
            whether to rewrite the cache (even if it exists)

        Notes
        -----
        joblib's caching is too heavy duty and sometimes introduces
        unecessary complexity/errors.  This is a stripped down version that
        I understand well.
        """

        loc = os.path.join(cache_dir, "labbot", func.__module__,
                           func.__name__)
        os.makedirs(loc, exist_ok=True)

        @wraps(func)
        def nfunc(*args, **kwds):

            state = {"args": args,
                     "kwds": kwds,
                     "code": inspect.getsource(func)}

            func_hash = joblib.hash(state)
            hash_dir = os.path.join(loc, func_hash)
            state_f = os.path.join(hash_dir, "state.pkl")
            output_f = os.path.join(hash_dir, "output.pkl")

            # if the cache exists, just return that
            if os.path.exists(output_f) and not overwrite:

                with open(output_f, "rb") as fd:
                    res = joblib.load(fd)

            # otherwise we need to run the entire function
            else:

                t0 = dt.now()

                res = func(*args, **kwds)

                os.makedirs(hash_dir, exist_ok=True)
                t1 = dt.now()
                state["time"] = t1
                state["runtime"] = t1 - t0
                # note we add the function here, instead of above,
                # because the decoration screws up the hashing
                state["func"] = func

                with open(state_f, "wb") as fd:
                    joblib.dump(state, fd)
                with open(output_f, "wb") as fd:
                    joblib.dump(res, fd)

            return res

        return nfunc


    def log(self, func, glob_t0, log_file, func_logger):
        """logs the run of func

        Parameters
        ----------
        func : function
            function to decorate
        glob_t0 : datetime
            start time for coordinator
        log_file : str
            location to write the log
        func_logger : function or None
            if provided, we assume this is a user specified function
            which takes the same args/kwds as func but returns
            an additional message (based on these args/kwds) which
            can be added to the log message

        Returns
        -------
        func : function
            decorated to support distributed/parallel logging
        """

        @wraps(func)
        def nfunc(*args, **kwds):

            # prep func_logger message
            if func_logger:
                add_msg = func_logger(*args, **kwds)
            else:
                add_msg = ""

            # prep variables for log
            func_name = func.__name__
            func_t0 = dt.now()

            # run the actual function
            res = func(*args, **kwds)

            # get runtimes
            t1 = dt.now()
            glob_tdiff = str(t1 - glob_t0)
            func_tdiff = str(t1 - func_t0)

            # log message
            msg = "{0} {1}      {2} glob runtime: {3} func runtime: {4}"
            msg = msg.format(t1, func_name, add_msg, func_tdiff, glob_tdiff)
            ACIDlog(msg, log_file)

            return res

        return nfunc


    def emailerror(self, func, email_config_f):
        """sends an email with the error log

        Parameters
        ----------
        func : function
            function to decorate
        email_config_f : str
            location of email config file

        Returns
        -------
        func : function
            decorated to support email logs
        """

        @wraps(func)
        def nfunc(*args, **kwds):

            cf = os.path.expanduser(email_config_f)

            try:
                res = func(*args, **kwds)
            except Exception as e:
                send_email(func, "failed with error %s" % str(e),
                           cf, *args, **kwds)
                raise e

            return res

        return nfunc


    def selectmap(self, serial, gather, pure):
        """select the corresponding map function based on flags

        Parameters
        ----------
        serial : bool
            whether the map operation should use the default map
        gather : bool
            whether a gather operation should be run on the result of map
        pure : bool
            whether the function should be treated as pure

        Returns
        -------
        function
            prepped to handle map operation
        """

        if serial:
            mapfn = map
        else:
            mapfn = super().map

        if pure:
            pmapfn = lambda *args, **kwds: mapfn(*args, **kwds, pure=True)
        else:
            pmapfn = mapfn

        # we only apply gather if our operations are in parallel
        if gather:
            gatherfn = super().gather
            gmapfn = lambda *args, **kwds: gatherfn(pmapfn(*args, **kwds))
            return gmapfn
        else:
            return pmapfn


    def geniter(self, iterables, testing, enum, invert):
        """method to handling special map cases by updating iterables

        Parameters
        ----------
        iterables : iterable
            object which we want to modify
        testing : scalar
            if > 0, we take this many elements from the start of the iterables
            as our new iterables (useful for testing map operations)
        enum : bool
            whether to prepend an index to start of the iterable
        invert : bool
            whether to invert the iterables

        Returns
        -------
        iterable
            updated to reflect flags
        """

        if testing:
            iterables = (list(toolz.take(testing, i)) for i in iterables)

        if enum:
            iterables = (list(range(min(len(i) for i in iterables))),
                         *iterables)

        if invert:
            iterables = (invertd(i) for i in iterables)

        return iterables