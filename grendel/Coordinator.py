"""
There is a growing ecosystem of tools out there to handle long-running,
distributed, "big-data" jobs.  The Coordinator class defined here tries
to pull some of these various tools into one object which can be dropped
into Python code to handle all the busy work.
"""
from dask_jobqueue import SLURMCluster, LSFCluster
from dask.distributed import Client, LocalCluster
from labbot.utilities import ACIDlog
from datetime import datetime as dt
from functools import wraps
from joblib import Memory
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
    log_file : str
        default locaton to write log
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

    # TODO handle args
    def __init__(self, cluster=None, cluster_type="local",
                 cluster_kwds={}, n_workers=1, cache_dir=".cache",
                 clear=False, log_file="coordinator.log", **kwds):

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

        # init memory
        self.memory = Memory(cache_dir, verbose=False)
        if clear:
            self.memory.clear()

        # init Client
        super().__init__(cluster, **kwds)


    def map(self, func, *iterables, cache=False, log=False, func_logger=None,
            serial=False, gather=False, pure=False, testing=0, enum=False,
            invert=False, **kwds):
        """map method with additional busy work handled

        Parameters
        ----------
        func : function
            method to run over map
        iterables : tuple
            Iterables, Iterators or Queues
        cache : bool
            whether to cache the function call
        log : bool
            whether to log the function call
        func_logger : function or None
            if provided, we assume this is a user specified function
            which takes the same args/kwds as func but returns
            an additional message (based on these args/kwds) which
            can be added to the log message
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
            func = self.memory.cache(func)
        if log:
            func = self.log(func, self.t0, self.log_file, func_logger)

        # select mapfn
        mapfn = self.selectmap(serial, gather, pure)

        # prep iterable
        iterables = self.geniter(iterables, testing, enum, invert)

        # run map operation
        return mapfn(func, *iterables, **kwds)


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
