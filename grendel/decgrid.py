"""
decorator to run a function-label pair on one of the research clusters
to which I have access.  Currently this supports:

    1. Grace

    http://docs.ycrc.yale.edu/clusters-at-yale/clusters/grace/

    2. SOM Research Cluster

    https://confluence.som.yale.edu/display/SC/Onboarding+for+Research+Computing
"""
from decorator import decorator
from fabric import Connection
from datetime import datetime
from functools import wraps
from invoke import Context
from socket import timeout
import inspect
import shutil
import time
import json
import os


def decgrid(func, label, host=None, cluster_kwds={}, connection_kwds={},
            path_kwds={}, run_kwds={}, **job_kwds):
    """decorates func with code necessary to run on a research grid

    Parameters
    ----------
    func : function
        function to decorate
    label : str
        label for func/task
    host : str or None
        host to connect to for cluster
    cluster_kwds : dict
        key-words to pass to prep_cluster
    connection_kwds : dict
        key-words passed to fabric connection
    path_kwds : dict
        key-words for initializing paths (passed to prep_path)
    run_kwds : dict
        key-words passed to job run method
    job_kwds : dict
        additional key words for the job passed to queue manager
    """

    def nfunc(func, *args, **kwds):

        # collapse args into kwds
        kwds = collapse_kwds(func, *args, **kwds)

        # prep cluster specific params
        cluster_dict = prep_cluster(**cluster_kwds)

        # establish connection
        conn = Connection(host, **connection_kwds)

        # prep local and remote paths
        path_dict, kwds = prep_path(conn, label, kwds, **path_kwds)

        # prep job script
        job_file = prep_job(conn, func, kwds, path_dict, cluster_dict,
                            **job_kwds)

        # sync input
        rsync(conn, path_dict["loc_in"], path_dict["rem_in"], True)

        # run actual job
        run_job(conn, job_file, path_dict, cluster_dict, **run_kwds)

        # TODO since we're now syncing the output after each wait,
        # we don't really need this here any more...
        # sync output
        rsync(conn, path_dict["rem_out"], path_dict["loc_out"], False)

    return decorator(nfunc)(func)


def collapse_kwds(func, *args, **kwds):
    """collapse the args into kwds st

    Parameters
    ----------
    func : function
        function we ultimately want to call
    args : tuple
        non key-word inputs
    kwds : dict
        key-word inputs

    Returns
    -------
    kwds : dict
        updated to reflect args
    """

    func_spec = inspect.getfullargspec(func)
    defaults = func_spec.defaults
    func_args = func_spec.args
    if defaults:
        func_args = func_args[:-len(defaults)]
    t_kwds = {**{a: av for a, av in zip(func_args, args)}, **kwds}

    return t_kwds



def prep_cluster(cluster_label="grace", add_modules=[]):
    """creates a dict containing cluster dependent params

    Parameters
    ----------
    cluster_label : str
        label mapping to default values for cluster_dict
    add_modules : list
        additional modules to load in addition to default python

    Returns
    -------
    cluster_dict : dict
        contains cluster specific params
    """

    cluster_dict = {}

    if cluster_label == "grace":
        cluster_dict["fail_states"] = ["BOOT_FAIL",
                                       "CANCELLED",
                                       "DEADLINE",
                                       "FAILED",
                                       "NODE_FAIL",
                                       "OUT_OF_MEMORY",
                                       "PREEMPTED",
                                       "REVOKED",
                                       "TIMEOUT"]
        cluster_dict["success_state"] = "COMPLETED"
        cluster_dict["queue_mngr"] = "SBATCH"
#        cluster_dict["queue_mngr"] = "SLURM"
        cluster_dict["py_cmd"] = "python3"
        cluster_dict["launch_fn"] = _grace_launch
        cluster_dict["state_fn"] = _grace_state
        cluster_dict["modules"] = ["Langs/Python/3.6.4"] + add_modules
        cluster_dict["pip"] = "pip3"

    elif cluster_label == "som":
        cluster_dict["fail_states"] = ["EXIT",
                                       "ZOMBI",
                                       "PSUSP",
                                       "USUSP",
                                       "SSUSP",
                                       "UNKWN"]
        cluster_dict["success_state"] = "DONE"
        cluster_dict["queue_mngr"] = "LSF"
        cluster_dict["py_cmd"] = "python3.6"
        cluster_dict["launch_fn"] = _som_launch
        cluster_dict["state_fn"] = _som_state
        cluster_dict["modules"] = ["opt-python"] + add_modules
        cluster_dict["pip"] = "pip3"

    else:
        raise ValueError("Unsupported cluster: %s" % cluster_label)

    return cluster_dict


def prep_path(conn, label, kwds, remote_dir="scratch60", in_key="in_dir",
              out_key="out_dir"):
    """prepares the local and remote directory paths

    Parameters
    ----------
    conn : fabric Connection instance
        connection to remote server
    label : str
        label for current task/func
    kwds : dict
        key-words for func
    remote_dir : str
        location on remote server where we can place files
    in_key : str
        label for key-word corresponding to the input directory
    out_key : str
        label for key-word corresponding to output directory

    Returns
    -------
    tuple
        containing

        1. path_dict : dict
            contains each pair of local/remote directory paths
        2. kwds : dict
            update key-words for func which replace local dirs with
            remote dirs
    """

    path_dict = {}

    # prep remote dir paths
    remote_dir = os.path.join(remote_dir, label)
    path_dict["rem_code"] = os.path.join(remote_dir, "code")
    path_dict["rem_in"] = os.path.join(remote_dir, "input")
    path_dict["rem_out"] = os.path.join(remote_dir, "output")

    # prep local dir paths
    path_dict["loc_code"] = os.getcwd()
    path_dict["loc_in"] = kwds[in_key]
    path_dict["loc_out"] = kwds[out_key]

    # create necessary local/remote dirs
    conn.run("mkdir -p %s" % path_dict["rem_code"])
    conn.run("mkdir -p %s" % path_dict["rem_in"])
    conn.run("mkdir -p %s" % path_dict["rem_out"])
    conn.local("mkdir -p %s" % path_dict["loc_out"])

    # update to reflect real location
    rem = conn.run("readlink -f %s" % path_dict["rem_code"]).stdout
    path_dict["rem_code"] = rem.split("\n")[0]
    rem = conn.run("readlink -f %s" % path_dict["rem_in"]).stdout
    path_dict["rem_in"] = rem.split("\n")[0]
    rem = conn.run("readlink -f %s" % path_dict["rem_out"]).stdout
    path_dict["rem_out"] = rem.split("\n")[0]

    # replace corresponding elements in kwds
    kwds[in_key] = path_dict["rem_in"]
    kwds[out_key] = path_dict["rem_out"]

    return path_dict, kwds


def prep_job(conn, func, kwds, path_dict, cluster_dict,
             packages=[], jobtype="source", **job_kwds):
    """copy and build all necessary code for job file

    Parameters
    ----------
    conn : fabric Connection instance
        connection to remote server
    func : python function
        actual function to run
    kwds : dict
        key-words for func
    path_dict : dict
        dictionary containing local/remote paths
    cluster_dict : dict
        dictionary containing cluster specific params
    packages : list
        additional packages to install for user with pip
    jobtype : str
        label for the type of run, determines how we prep
        code,
    job_kwds : dict
        key-words provided to queue mananger

    Returns
    -------
    job_f : str
        basename corresponding to job file
    """


    # prep base filenames
    dstamp = datetime.now().strftime("%Y%m%d%H%M%S")
    run_f = os.path.join(path_dict["loc_code"], "run_%s.py" % dstamp)
    json_f = os.path.join(path_dict["loc_code"], "%s.json" % dstamp)
    job_f = os.path.join(path_dict["loc_code"], "%s.job" % dstamp)

    # handle different jobtypes
    if jobtype == "source":
        mod_lab = "mod_%s" % dstamp
    elif jobtype == "module":
        mod_lab = func.__module__
    else:
        raise ValueError("Unknown jobtype: %s" % jobtype)

    # create module file containing code from func
    mod_f = os.path.join(path_dict["loc_code"], "%s.py" % mod_lab)
    mod_script = inspect.getfile(func)
    shutil.copy(mod_script, mod_f)

    # prep run script
    fn_name = func.__name__
    with open(run_f, "w") as fd:
        fd.write("from %s import %s as func\n" % (mod_lab, fn_name))
        fd.write("import json\n")
        fd.write("\n")
        fd.write("json_file = '%s.json'\n" % dstamp)
        fd.write("\n")
        fd.write("with open(json_file, 'r') as fd:\n")
        fd.write("    func_kwds = json.load(fd)\n")
        fd.write("\n")
        fd.write("func(**func_kwds)\n")

    # prep job script
    with open(job_f, "w") as fd:
        fd.write("#!/bin/bash\n")
        for k in job_kwds:
            fd.write("#%s %s=%s\n" % (cluster_dict["queue_mngr"], k,
                                      str(job_kwds[k])))
        fd.write("\n")
        for mod in cluster_dict["modules"]:
            fd.write("module load %s\n" % mod)
        fd.write("\n")
        fd.write("%s run_%s.py > run.log\n" % (cluster_dict["py_cmd"], dstamp))

    # prep kwds/json
    with open(json_f, "w") as fd:
        json.dump(kwds, fd)

    # sync local files to remote
    rsync(conn, path_dict["loc_code"], path_dict["rem_code"], True)

    # install necessary packages
    if len(packages) > 0:
        mod_str = "\n".join(["module load %s" % mod for mod in
                             cluster_dict["modules"]])
        pkg_str = "%s install %s --user --upgrade" % (cluster_dict["pip"],
                                                      " ".join(packages))
        cmd = "{mod_str}\n{pkg_str}"
        cmd = cmd.format(mod_str=mod_str, pkg_str=pkg_str)
        conn.run(cmd)

    # run setup if necessary (for local cython code)
    if os.path.isfile(os.path.join(path_dict["loc_code"], "setup.py")):
        run_str = "{py_cmd} setup.py build_ext --inplace --force"
        run_str = run_str.format(**cluster_dict)
        mod_str = "\n".join(["module load %s" % mod for mod in
                             cluster_dict["modules"]])
        cmd = "cd {rem_code}\n{mod_str}\n{run_str}"
        cmd = cmd.format(rem_code=path_dict["rem_code"],
                         mod_str=mod_str,
                         run_str=run_str)
        conn.run(cmd)

    # remove temporary local files
    os.remove(mod_f)
    os.remove(run_f)
    os.remove(json_f)
    os.remove(job_f)

    # return jobfile
    return os.path.basename(job_f)


def run_job(conn, job_f, path_dict, cluster_dict, tsleep=360, run_timeout=10):
    """runs the grid job and tracks its status

    Parameters
    ----------
    conn : fabric Connection instance
        current connection to remote server
    job_f : str
        basename of job file
    path_dict : dict
        contains local/remote paths, we need it here to launch the job
        in the corresponding working directory
    cluster_dict : dict
        cluster specific params
    tsleep : scalar
        how long to wait between each status check
    run_timeout : scalar
        how long to wait for env_aware_run
    """

    # run func and get resulting job-id
    jid = cluster_dict["launch_fn"](conn, path_dict["rem_code"], job_f,
                                    run_timeout)

    # now we monitor the state until the task finishes
    run_status = True
    while run_status:
        time.sleep(tsleep)

        state = cluster_dict["state_fn"](conn, jid, run_timeout)
        # TODO remove print
        print(state)
        if state == cluster_dict["success_state"]:
            run_status = False
        elif state in cluster_dict["fail_states"]:
            raise RuntimeError("%s %s job %d failed with state %s" %
                               (job_f, cluster_dict["queue_mngr"],
                                jid, state))

        # sync output if there is any
        if conn.run("ls %s" % path_dict["rem_out"]).stdout != "":
            rsync(conn, path_dict["rem_out"], path_dict["loc_out"], False)


def rsync(conn, source_dir, dest_dir, ltr):
    """syncs the contents of source_dir to dest_dir

    Parameters
    ----------
    conn : fabric Connection instance
        current connection to remote server
    source_dir : str
        location of source files
    dest_dir : str
        location where source files will be rsynced
    ltr : bool
        whether to sync local->remote or remote->local
    """

    port = conn.port
    user = conn.user
    host = conn.host

    source_dir = os.path.join(source_dir, "*")

    if ltr:
        conn.local("rsync -ave 'ssh -p %d' %s %s@%s:%s" %
                   (port, source_dir, user, host, dest_dir))
    else:
        conn.local("rsync -ave 'ssh -p %d' %s@%s:%s %s" %
                   (port, user, host, source_dir, dest_dir))


##############################################################################
#                          Cluster Specific Fns                              #
##############################################################################

def _grace_launch(conn, code_dir, job_f, run_timeout):

    res = conn.run("cd %s\nsbatch %s" % (code_dir, job_f))
    jid = int(res.stdout.split(" ")[-1].replace("\n", ""))
    return jid


def _grace_state(conn, jid, run_timeout):

    state = conn.run("sacct -j %d -n -P -o state" % jid).stdout
    state = state.split("\n")[0]
    return state


def _som_launch(conn, code_dir, job_f, run_timeout):

    res = env_aware_run(conn, "cd %s\nbsub bash %s" % (code_dir, job_f),
                        run_timeout=run_timeout)
    jid = res.split("\n")[-2].split(" ")[1]
    jid = int(jid.replace("<", "").replace(">", ""))
    return jid


def _som_state(conn, jid, run_timeout):

    state = env_aware_run('bjobs -o "stat:" %d' % jid,
                          run_timeout=run_timeout)
    state = state.split("\n")[-2].split(" ")[0]
    return state


def env_aware_run(conn, cmd, run_timeout=10):
    """runs the specified command st it aligns with the comparable ssh run

    This is necessary for the SOM cluster because they have some specific
    start up scripts which don't place nice with fabric Connections

    Parameters
    ----------
    conn : fabric Connection instance
        the actual connection to SOM cluster
    cmd : str
        the command which you'd like to run in an enviromentally aware way
    run_timeout : scalar or None
        we need to set a run_timeout otherwise this will run forever

        Note that there should be a better way to handle this

    Returns
    -------
    stdout_str : str
        string corresponding to stdout

    Notes
    -----
    The issue here is that SOM has some special code that is run when you
    connect to their server.  This code is not invoked when a regular
    connection is established.  Worse still, it doesn't appear that the
    natural next attempt:

        conn.client.exec_command(<cmd>, get_pty=True)

    works.  Note here that conn.client is a Paramiko Client instance.

    Instead, I've implemented this function which handles everything
    correctly, by which I mean it gives results comparable to what we'd get
    if we just sshed directly.

    There is likely some "nice" abstraction that is lost as a result
    """

    channel = conn.client.invoke_shell()
    channel.send(cmd + "\n")
    channel.set_timeout(run_timeout)
    stdout = chan.makefile("r", -1)
    stdout_str = ""
    while True:
        try:
            line = stdout.readline()
            stdout_str += line
        except run_timeout:
            break

    return stdout_str
