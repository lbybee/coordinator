from .Coordinator import Coordinator
import pandas as pd
import json
import csv
import os


class DataCollection(Coordinator):
    """handles collections of partitioned data-frames with shared indices/axes

    Parameters
    ----------

    Attributes
    ----------
    data_labels : list
        list of data set labels
    axis_labels : list
        list of labels for axes
    partitions : dict
        dictionary mapping each axis to the partition labels
    data_axes : dict
        dictionary mapping data_labels to corresponding axis_labels
    futures : dict
        dictionary of dask futures (one for each data-partition)
    """

    def __init__(self, data_labels=[], axis_labels=[], partitions={},
                 data_axes={}, futures={}, index_delimiter="_",
                 index_label="id", **kwds):

        # init attributes
        self.data_labels = data_lables
        self.axis_labels = axis_labels
        self.partitions = partitions
        self.data_axes = data_axes

        # init Coordinator
        Coordinator.__init__(self, **kwds)

        # init futures if still files
        ip = index_delimiter + index_label
        for f_l in futures:
            f = futures[f_l]
            if os.path.isfile(f):
                with open(f, "r") as fd:
                    reader = csv.reader(fd)
                    header = next(reader)
                    index_col = [c for c in header if ip in c]

                readf = lambda x: pd.read_csv(x, index_col=index_col)

                self.futures[f_l] = self.submit(readf, f)


    def map(self, func, *args, **kwds):

        return super().map(func, *args, **kwds)


    def repatition(self, part):

        return None


def read_csv(in_dir=".", index_f=None):
    """reads a directory containing csvs for a data collection

    Returns
    -------
    populated DataCollection

    Notes
    -----
    We assume that the input directory contains an index.json
    file which contains all the necessary metadata to handle the
    csvs, including all necessary metadata to init a DataCollection
    """

    in_dir = os.abspath(os.expanduser(in_dir))

    # load metadata
    if index_f is None:
        index_f = os.path.join(in_dir, "index.json")
    if not os.path.exists(index_f):
        raise ValueError("read_csv requires an index json file for \
                          DataCollection metadata")
    with open(index_f, "r") as fd:
        metadata = json.load(index_f)

    # prep futures
    files = [f for f in os.listdir(in_dir)
             if os.path.splitext(f)[1] == ".csv"]

    data_axes = {}
    for f in files:
        f_split = f.replace(".csv", "").split("_")
        lab = f_split[0]
        part_l = [os.path.join(in_dir, f)] + f_split[1:]
        if lab not in data_axes:
            data_axes[lab] = []
        data_axes[lab].append(part_l)
    data_axes = {lab: pd.DataFrame(data_axes[lab]) for lab in data_axes}

    # check that each
    labels = list(set([f.split("_")[0] for f in files]))

    for dl in metadata["data_labels"]:
        indices = metadata["data_axes"][dl]
        d_files = [f for f in files if f.split("_")[0] == dl]

        def readf(f):

            f = os.path.join(in_dir, f)
            df = pd.read_csv(f, index_col=indices)
            return df


    # init DataCollection
    metadata["futures"] = futures
    dc = DataCollection(**metadata)
    return dc



    self.data_labels = data_labels
    self.part_labels = part_labels
        self.data_parts = data_parts




    def write_csv(self, out_dir="."):
        """writes a csv for every future"""

        out_dir = os.abspath(os.expanduser(out_dir))
        wcsv = lambda key, val: val.to_csv("%s/%s.csv" % (out_dir, key))
        self.map(wcsv, self.futures.items(), out_dir=out_dir, gather=True)
