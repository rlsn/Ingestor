"""
This script contains utils functions for the backend module
rlsn 2024
"""
import os
import pandas as pd
import pyarrow.parquet as pq
from pyarrow.dataset import dataset
from pyarrow.parquet import ParquetDataset

def all_partitions(*metadata_path):
    from .core_api import get_meta
    info = get_meta(*metadata_path)
    def part_loop():
        for part in info["partitions"].values():
            yield part
    def subs_loop():
        for subs in info["subsets"].values():
            for part in subs["partitions"].values():
                yield part
    if "subsets" in info:
        yield from subs_loop()
    else:
        yield from part_loop()

def divide_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 

def normpath(path):
    return os.path.normpath(path).replace('\\','/')

def joinpath(*path):
    return normpath(os.path.join(*path))

def compute_subset_download(subs):
    return sum([1 if part['downloaded'] else 0 for part in subs["partitions"].values()])

def compute_subset_n_samples(subs):
    return sum([part['n_samples'] for part in subs["partitions"].values()])

def compute_subset_size(subs):
    return sum([part['size'] for part in subs["partitions"].values()])

def compute_nsamples(parquet):
    dataset = ParquetDataset(parquet)
    n_samples = sum(p.count_rows() for p in dataset.fragments)
    return n_samples

def read_schema(parquet):
    return pq.read_schema(parquet)

def load_parquets_in_batch(parquets, batchsize=10):    
    def generator():
        ds = dataset(parquets, format="parquet")
        for batch in ds.to_batches(batch_size=batchsize):
            yield batch.to_pandas()
    yield from generator()

def load_parquets(parquets):
    dataframes = [pd.read_parquet(f, engine='pyarrow') for f in parquets]
    data = pd.concat(dataframes, axis=0)
    return data

import json
class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

    def to_json(self, filename):
        with open(filename, "w") as wf:
            wf.write(json.dumps(self, indent=4))

    @classmethod
    def from_json(cls, filename):
        with open(filename, "r") as f:
            obj = cls(json.loads(f.read()))
        return obj
    
class Mutable(object):
    def __init__(self, v=None):
        self.v = v
    def set(self, v):
        self.v = v
    def get(self):
        return self.v