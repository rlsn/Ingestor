"""
This script contains utils functions for the backend module
rlsn 2024
"""
import pyarrow.parquet as pq
from pyarrow.dataset import dataset
from pyarrow.parquet import ParquetDataset
import pandas as pd

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

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self