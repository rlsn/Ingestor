"""
This script serves as a registry for all dataset classes
rlsn 2024
"""
import pandas as pd
from .utils import AttrDict

def dataset_struct(path="",source="",description="",
                   modality="",formats="",subsets=None,
                   dataset_class="",**kargs):
    return dict(
        path=path,
        source=source,
        description=description,
        modality=modality,
        subsets=subsets if subsets is not None else dict(),
        formats=formats,
        dataset_class=dataset_class,
        **kargs
        )

def subset_struct(path="",partitions=None,description="",**kargs):
    return dict(
        path=path,
        description=description,
        partitions=partitions if partitions is not None else dict(),
        **kargs
        )

def partition_struct(path="",size=0,n_samples=0,downloaded=False,
                     acquisition_time=None,**kargs):
    return dict(
        path=path,
        size=size,
        n_samples=n_samples,
        downloaded=downloaded,
        acquisition_time=acquisition_time,
        **kargs
        )

class Dataset(object):
    _dataset_classes = {}

    @classmethod
    def get(cls, dataset_name:str):
        try:
            return cls._dataset_classes[dataset_name]()
        except KeyError:
            raise ValueError(f"unknown dataset name : {dataset_name}")

    @classmethod
    def register(cls, dataset_name:str):
        def inner_wrapper(wrapped_class):
            cls._dataset_classes[dataset_name] = wrapped_class
            return wrapped_class
        return inner_wrapper
    
class BaseDataset(object):
    @staticmethod
    def get_metadata(*args, **kargs):
        pass
    @staticmethod
    def download(datapath):
        pass
    @staticmethod
    def check_update_to_date(name):
        pass
    @staticmethod
    def process_samples(samples:pd.DataFrame)->AttrDict:
        pass

from .datasets import *