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
        if dataset_name in cls._dataset_classes:
            return cls._dataset_classes[dataset_name]
        else:
            return None

    @classmethod
    def register(cls, dataset_name:str):
        def inner_wrapper(wrapped_class):
            cls._dataset_classes[dataset_name] = wrapped_class
            return wrapped_class
        return inner_wrapper
    
    @classmethod
    def get_abstract_names(cls)->list:
        ret = []
        for cls_name in cls._dataset_classes:
            if cls._dataset_classes[cls_name].abstract:
                ret.append(cls_name)
        return ret
    
class BaseDataset(object):
    @classmethod
    def get_metadata(cls, *args, **kargs):
        pass
    @classmethod
    def download(cls, datapath):
        pass
    @classmethod
    def check_update_to_date(cls, name):
        pass
    @classmethod
    def process_samples(cls, samples:pd.DataFrame)->AttrDict:
        pass

from .datasets import *