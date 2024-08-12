"""
This script serves as a registry for all dataset classes
rlsn 2024
"""

def dataset_struct(path="",source="",description="",modality=[],subsets=dict(),**kargs):
    return dict(
        path=path,
        source=source,
        description=description,
        modality=modality,
        subsets=subsets,
        **kargs
        )

def subset_struct(path="",partitions=dict(),description="",formats=[],**kargs):
    return dict(
        path=path,
        description=description,
        partitions=partitions,
        formats=formats,
        **kargs
        )

def partition_struct(path="",size=0,n_samples=0,downloaded=False,**kargs):
    return dict(
        path=path,
        size=size,
        n_samples=n_samples,
        downloaded=downloaded,
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
    
from .datasets import *