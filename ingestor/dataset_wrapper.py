"""
This script serves as a registry for all dataset classes
rlsn 2024
"""
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
    
from .dataset_classes import *