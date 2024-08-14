"""
An API for ingesting wikimedia/wikipedia dataset at https://huggingface.co/datasets/wikimedia/wikipedia
rlsn 2024
"""

import pandas as pd
from ..dataset_wrapper import BaseDataset, Dataset
from ..utils import AttrDict

@Dataset.register('wikimedia/wikipedia')
class WikipediaDataset(BaseDataset):
    namespace = "wikimedia/wikipedia"
    abstract = False
    @staticmethod
    def get_metadata(verbose=False):
        from .hf_parquet import HuggingFaceParquetDataset
        meta = HuggingFaceParquetDataset.get_metadata(WikipediaDataset.namespace, verbose)
        meta["description"] = "Wikipedia dataset containing cleaned articles of all languages."
        meta["modality"]="text"
        return meta
        
    @staticmethod
    def download(datapath):
        from .hf_parquet import HuggingFaceParquetDataset
        return HuggingFaceParquetDataset.download(datapath)

    @staticmethod
    def check_update_to_date(name):
        from .hf_parquet import HuggingFaceParquetDataset
        return HuggingFaceParquetDataset.check_update_to_date(name)

    @staticmethod
    def process_samples(samples:pd.DataFrame)->AttrDict:
        data = AttrDict([(col,[]) for col in samples.columns])
        for row in samples.itertuples():
            for col in samples.columns:
                data[col].append(getattr(row, col))
        return data
