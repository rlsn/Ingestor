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
    @classmethod
    def get_metadata(cls, verbose=False):
        from .hf_parquet import HuggingFaceParquetDataset
        url = "https://huggingface.co/datasets/wikimedia/wikipedia"
        meta = HuggingFaceParquetDataset.get_metadata(cls.namespace, url, verbose)
        meta["description"] = "Wikipedia dataset containing cleaned articles of all languages."
        meta["modality"]="text"
        return meta
        
    @classmethod
    def download(cls, datapath):
        from .hf_parquet import HuggingFaceParquetDataset
        return HuggingFaceParquetDataset.download(datapath)

    @classmethod
    def check_update_to_date(cls, name):
        from .hf_parquet import HuggingFaceParquetDataset
        return HuggingFaceParquetDataset.check_update_to_date(name)

    @classmethod
    def process_samples(cls, samples:pd.DataFrame)->AttrDict:
        data = AttrDict([(col,[]) for col in samples.columns])
        for row in samples.itertuples():
            for col in samples.columns:
                data[col].append(getattr(row, col))
        return data
