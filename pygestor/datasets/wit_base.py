"""
An API for ingesting wikimedia/wit_base dataset at https://huggingface.co/datasets/wikimedia/wit_base
rlsn 2024
"""
import pandas as pd
from PIL import Image
from PIL.JpegImagePlugin import JpegImageFile
import io
from ..dataset_wrapper import BaseDataset, Dataset
from ..utils import AttrDict

@Dataset.register('wikimedia/wit_base')
class WitbaseDataset(BaseDataset):
    namespace = "wikimedia/wit_base"
    abstract = False
    @classmethod
    def get_metadata(cls, verbose=False):
        from .hf_parquet import HuggingFaceParquetDataset
        url = "https://huggingface.co/datasets/wikimedia/wit_base"
        meta = HuggingFaceParquetDataset.get_metadata(cls.namespace, url, verbose)
        meta["description"] = "Wikimedia's version of the Wikipedia-based Image Text (WIT) Dataset, a large multimodal multilingual dataset."
        meta["modality"]="text,image"
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
                if col=="image":
                    img = Image.open(io.BytesIO(getattr(row, col)['bytes']))
                    if type(img)!=JpegImageFile:
                        with io.BytesIO() as b:
                            img.convert('RGB').save(b, format="jpeg")
                            img = Image.open(b)
                            img.load()
                    data[col].append(img)
                else:
                    data[col].append(getattr(row, col))
        return data