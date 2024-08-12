"""
An API for ingesting wikimedia/wit_base dataset at https://huggingface.co/datasets/wikimedia/wit_base
rlsn 2024
"""
import os
import pandas as pd
from huggingface_hub import hf_hub_download, list_repo_tree
from ..dataset_wrapper import Dataset, dataset_struct, subset_struct, partition_struct
from ..__init__ import DATA_DIR, CACHE_DIR, AUTO_CLEAR_CACHE
from ..utils import compute_nsamples, AttrDict
from PIL import Image
from PIL.JpegImagePlugin import JpegImageFile
import io

@Dataset.register('wikimedia/wit_base')
class WitbaseDataset(object):
    namespace = "wikimedia/wit_base"
    @staticmethod
    def get_metadata(verbose=False):
        meta = dataset_struct(
            path=WitbaseDataset.namespace,
            modality=["text","image"],
            source="https://huggingface.co/datasets/wikimedia/wit_base",
            description="Wikimedia's version of the Wikipedia-based Image Text (WIT) Dataset, a large multimodal multilingual dataset.",
            subsets={}
        )
        meta["subsets"]["data"] = subset_struct(
            path=os.path.join(WitbaseDataset.namespace, "data"),
            downloaded=0,
            formats=["parquet"],
            partitions={}
        )
        if verbose:
            print(f"retrieving info from data")
        for part in list_repo_tree(WitbaseDataset.namespace,path_in_repo='data', repo_type="dataset", expand=True):
            path = os.path.join(WitbaseDataset.namespace, "data", os.path.basename(part.path))
            download_path = os.path.join(DATA_DIR,path)
            downloaded = os.path.exists(download_path)
            meta["subsets"]["data"]["partitions"][os.path.basename(part.path)]=partition_struct(
                path=path,
                size=part.size,
                downloaded=downloaded,
                n_samples=compute_nsamples(download_path) if downloaded else 0

            )
            meta["subsets"]["data"]["downloaded"] += 1 if downloaded else 0
        return meta
        
    @staticmethod
    def download(subset, partition):
        repo_id = "wikimedia/wit_base"
        filepath=hf_hub_download(repo_id=repo_id,
                         filename=f"data/{partition}",
                         force_download = True,
                         local_dir=os.path.join(DATA_DIR, WitbaseDataset.namespace),
                         cache_dir=CACHE_DIR,repo_type="dataset")
        
        if AUTO_CLEAR_CACHE:
            from ..core_api import clear_cache
            clear_cache()
        return filepath
    
    @staticmethod
    def process_samples(samples:pd.DataFrame)->AttrDict:
        data = AttrDict([(col,[]) for col in samples.columns])
        for row in samples.itertuples():
            for col in samples.columns:
                if col=="image":
                    img = Image.open(io.BytesIO(getattr(row, col)['bytes']))
                    if type(img)!=JpegImageFile:
                        with io.BytesIO() as b:
                            img.save(b, format="jpeg")
                            img = Image.open(b)
                            img.load()
                    data[col].append(img)
                else:
                    data[col].append(getattr(row, col))
        return data