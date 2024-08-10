"""
An API for ingesting wikimedia/wit_base dataset at https://huggingface.co/datasets/wikimedia/wit_base
rlsn 2024
"""
import os
import pandas as pd
from huggingface_hub import hf_hub_download, list_repo_tree
from huggingface_hub.hf_api import RepoFolder
from ..dataset_wrapper import Dataset
from ..__init__ import DATA_DIR, CACHE_DIR, AUTO_CLEAR_CACHE

@Dataset.register('wikimedia/wit_base')
class WitbaseDataset(object):
    namespace = "wikimedia/wit_base"
    @staticmethod
    def get_metadata(verbose=False):
        meta = {
            "path":os.path.join(DATA_DIR, WitbaseDataset.namespace),
            "subsets":{}
        }
        meta["subsets"]["data"] = {
            "path":os.path.join(DATA_DIR, WitbaseDataset.namespace, "data"),
            "downloaded":0,
            "partitions":{}
        }
        if verbose:
            print(f"retrieving info from data")
        for part in list_repo_tree(WitbaseDataset.namespace,path_in_repo='data', repo_type="dataset", expand=True):
            path = os.path.join(DATA_DIR, WitbaseDataset.namespace, "data", os.path.basename(part.path))
            downloaded = os.path.exists(path)
            meta["subsets"]["data"]["partitions"][os.path.basename(part.path)]={
                "path":path,
                "size":part.size,
                "downloaded":downloaded
            }
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
    def load_partition(path):
        data = pd.read_parquet(path, engine='pyarrow')
        return data