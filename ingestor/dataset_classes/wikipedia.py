"""
An API for ingesting wikimedia/wikipedia dataset at https://huggingface.co/datasets/wikimedia/wikipedia
rlsn 2024
"""

import os
import pandas as pd
from huggingface_hub import hf_hub_download, list_repo_tree
from huggingface_hub.hf_api import RepoFolder
from ..dataset_wrapper import Dataset
from ..__init__ import DATA_DIR, CACHE_DIR, AUTO_CLEAR_CACHE


@Dataset.register('wikimedia/wikipedia')
class WikipediaDataset(object):
    namespace = "wikimedia/wikipedia"
    modality = "text"
    @staticmethod
    def get_metadata(verbose=False):
        meta = {
            "path":WikipediaDataset.namespace,
            "modality": WikipediaDataset.modality,
            "subsets":{}
        }
        for subs in list_repo_tree(WikipediaDataset.namespace, repo_type="dataset"):
            if type(subs)!=RepoFolder:
                continue
            meta["subsets"][subs.path] = {
                "path":os.path.join(WikipediaDataset.namespace, subs.path),
                "downloaded":0,
                "partitions":{}
            }
            if verbose:
                print(f"retrieving info from {subs.path}")
            for part in list_repo_tree(WikipediaDataset.namespace,path_in_repo=subs.path, repo_type="dataset", expand=True):
                path = os.path.join(WikipediaDataset.namespace, part.path)
                downloaded = os.path.exists(path)
                meta["subsets"][subs.path]["partitions"][os.path.basename(part.path)]={
                    "path":path,
                    "size":part.size,
                    "downloaded":downloaded
                }
                meta["subsets"][subs.path]["downloaded"] += 1 if downloaded else 0
        return meta
        
    @staticmethod
    def download(subset, partition):
        repo_id = "wikimedia/wikipedia"
        filepath=hf_hub_download(repo_id=repo_id,
                         filename=f"{subset}/{partition}",
                         force_download = True,
                         local_dir=os.path.join(DATA_DIR, WikipediaDataset.namespace),
                         cache_dir=CACHE_DIR,repo_type="dataset")

        if AUTO_CLEAR_CACHE:
            from ..core_api import clear_cache
            clear_cache()
        return filepath
        
    @staticmethod
    def load_partition(path):
        data = pd.read_parquet(path, engine='pyarrow')
        return data