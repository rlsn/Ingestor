"""
An API for ingesting wikimedia/wikipedia dataset at https://huggingface.co/datasets/wikimedia/wikipedia
rlsn 2024
"""

import os
import pandas as pd
from huggingface_hub import hf_hub_download, list_repo_tree
from huggingface_hub.hf_api import RepoFolder
from ..dataset_wrapper import Dataset, dataset_struct, subset_struct, partition_struct
from ..__init__ import DATA_DIR, CACHE_DIR, AUTO_CLEAR_CACHE
from ..utils import compute_nsamples, AttrDict

@Dataset.register('wikimedia/wikipedia')
class WikipediaDataset(object):
    namespace = "wikimedia/wikipedia"
    @staticmethod
    def get_metadata(verbose=False):
        meta = dataset_struct(
            path=WikipediaDataset.namespace,
            modality=["text"],
            source="https://huggingface.co/datasets/wikimedia/wikipedia",
            description="Wikipedia dataset containing cleaned articles of all languages.",
            subsets={}
        )
        for subs in list_repo_tree(WikipediaDataset.namespace, repo_type="dataset"):
            if type(subs)!=RepoFolder:
                continue
            meta["subsets"][subs.path] = subset_struct(
                path=os.path.join(WikipediaDataset.namespace, subs.path),
                downloaded=0,
                formats=["parquet"],
                partitions={}
            )
            if verbose:
                print(f"retrieving info from {subs.path}")
            for part in list_repo_tree(WikipediaDataset.namespace,path_in_repo=subs.path, repo_type="dataset", expand=True):
                path = os.path.join(WikipediaDataset.namespace, part.path)
                download_path = os.path.join(DATA_DIR,path)
                downloaded = os.path.exists(download_path)
                meta["subsets"][subs.path]["partitions"][os.path.basename(part.path)]=partition_struct(
                    path=path,
                    size=part.size,
                    downloaded=downloaded,
                    n_samples=compute_nsamples(download_path) if downloaded else 0
                )
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
    def process_samples(samples:pd.DataFrame)->AttrDict:
        data = AttrDict([(col,[]) for col in samples.columns])
        for row in samples.itertuples():
            for col in samples.columns:
                data[col].append(getattr(row, col))
        return data
