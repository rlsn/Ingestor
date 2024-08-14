"""
An API for ingesting wikimedia/wikipedia dataset at https://huggingface.co/datasets/bigdata-pw/Diffusion1B
rlsn 2024
"""

import os
import pandas as pd
import time
from huggingface_hub import hf_hub_download, list_repo_files, get_paths_info
from ..dataset_wrapper import BaseDataset, Dataset, dataset_struct, subset_struct, partition_struct
from ..__init__ import DATA_DIR, CACHE_DIR, AUTO_CLEAR_CACHE, DEFAULT_SUBSET_NAME
from ..utils import compute_nsamples, all_partitions, divide_chunks, joinpath, AttrDict

@Dataset.register('HuggingFaceParquet')
class HuggingFaceParquetDataset(BaseDataset):
    namespace = "HuggingFaceParquet"
    abstract = True
    @classmethod
    def get_metadata(cls, repo_name, url, verbose=False):
        """retrieve metadata from url
        Args:
            repo_name (str): a unique repo_name for this dataset in your storage 
            url (str): https://huggingface.co/datasets/{repo_id}
            verbose (bool, optional): verbose mode. Defaults to False.

        Returns:
            _type_: success or not
        """
        repo_id = url.split("huggingface.co/datasets/")[-1]

        meta = dataset_struct(
            path=repo_name,
            formats="parquet",
            source=url,
            dataset_class=HuggingFaceParquetDataset.namespace,
        )
        
        parquets = []
        for path in list_repo_files(repo_id, repo_type="dataset"):
            if path.endswith("parquet"):
                parquets.append(path)
        
        paths_info = []
        for chunks in divide_chunks(parquets, 100):
            paths_info.extend(get_paths_info(repo_id, chunks, repo_type="dataset"))
        for info in paths_info:
            if verbose:
                print(f"retrieving info from {info.path}")

            path = info.path.split("/")
            part = path[-1]
            subs = path[-2] if len(path)>1 else DEFAULT_SUBSET_NAME

            if subs not in meta["subsets"]:
                meta["subsets"][subs] = subset_struct(
                    path=joinpath(repo_name, subs),
                )
            part_path = joinpath(repo_name, subs, part)
            download_path = joinpath(DATA_DIR, part_path)
            downloaded = os.path.exists(download_path)
            meta["subsets"][subs]["partitions"][part]=partition_struct(
                path=part_path,
                size=info.size,
                downloaded=downloaded,
                hf_path=info.path,
                n_samples=compute_nsamples(download_path) if downloaded else 0,
                acquisition_time=time.time() if downloaded else None
            )
        return meta
    
    @classmethod
    def download(cls, datapath):
        from ..core_api import get_meta
        name,_,_ = datapath
        repo_id = get_meta(datapath[0])["source"].split("huggingface.co/datasets/")[-1]
        part_info = get_meta(*datapath)
        download_path = joinpath(DATA_DIR, part_info["path"])
        blob_id = get_paths_info("wikimedia/wikipedia", part_info["hf_path"], repo_type="dataset")[0].blob_id

        os.makedirs(os.path.dirname(download_path),exist_ok=True)
        filepath=hf_hub_download(repo_id=repo_id,
                        filename=part_info["hf_path"],
                        force_download = True,
                        local_dir=CACHE_DIR,
                        cache_dir=CACHE_DIR,repo_type="dataset")
        part_info["blob_id"] = blob_id
        os.replace(filepath, download_path)
        if AUTO_CLEAR_CACHE:
            from ..core_api import clear_cache
            clear_cache()
        return download_path
    
    @classmethod
    def check_update_to_date(cls, name):
        up_to_date = True
        for part in all_partitions(name):
            if part["downloaded"]:
                latest_blob = get_paths_info(name, [part["hf_path"]], repo_type="dataset")[0].blob_id
                part["is_latest"] = "blob_id" in part and part["blob_id"]==latest_blob
                up_to_date &= part["is_latest"]
        return up_to_date

    @classmethod
    def process_samples(cls, samples:pd.DataFrame)->AttrDict:
        data = AttrDict([(col,[]) for col in samples.columns])
        for row in samples.itertuples():
            for col in samples.columns:
                data[col].append(getattr(row, col))
        return data