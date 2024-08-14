"""
This script contains core functional APIs of the module
rlsn 2024
"""
import os, shutil
import json
import pandas as pd
import time
from typing import Generator
from .dataset_wrapper import BaseDataset, Dataset
from .__init__ import DATA_DIR, META_PATH, CACHE_DIR, DEFAULT_SUBSET_NAME
from .utils import AttrDict, compute_nsamples, load_parquets, load_parquets_in_batch, compute_subset_download, compute_subset_size, joinpath
    
_metadata = dict()

def initialize_root():
    metadata = dict()

    metadata["data_root"] = DATA_DIR
    metadata["cache_dir"] = CACHE_DIR
    metadata["datasets"] = dict()

    return metadata

def load_meta():
    try:
        with open(META_PATH, "r") as fp:
            metadata = json.load(fp)
        print("[INFO] loaded metadata file.")
        
    except:
        prompt = input("[WARNING] metadata file is corrupted or not initialized, a new file will be created. Continue?[y/n]")
        if prompt.lower()!="y":
            print("[INFO] initialization aborted.")
            return
        # create a backup in case of bad decision
        if os.path.exists(META_PATH):
            shutil.copyfile(META_PATH, META_PATH+'.bak')
        metadata = initialize_root()

    for k in metadata:
        _metadata[k] = metadata[k]
    return _metadata

load_meta()

def get_meta(*args):
    path = list(args)
    if len(path)==0:
        return _metadata
    metadata = _metadata["datasets"][path.pop(0)]
    if len(path)>0:
        metadata = metadata["subsets"][path.pop(0)]
    if len(path)>0:
        metadata = metadata["partitions"][path.pop(0)]
    return metadata

def write_meta(metadata=None):
    if metadata is not None:
        for k in metadata:
            _metadata[k] = metadata[k]

    with open(META_PATH, "w") as fp:
        json.dump(_metadata, fp, ensure_ascii=True, indent=4)
    print("[INFO] metadata file updated.")

def clear_cache():
    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR, ignore_errors=True)
    os.makedirs(CACHE_DIR,exist_ok=True)

def get_data_cls(name):
    if name not in Dataset._dataset_classes:
        data_cls = Dataset.get(get_meta()["datasets"][name]["dataset_class"])
    else:
        data_cls = Dataset.get(name)
    return data_cls

def import_from_path(path=None, name=None, subset=None, partition=None):
    pass

def initialize_dataset(name:str, dataset_cls:str=None, verbose:bool=False, **kargs)->bool:
    metadata = get_meta()
    ret = True
    try:
        if Dataset.get(name) is not None and not Dataset.get(name).abstract:
            data_info = Dataset.get(name).get_metadata(verbose=verbose)
            metadata["datasets"][name] = data_info
    
        elif dataset_cls is not None:
            data_info = Dataset.get(dataset_cls).get_metadata(name, verbose=verbose, **kargs)
            metadata["datasets"][name] = data_info

        write_meta(metadata)
    except Exception as e:
        print(f"[ERROR] Initialization failed:\n {e}")
        ret = False
    return ret

def remove_dataset_metadata(name):
    meta = get_meta()
    if name in meta["datasets"]:
        del meta["datasets"][name]
        print(f"[INFO] removed {name} from metadata.")
    write_meta(meta)

def initialize(name:str=None, dataset_id:str=None, verbose:bool=True)->bool:
    ret = True

    if name is not None:
        ret &= initialize_dataset(name, dataset_id, verbose=verbose)
    else:
        if input("[INFO] no dataset specified, will update all registered datasets.[y/n]").lower()=='y':
            for ds in Dataset._dataset_classes:
                print(f"[INFO] updating metadata for {ds}")
                ret &= initialize_dataset(ds, verbose=verbose)
    return ret

def list_datasets(display=True):
    root = get_meta()
    metadata = root["datasets"]
    datasets = sorted(list(metadata.keys()))
    if display:
        print(f"{'dataset name':^25}|{'modality':^25}|{'description':^30}")
        for ds in datasets:
            print(f"{ds:<25}|{metadata[ds]['modality']:^25}|{metadata[ds]['description']}")
    return datasets

def list_subsets(name, display=True):
    root = get_meta()
    metadata = root["datasets"]

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")
    
    subsets = list(metadata[name]["subsets"])
    subsets = sorted(subsets, key=lambda x: (-compute_subset_download(metadata[name]["subsets"][x]),x))
    if display:
        print(f"dataset name: {name}\n{'subsets':^25}|{'downloaded partitions':^25}|{'size(MB)':^10}|path")
        for subs in subsets:
            info = metadata[name]["subsets"][subs]
            downloaded = compute_subset_download(info)
            downloaded_str = str(compute_subset_download(info))+'/'+str(len(info['partitions']))
            size = compute_subset_size(info)/1e6
            print(f"{subs:<25}|{downloaded_str:^25}|{size:<10.2f}|{info['path'] if downloaded>0 else ''}")
    return subsets

def list_partitions(name, subset=None, display=True):
    root = get_meta()
    metadata = root["datasets"]
    
    if subset is None:
        subset = DEFAULT_SUBSET_NAME
        print(f"[INFO] subset name not specified, using default name '{DEFAULT_SUBSET_NAME}'")

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")

    if subset not in metadata[name]["subsets"]:
        raise Exception(f"[ERROR] subset {subset} not found in {name}.")
    
    subs_info = metadata[name]['subsets'][subset]
    partitions = sorted(list(subs_info["partitions"]))

    if display:
        print(f"dataset name: {name}\nsubset name: {subset}")
        downloaded = compute_subset_download(subs_info)
        print(f"downloaded: {downloaded}/{len(subs_info['partitions'])}")
        print(f"{'partition':^30}|{'downloaded':^15}|{'size(MB)':^10}|path")
        for part in partitions:
            info = subs_info["partitions"][part]
            print(f"{part:<30}|{'✔' if info['downloaded'] else 'X':^15}|{info['size']/1e6:<10.3f}|{info['path'] if info['downloaded'] else ''}")
        
    return partitions


def remove(name, subset=None, partitions=[], force_remove=False):
    root = get_meta()
    metadata = root["datasets"]

    if name not in metadata:
        raise Exception(f"[ERROR] dataset '{name}' not found.")
        
    if subset is None:
        # remove dataset
        print(f"[WARNING] Subset is name not specified, you are about to remove all downloaded subsets from {name}.")

        if not force_remove:
            prompt = input("[WARNING] Once deleted, they cannot be restored. type 'REMOVE' to confirm >")
            if prompt.lower()!="remove":
                print("[INFO] Deletion aborted")
                return

        shutil.rmtree(joinpath(DATA_DIR, metadata[name]["path"]), ignore_errors=True)

        # update metadata
        for subset in metadata[name]['subsets'].values():
            for part in subset['partitions'].values():
                part['downloaded']=False
                part['n_samples']=0

        write_meta(root)
        print(f"[INFO] {metadata[name]['path']} deleted")
        return

    if subset not in metadata[name]["subsets"]:
        raise Exception(f"[ERROR] subset '{subset}' not found in '{name}'.")

    if len(partitions)<1:
        # remove subset
        print(f"[WARNING] You are about to remove all downloaded data in {subset} from {name}.")

        if not force_remove:
            prompt = input("[WARNING] Once deleted, they cannot be restored. type 'REMOVE' to confirm >")
            if prompt.lower()!="remove":
                print("[INFO] Deletion aborted")
                return
        info = metadata[name]["subsets"][subset]
        shutil.rmtree(joinpath(DATA_DIR, info["path"]), ignore_errors=True)
        # update metadata
        for part in info['partitions']:
            part_info = info['partitions'][part]
            part_info['downloaded']=False
            part_info['n_samples']=0
            part_info["acquisition_time"]=None
        write_meta(root)
        print(f"[INFO] {info['path']} deleted")
        return
    
    # remove partitions
    for part in partitions:
        info = metadata[name]["subsets"][subset]["partitions"][part]
        shutil.rmtree(joinpath(DATA_DIR, info["path"]), ignore_errors=True)
        # update metadata
        info['downloaded']=False
        info['n_samples']=0
        print(f"[INFO] {info['path']} deleted")
    write_meta(root)

def download(name:str, subset:str=None, partitions:list=None, force_redownload:bool=False, verbose:bool=True)->None:
    root = get_meta()
    metadata = root["datasets"]
    os.makedirs(DATA_DIR,exist_ok=True)
    os.makedirs(CACHE_DIR,exist_ok=True)

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")

    if subset is None:
        subset = DEFAULT_SUBSET_NAME
        if verbose:
            print(f"[INFO] subset name not specified, using default name '{DEFAULT_SUBSET_NAME}'")

    if subset not in metadata[name]["subsets"]:
        raise Exception(f"[ERROR] subset '{subset}' not found in '{name}'.")

    data_cls = get_data_cls(name)    
    data_info = metadata[name]["subsets"][subset]

    if partitions is None:
        # default as all partitions
        partitions = list(data_info["partitions"].keys())

    for i, part in enumerate(partitions):
        info = data_info["partitions"][part]
        if verbose:
            print(f"[INFO] [{i+1}/{len(partitions)}] downloading {info['path']}")

        if not info["downloaded"] or force_redownload:
            downloaded_path = data_cls.download((name, subset, part))
            # update download info
            info["downloaded"]=True
            # compute num samples
            info["n_samples"] = compute_nsamples(downloaded_path)
            # timestamp
            info["acquisition_time"] = time.time()
            write_meta(root)

    if verbose:
        print("[INFO] downloading complete.")

def get_filepaths(name, subset=None, partitions=None, download_if_missing=False, verbose:bool=False, **kargs):
    filepaths = []
    root = get_meta()
    metadata = root["datasets"]
    os.makedirs(DATA_DIR,exist_ok=True)
    os.makedirs(CACHE_DIR,exist_ok=True)

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")

    if subset is None:
        subset = DEFAULT_SUBSET_NAME
        if verbose:
            print(f"[INFO] subset name not specified, using default name '{DEFAULT_SUBSET_NAME}'")

    if subset not in metadata[name]["subsets"]:
        raise Exception(f"[ERROR] subset '{subset}' not found in '{name}'.")

    data_info = metadata[name]["subsets"][subset]

    if partitions is None:
        # default as all partitions
        partitions = list(data_info["partitions"].keys())

    tbd_parts = []
    if not download_if_missing:
        # print(f"[WARNING] partition {part} is not downloaded and therefore skipped.")
        for part in partitions:
            if not data_info["partitions"][part]["downloaded"]:
                continue
            tbd_parts.append(part)
    else:
        tbd_parts = partitions

    download(name, subset, tbd_parts, verbose=verbose)
    filepaths = [joinpath(DATA_DIR, data_info["partitions"][part]["path"]) for part in tbd_parts]
    return filepaths

def version_check(name:str)->bool:
    metadata = get_meta()
    data_cls:BaseDataset = get_data_cls(name)
    is_updated = data_cls.check_update_to_date(name)
    write_meta(metadata)
    return is_updated

def load_dataset(name:str, subset:str=None, partitions:list=None, download_if_missing:bool=False, **kwargs)->pd.DataFrame:
    
    filepaths = get_filepaths(name, subset, partitions, download_if_missing, **kwargs)

    if len(filepaths)==0:
        return []
    
    data = load_parquets(filepaths)
    return data

def stream_dataset(name:str, subset:str=None, partitions:list=None, download_if_missing:bool=False, batch_size:int=16, preprocess:bool=False, **kwargs)->Generator[pd.DataFrame,None,None]:

    filepaths = get_filepaths(name, subset, partitions, download_if_missing, **kwargs)

    if len(filepaths)==0:
        yield []

    if batch_size>0:
        batches = load_parquets_in_batch(filepaths, batchsize=batch_size)
        yield from batches

def process_samples(name:str, samples:pd.DataFrame)->AttrDict:
    if name not in Dataset._dataset_classes:
        return Dataset.get(get_meta()["datasets"][name]["dataset_class"]).process_samples(samples)
    else:
        return Dataset.get(name).process_samples(samples)