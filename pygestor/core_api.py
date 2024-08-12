"""
This script contains core functional APIs of the module
rlsn 2024
"""
import os, shutil
import json
import pandas as pd
from typing import Generator
from .dataset_wrapper import Dataset
from .__init__ import DATA_DIR, META_PATH, CACHE_DIR, DEFAULT_SUBSET_NAME
from .utils import AttrDict, compute_nsamples, load_parquets, load_parquets_in_batch, compute_subset_download, compute_subset_size, normpath

def clear_cache():
    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR, ignore_errors=True)
    os.makedirs(CACHE_DIR,exist_ok=True)
    
def load_meta():
    try:
        with open(META_PATH, "r") as fp:
            metadata = json.load(fp)
    except:
        raise Exception("metadata file is corrupted or not initialized, try initialize it first with 'initialize()'")
    return metadata

def write_meta(metadata):
    with open(META_PATH, "w") as fp:
        json.dump(metadata, fp, ensure_ascii=True, indent=4)

def initialize_root():
    metadata = dict()

    metadata["data_root"] = DATA_DIR
    metadata["cache_dir"] = CACHE_DIR
    metadata["datasets"] = dict()

    return metadata

def import_from_path(path=None, name=None, subset=None, partition=None):
    pass

def initialize_dataset(metadata, name, verbose=False):
    data_info = Dataset.get(name).get_metadata(verbose=verbose)
    metadata["datasets"][name] = data_info


def initialize(name=None, verbose=True):
    # run the dataset survey, update the metadata based on survey and data inventory
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


    if name is not None:
        initialize_dataset(metadata, name, verbose=verbose)
    else:
        print("[INFO] no dataset specified, will update all registered datasets.")
        for ds in Dataset._dataset_classes:
            print(f"[INFO] updating metadata for {ds}")
            initialize_dataset(metadata, ds, verbose=verbose)

    write_meta(metadata)
    print("[INFO] metadata file updated.")
    
def list_datasets(display=True):
    root = load_meta()
    metadata = root["datasets"]
    datasets = sorted(list(metadata.keys()))
    if display:
        print(f"{'dataset name':^25}|{'modality':^25}|{'description':^30}")
        for ds in datasets:
            print(f"{ds:<25}|{','.join(metadata[ds]['modality']):^25}|{metadata[ds]['description']}")
    return datasets

def list_subsets(name, display=True):
    root = load_meta()
    metadata = root["datasets"]

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")
    
    subsets = list(metadata[name]["subsets"])
    subsets = sorted(subsets, key=lambda x: (-metadata[name]["subsets"][x]["downloaded"],x))
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
    root = load_meta()
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
    root = load_meta()
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

        shutil.rmtree(normpath(os.path.join(DATA_DIR, metadata[name]["path"])), ignore_errors=True)

        # update metadata
        for subset in metadata[name]['subsets']:
            for part in metadata[name]['subsets'][subset]['partitions']:
                metadata[name]['subsets'][subset]['partitions'][part]['downloaded']=False
                metadata[name]['subsets'][subset]['partitions'][part]['n_samples']=0

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
        shutil.rmtree(normpath(os.path.join(DATA_DIR, info["path"])), ignore_errors=True)
        # update metadata
        for part in info['partitions']:
            info['partitions'][part]['downloaded']=False
            info['partitions'][part]['n_samples']=0
        write_meta(root)
        print(f"[INFO] {info['path']} deleted")
        return
    
    # remove partitions
    for part in partitions:
        info = metadata[name]["subsets"][subset]["partitions"][part]
        shutil.rmtree(normpath(os.path.join(DATA_DIR, info["path"])), ignore_errors=True)
        # update metadata
        info['downloaded']=False
        info['n_samples']=0
        print(f"[INFO] {info['path']} deleted")
    write_meta(root)

def download(name:str, subset:str=None, partitions:list=None, force_redownload:bool=False)->None:
    root = load_meta()
    metadata = root["datasets"]
    os.makedirs(DATA_DIR,exist_ok=True)
    os.makedirs(CACHE_DIR,exist_ok=True)

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")

    if subset is None:
        subset = DEFAULT_SUBSET_NAME
        print(f"[INFO] subset name not specified, using default name '{DEFAULT_SUBSET_NAME}'")

    if subset not in metadata[name]["subsets"]:
        raise Exception(f"[ERROR] subset '{subset}' not found in '{name}'.")

    data_cls = Dataset.get(name)
    data_info = metadata[name]["subsets"][subset]

    if partitions is None:
        # default as all partitions
        partitions = list(data_info["partitions"].keys())


    for i, part in enumerate(partitions):
        info = data_info["partitions"][part]
        if not info["downloaded"] or force_redownload:
            print(f"[INFO] [{i+1}/{len(partitions)}] downloading {info['path']}")
            downloaded_path = data_cls.download(subset, part)
            # update download info
            info["downloaded"]=True
            data_info["downloaded"] = compute_subset_download(data_info)
            # compute num samples
            info["n_samples"] = compute_nsamples(downloaded_path)

            write_meta(root)

    print("[INFO] downloading complete.")

def get_filepaths(name, subset=None, partitions=None, download_if_missing=False):
    filepaths = []
    root = load_meta()
    metadata = root["datasets"]
    os.makedirs(DATA_DIR,exist_ok=True)
    os.makedirs(CACHE_DIR,exist_ok=True)

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")

    if subset is None:
        subset = DEFAULT_SUBSET_NAME
        print(f"[INFO] subset name not specified, using default name '{DEFAULT_SUBSET_NAME}'")

    if subset not in metadata[name]["subsets"]:
        raise Exception(f"[ERROR] subset '{subset}' not found in '{name}'.")

    data_cls = Dataset.get(name)
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

    download(name, subset, tbd_parts)
    filepaths = [normpath(os.path.join(DATA_DIR, data_info["partitions"][part]["path"])) for part in tbd_parts]
    return filepaths

def load_dataset(name:str, subset:str=None, partitions:list=None, download_if_missing:bool=False, **kwargs)->pd.DataFrame:
    
    filepaths = get_filepaths(name, subset, partitions, download_if_missing)

    if len(filepaths)==0:
        return []
    
    data = load_parquets(filepaths)
    return data

def stream_dataset(name:str, subset:str=None, partitions:list=None, download_if_missing:bool=False, batch_size:int=16, preprocess:bool=False)->Generator[pd.DataFrame,None,None]:

    filepaths = get_filepaths(name, subset, partitions, download_if_missing)

    if len(filepaths)==0:
        yield []

    if batch_size>0:
        batches = load_parquets_in_batch(filepaths, batchsize=batch_size)
        yield from batches

def process_samples(name:str, samples:pd.DataFrame)->AttrDict:
    return Dataset.get(name).process_samples(samples)