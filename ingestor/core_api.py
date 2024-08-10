"""
This script contains core functional APIs of the module
rlsn 2024
"""
import os
import json
import pandas as pd
from .dataset_wrapper import Dataset
from .__init__ import DATA_DIR, META_PATH, CACHE_DIR, DEFAULT_SUBSET_NAME

def clear_cache():
    import shutil
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

def initialize(name=None, verbose=True):
    # run the dataset survey, update the metadata based on survey and data inventory
    try:
        with open(META_PATH, "r") as fp:
            metadata = json.load(fp)
        print("[INFO] loaded metadata file.")
    except:
        print("[WARNING] metadata file is corrupted or not initialized, a new file will be created.")
        metadata = {}
    if name is not None:
        data_info = Dataset.get(name).get_metadata(verbose=verbose)
        metadata[name] = data_info
    else:
        metadata = {}
        print("[INFO] no dataset specified, will update all registered datasets.")
        for ds in Dataset._dataset_classes:
            print(f"updating metadata for {ds}")
            data_info = Dataset.get(ds).get_metadata(verbose=verbose)
            metadata[ds] = data_info

    write_meta(metadata)
    print("[INFO] metadata file updated.")
    
def list_datasets(display=True):
    datasets = sorted(list(Dataset._dataset_classes.keys()))
    if display:
        for ds in datasets:
            print(ds)
    return datasets

def list_subsets(name, display=True):
    metadata = load_meta()

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")
    
    subsets = list(metadata[name]["subsets"])
    subsets = sorted(subsets, key=lambda x: (-metadata[name]["subsets"][x]["downloaded"],x))
    if display:
        print(f"dataset name: {name}\n{'subsets':^25}|{'downloaded partitions':^25}|{'size(MB)':^10}|path")
        for subs in subsets:
            info = metadata[name]["subsets"][subs]
            downloaded = str(info['downloaded'])+'/'+str(len(info['partitions']))
            size = sum([part['size'] for part in info['partitions'].values()])/1e6
            print(f"{subs:<25}|{downloaded:^25}|{size:<10.2f}|{info['path'] if info['downloaded']>0 else ''}")
    return subsets

def list_partitions(name, subset=None, display=True):
    metadata = load_meta()
    
    if subset is None:
        subset = DEFAULT_SUBSET_NAME
        print(f"[INFO] subset name not specified, using default name '{DEFAULT_SUBSET_NAME}'")

    if name not in metadata:
        raise Exception(f"[ERROR] dataset {name} not found.")

    if subset not in metadata[name]["subsets"]:
        raise Exception(f"[ERROR] subset {subset} not found in {name}.")
    
    partitions = sorted(list(metadata[name]["subsets"][subset]["partitions"]))

    if display:
        print(f"dataset name: {name}\nsubset name: {subset}")
        print(f"downloaded: {metadata[name]['subsets'][subset]['downloaded']}/{len(metadata[name]['subsets'][subset]['partitions'])}")
        print(f"{'partition':^30}|{'downloaded':^15}|{'size(MB)':^10}|path")
        for part in partitions:
            info = metadata[name]["subsets"][subset]["partitions"][part]
            print(f"{part:<30}|{'✔' if info['downloaded'] else 'X':^15}|{info['size']/1e6:<10.3f}|{info['path'] if info['downloaded'] else ''}")
        
    return partitions


def remove(name, subset=None, force_remove=False):
    import shutil
    metadata = load_meta()

    if name not in metadata:
        raise Exception(f"[ERROR] dataset '{name}' not found.")
        
    if subset is None:
        print(f"[WARNING] Subset is name not specified, you are about to remove all downloaded subsets from {name}.")

        if not force_remove:
            prompt = input("[WARNING] Once deleted, they cannot be restored. type 'REMOVE' to confirm.")
            if prompt.lower()!="remove":
                print("[INFO] Deletion aborted")
                return

        shutil.rmtree(metadata[name]["path"], ignore_errors=True)

        # update metadata
        for subset in metadata[name]['subsets']:
            metadata[name]['subsets'][subset]['downloaded']=0
            for part in metadata[name]['subsets'][subset]['partitions']:
                metadata[name]['subsets'][subset]['partitions'][part]['downloaded']=False
        write_meta(metadata)
        print(f"[INFO] {metadata[name]['path']} deleted")
        return

    if subset not in metadata[name]["subsets"]:
        raise Exception(f"[ERROR] subset '{subset}' not found in '{name}'.")

    print(f"[WARNING] You are about to remove all downloaded data in {subset} from {name}.")

    if not force_remove:
        prompt = input("[WARNING] Once deleted, they cannot be restored. type 'REMOVE' to confirm.")
        if prompt.lower()!="remove":
            print("[INFO] Deletion aborted")
            return

    shutil.rmtree(metadata[name]["subsets"][subset]["path"], ignore_errors=True)
    # update metadata
    metadata[name]['subsets'][subset]['downloaded']=0
    for part in metadata[name]['subsets'][subset]['partitions']:
        metadata[name]['subsets'][subset]['partitions'][part]['downloaded']=False
    write_meta(metadata)
    print(f"[INFO] {metadata[name]['subsets'][subset]['path']} deleted")


def download(name, subset=None, partitions=None, force_redownload=False):
    metadata = load_meta()
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
        filepath = data_info["partitions"][part]["path"]

        if not data_info["partitions"][part]["downloaded"] or force_redownload:
            print(f"[INFO] [{i+1}/{len(partitions)}] downloading {part}")
            downloaded_path = data_cls.download(subset, part)
            data_info["partitions"][part]["downloaded"]=True
            data_info["downloaded"] = sum([1 if part["downloaded"] else 0 for part in data_info["partitions"].values()])
            write_meta(metadata)

    print("[INFO] downloading complete.")

def load_dataset(name, subset=None, partitions=None, downloaded_only=False, **kwargs):
    filepaths = []
    metadata = load_meta()
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
    dataframes = []
    for i, part in enumerate(partitions):
        filepath = data_info["partitions"][part]["path"]
        # download if needed
        if not data_info["partitions"][part]["downloaded"]:
            if downloaded_only:
                print(f"[WARNING] partition {part} is not downloaded and therefore skipped.")
                continue
            else:
                print(f"[INFO] [{i+1}/{len(partitions)}] downloading {part}")
                data_cls.download(subset, part)
                data_info["partitions"][part]["downloaded"]=True
                data_info["downloaded"]+=1


        # load
        dataframes.append(data_cls.load_partition(filepath))

    write_meta(metadata)
    print("[INFO] metadata file updated.")
        
    if len(dataframes)==0:
        return []

    data = pd.concat(dataframes, axis=0)
    return data