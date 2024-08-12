"""
This script contains functions for infoview display
rlsn 2024
"""
import os
from nicegui import ui
from pygestor import write_meta, DATA_DIR
from pygestor.utils import compute_subset_download, compute_subset_n_samples, compute_subset_size

def show_dataset_info_list(views, metadata, name):
    info = metadata["datasets"][name]
    new_desc = [info["description"]]
    def on_desc_change(x):
        new_desc[0]=x.value
    def on_save_desc():
        info["description"] = new_desc[0]
        write_meta(metadata)
        ui.notify("Change saved.")
        from gui.dataviewer import show_datasets, show_dataset_info
        show_datasets(views, metadata)
        show_dataset_info(views, metadata, name)


    with ui.grid(columns="auto auto").classes('p-3'):
        ui.label("Dataset")
        ui.label(name)
        ui.label("Modality")
        ui.label(", ".join(info["modality"]))
        ui.label("Path")
        ui.label(os.path.abspath(os.path.join(DATA_DIR, info["path"])).replace("\\","/"))
        ui.label("Source")
        ui.label(info["source"])
        ui.label("Description")
        ui.textarea(value=info["description"], on_change=lambda x: on_desc_change(x))
        ui.label("")
        ui.button("Save description", icon="save", on_click=lambda: on_save_desc())

def show_subset_info_list(views, metadata, path):
    name, subs = path
    info = metadata["datasets"][name]["subsets"][subs]
    new_desc = [info["description"]]
    def on_desc_change(x):
        new_desc[0]=x.value
    def on_save_desc():
        info["description"] = new_desc[0]
        write_meta(metadata)
        ui.notify("Change saved.")
        from gui.dataviewer import show_subsets, show_subset_info
        show_subsets(views, metadata, name)
        show_subset_info(views, metadata, path)

    with ui.grid(columns="auto auto").classes('p-3'):
        ui.label("Dataset")
        ui.label(name)
        ui.label("Subset")
        ui.label(subs)
        ui.label("Size (MB)")
        ui.label(round(compute_subset_size(info)/1e6,3))
        ui.label("Samples")
        ui.label(compute_subset_n_samples(info))
        ui.label("Downloaded")
        ui.label(compute_subset_download(info))
        ui.label("Partitions")
        ui.label(len(info["partitions"]))
        ui.label("Download Path")
        ui.label(os.path.abspath(os.path.join(DATA_DIR, info["path"])).replace("\\","/"))
        ui.label("Formats")
        ui.label(", ".join(info["formats"]))
        ui.label("Description")
        ui.textarea(value=info["description"], on_change=lambda x: on_desc_change(x))
        ui.label("")
        ui.button("Save description", icon="save", on_click=lambda: on_save_desc())

def show_partition_info_list(metadata, path):
    name, subs, part = path
    info = metadata["datasets"][name]["subsets"][subs]["partitions"][part]
    with ui.grid(columns="auto auto").classes('p-3'):
        ui.label("Dataset")
        ui.label(name)
        ui.label("Subset")
        ui.label(subs)
        ui.label("Partition")
        ui.label(part)
        ui.label("Size (MB)")
        ui.label(round(info["size"]/1e6,3))
        ui.label("Samples")
        ui.label(info["n_samples"])
        ui.label("Downloaded")
        ui.label('Yes' if info["downloaded"] else 'No')
        ui.label("Download Path")
        ui.label(os.path.abspath(os.path.join(DATA_DIR, info["path"])).replace("\\","/"))