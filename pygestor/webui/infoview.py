"""
This script contains functions for infoview display
rlsn 2024
"""
import os
from nicegui import ui
from datetime import datetime
from pygestor import get_meta, write_meta, DATA_DIR
from pygestor.utils import compute_subset_download, compute_subset_n_samples, compute_subset_size
from pygestor.webui.webui_utils import is_part_latest, is_subs_latest

def display_info(info, name, key):
    if key in info:
        ui.label(name)
        ui.label(info[key])

def display(field, value):
    ui.label(field)
    ui.label(value)

def show_dataset_info_list(name):
    metadata = get_meta()
    info = metadata["datasets"][name]
    new_desc = [info["description"]]
    changes = dict()

    def on_save():
        for k,v in changes.items():
            info[k] = v
        write_meta()
        ui.notify("Change saved.")
        from pygestor.webui.dataviewer import show_datasets, show_dataset_info
        show_datasets()
        show_dataset_info(name)

    with ui.grid(columns="auto auto").classes('p-3'):
        display("Dataset",name)
        ui.label("Modality")
        ui.input(value=info["modality"], on_change=lambda x: changes.update({"modality":x.value})).props('dense')
        display_info(info, "Formats", "formats")
        display("Path",os.path.abspath(os.path.join(DATA_DIR, info["path"])).replace("\\","/"))
        display_info(info, "Source", "source")
        ui.label("Description")
        ui.textarea(value=info["description"], on_change=lambda x: changes.update({"description":x.value}))
        ui.label("")
        ui.button("Save changes", icon="save", on_click=lambda: on_save())

def show_subset_info_list(path):
    info = get_meta(*path)
    name, subs = path
    changes = dict()

    def on_save():
        for k,v in changes.items():
            info[k] = v
        write_meta()
        ui.notify("Change saved.")
        from pygestor.webui.dataviewer import show_subsets, show_subset_info
        show_subsets(name)
        show_subset_info(path)

    with ui.grid(columns="auto auto").classes('p-3'):
        display("Dataset", name)
        display("Subset", subs)
        display("Size (MB)", round(compute_subset_size(info)/1e6,3))

        display("Samples", compute_subset_n_samples(info))
        downloaded = compute_subset_download(info)
        display("Downloaded", downloaded)
        display("Partitions", len(info["partitions"]))
        display("Up-to-date", is_subs_latest(info))
        display("Download Path", os.path.abspath(os.path.join(DATA_DIR, info["path"])).replace("\\","/"))
        ui.label("Description")
        ui.textarea(value=info["description"], on_change=lambda x: changes.update({"description":x.value}))
        ui.label("")
        ui.button("Save changes", icon="save", on_click=lambda: on_save())

def show_partition_info_list(path):
    info = get_meta(*path)
    name, subs, part = path
    with ui.grid(columns="auto auto").classes('p-3'):
        display("Dataset", name)
        display("Subset", subs)
        display("Partition", part)
        display("Size (MB)", round(info["size"]/1e6,3))
        display("Samples", info["n_samples"])
        display("Downloaded", 'Yes' if info["downloaded"] else 'No')
        display("Up-to-date", is_part_latest(info))
        display("Download Path", os.path.abspath(os.path.join(DATA_DIR, info["path"])).replace("\\","/"))
        display("Acquisition Time", datetime.fromtimestamp(info["acquisition_time"]).strftime("%c") if info["acquisition_time"] is not None else "")