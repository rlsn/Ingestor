# Pygestor
A data interface designed to seamlessly acquire, organize, and manage diverse datasets, offering AI researchers a one-line downloader and data-loader for quick access to data, while providing a scalable and easily manageable system for future dataset acquisition.

[![Python application](https://github.com/rlsn/Ingestor/actions/workflows/python-app.yml/badge.svg)](https://github.com/rlsn/Ingestor/actions/workflows/python-app.yml)
[![Publish Python Package](https://github.com/rlsn/Pygestor/actions/workflows/python-publish.yml/badge.svg)](https://github.com/rlsn/Pygestor/actions/workflows/python-publish.yml)
![GitHub deployments](https://img.shields.io/github/deployments/rlsn/Pygestor/pypi)
![GitHub Release](https://img.shields.io/github/v/release/rlsn/Pygestor)
![PyPI - Version](https://img.shields.io/pypi/v/pygestor)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Key Features
- Dataset Acquisition:
     - Support for downloading and loading datasets with a simple one-line command.
     - Automatic handling of subsets and partitions for efficient data storage and access.

- Data Organization:
    - Three-level data organization structure: dataset, subset, and partition.
    - Support for both local and network file systems for data storage.
    - Efficient handling of large files by allowing batched loading.

- Web Interface
    - Introduced a web UI for intuitive data management and analysis.
    - Support for viewing schema, metadata and data samples.
    - Ability to download and remove one subset or multiple partitions in one go.
    - Support for data searching and sorting.
    - Ability to generate code snippets for quick access to datasets.

## Quick Start
### Installation
```
pip install -r requirements.txt
```
or
```
pip install pygestor
```
The module can be used with a webUI, terminal commands or Python APIs (more functionalities). For Python APIs introductions please refer to [this notebook](notebooks/api_demo.ipynb).

### Configurations
Edit [`confs/system.conf`](confs/system.conf) to change the default system settings. In particular, set `data_dir` to the desired data storage location, either a local path or a cloud NFS.

### Run GUI
```
python .\run-gui.py
```

### Data info and availability
```
# list support datasets: 
python cli.py -l

# list subsets in a datatset:
python cli.py -l -d <dataset_name>

# list partitions in a subset:
python cli.py -l -d <dataset_name> -s <subset_name>
```
### Dataset management and extension
To download a specific subset:
```
python cli.py -l -d <dataset_name> -s <subset_name>
```
To remove downloaded data files in a subset:
```
python cli.py -r -d <dataset_name> -s <subset_name>
```

**To support a new dataset, add a new class file to [`pygestor/datasets`](pygestor/datasets) that defines how to organize, download and load data, following the example in [`pygestor/datasets/wikipedia.py`](pygestor/datasets/wikipedia.py). Then update the metadata by running `python cli.py -init -d <new_dataset_name>`**


## Technical Details
### Storage
The data is stored in a file storage system and organized into three levels: dataset, subset (distinguished by version, language, class, split, annotation, etc.), and partition (splitting large files into smaller chunks for memory efficiency), as follows:

```
dataset_A
├── subset_a
│   ├── partition_1
│   └── partition_2
└── subset_b
    ├── partition_1
    └── partition_2
...
```
File storage is used for its comparatively high cost efficiency, scalability, and ease of management compared to other types of storage.

The dataset info and storage status is tracked by a metadata file `metadata.json` for efficient reference and update.
### Dependencies
- python >= 3.11
- huggingface_hub: Provides native support for datasets hosted on Hugging Face, making it an ideal library for downloading.
- pyarrow: Used to compress and extract parquet files, a data file format designed for efficient data storage and retrieval.
- pandas: Used to structure the dataset info tabular form for downstream data consumers. It provides a handy API for data manipulation and access, as well as chunking and datatype adjustments for memory efficiency.
- nicegui (optional): Used to serve webUI frontend

## Workflow
Refer to [docs/workflow.md](docs/workflow.md) for comprehensive instructions on how to set up the system, acquire, manage and utilize datasets.
