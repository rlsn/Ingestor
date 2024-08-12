# Pygestor
A platform designed to seamlessly acquire, organize, and manage diverse datasets, offering AI researchers a one-line downloader and data-loader for quick access to data, while providing a scalable and easily manageable system for future dataset acquisition.

[![Python application](https://github.com/rlsn/Ingestor/actions/workflows/python-app.yml/badge.svg)](https://github.com/rlsn/Ingestor/actions/workflows/python-app.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
## Quick Start
### Install dependencies
```
pip install -r requirements.txt
python .\run-gui.py
```
The module can be used with terminal commands or Python APIs (more functionalities). For Python APIs use cases please refer to [this notebook](api_demo.ipynb).

### Configurations
Edit [`pygestor/__init__.py`](pygestor/__init__.py) to change the default system settings. In particular, set `DATA_DIR` to the desired data storage location, either a local path or a remote path, such as a mounted NFS.

### Data info and availability
To list support datasets: 
```
python cli.py -l
```
To list subsets in a datatset:
```
python cli.py -l -d <dataset_name>
```
To list partitions in a subset:
```
python cli.py -l -d <dataset_name> -s <subset_name>
```
### Dataset management and extension
To download a specific subset:
```
python cli.py -l -d <dataset_name> -s <subset_name>
```
To download specific partitions, use Python API `pygestor.download()`.

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
File storage is chosen for its comparatively high cost efficiency, scalability, and ease of management compared to other types of storage.

The dataset info and storage status is tracked by a metadata file `metadata.json` for efficient reference and update.
### Dependencies
- python >= 3.11
- huggingface_hub: Provides native support for datasets hosted on Hugging Face, making it an ideal library for downloading.
- pyarrow: Used to compress and extract parquet files, a data file format designed for efficient data storage and retrieval, compatible with pandas.
- pandas: Used to load the text dataset into memory for downstream data consumers. It provides a handy API for data manipulation and access, as well as chunking and datatype adjustments for memory efficiency.
