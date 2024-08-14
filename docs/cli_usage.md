# Using CLI
This document provides an introduction to the basic usage of the Pygestor CLI.

## Metadata Initialization
```
# all available
python cli.py -init

# single dataset
python cli.py -init -d <dataset_name>
```

## Data info and availability
```
# list support datasets: 
python cli.py -l

# list subsets in a datatset:
python cli.py -l -d <dataset_name>

# list partitions in a subset:
python cli.py -l -d <dataset_name> -s <subset_name>
```

## Dataset management and extension
To download a specific subset:
```
python cli.py -l -d <dataset_name> -s <subset_name>
```
To remove downloaded data files in a subset:
```
python cli.py -r -d <dataset_name> -s <subset_name>
```