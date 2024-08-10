#!/usr/bin/env python
"""
This script runs the ingestor module in command line.
rlsn 2024
"""
import argparse
import ingestor

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-init', '--initialize', type=str, help="initialize metadata")

    parser.add_argument('-l', '--list', action='store_true', help="list available datasets/subsets/partitions as specified (using -d and -s)")
    parser.add_argument('-d', '--dataset_name', type=str, help="specify dataset", default=None)
    parser.add_argument('-s', '--subset_name', type=str, help="specify subset", default=None)
    parser.add_argument('-r', '--remove', action='store_true', help="remove downloaded data as specified (using -d and -s)")
    parser.add_argument('-u', '--download', action='store_true', help="download the specified subset of a dataset (using -d and -s)")

    args = parser.parse_args()

    if args.initialize:
        ingestor.initialize(args.dataset_name, verbose=True)

    elif args.list:
        if args.dataset_name is not None and args.subset_name is not None:
            ingestor.list_partitions(args.dataset_name, args.subset_name)
        elif args.dataset_name is not None:
            ingestor.list_subsets(args.dataset_name)
        else:
            ingestor.list_datasets()
    
    elif args.download:
        ingestor.download(args.dataset_name, args.subset_name)

    elif args.remove:
        ingestor.remove(args.dataset_name, args.subset_name)
