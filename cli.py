#!/usr/bin/env python
"""
This script runs the ingestor module in command line.
rlsn 2024
"""
import argparse
import pygestor

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-init', '--initialize', action='store_true', help="initialize metadata")
    parser.add_argument('-c', '--class_id', type=str, help="designate a general dataset class for metadata retrieval (e.g. HuggingFaceParquet)", default=None)
    parser.add_argument('-deinit', '--deinitialize', action='store_true', help="remove a dataset from metadata")

    parser.add_argument('-l', '--list', action='store_true', help="list available datasets/subsets/partitions as specified (using -d and -s)")
    
    parser.add_argument('-d', '--dataset_name', type=str, help="specify dataset", default=None)
    parser.add_argument('-s', '--subset_name', type=str, help="specify subset", default=None)
    parser.add_argument('-r', '--remove', action='store_true', help="remove downloaded data as specified (using -d and -s)")
    parser.add_argument('-u', '--download', action='store_true', help="download the specified subset of a dataset (using -d and -s)")

    args = parser.parse_args()

    if args.initialize:
        pygestor.initialize(args.dataset_name, dataset_id=args.class_id, verbose=True)
    elif args.deinitialize:
        pygestor.remove_dataset_metadata(args.dataset_name)

    elif args.list:
        if args.dataset_name is not None and args.subset_name is not None:
            pygestor.list_partitions(args.dataset_name, args.subset_name)
        elif args.dataset_name is not None:
            pygestor.list_subsets(args.dataset_name)
        else:
            pygestor.list_datasets()
    
    elif args.download:
        pygestor.download(args.dataset_name, args.subset_name)

    elif args.remove:
        pygestor.remove(args.dataset_name, args.subset_name)
