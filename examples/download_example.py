"""
An example script used to download wikimedia/wikipedia/20231101.en
and the first 10 parquet files of wikimedia/wit_base

rlsn 2024
"""
import os, sys
sys.path.append(os.getcwd())
import pygestor

pygestor.download("wikimedia/wikipedia", "20231101.en", force_redownload=False)

parts = pygestor.list_partitions("wikimedia/wit_base", "data", False)[:10]
pygestor.download("wikimedia/wit_base", "data", partitions=parts, force_redownload=False)