"""
An example script used to download wikimedia/wikipedia/20231101.en
and the first 10 parquet files of wikimedia/wit_base

rlsn 2024
"""
import ingestor

ingestor.download("wikimedia/wikipedia", "20231101.en", force_redownload=False)

parts = ingestor.list_partitions("wikimedia/wit_base", "data", False)[:10]
ingestor.download("wikimedia/wit_base", "data", partitions=parts, force_redownload=False)