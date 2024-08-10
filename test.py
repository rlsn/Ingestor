"""
An unit test script to test core functionalities

rlsn 2024
"""
import ingestor

ingestor.initialize("wikimedia/wit_base",verbose=True)

ingestor.list_datasets()
ingestor.list_subsets("wikimedia/wikipedia")

ingestor.download("wikimedia/wikipedia", "20231101.ady", force_redownload=True)


ds = ingestor.load_dataset("wikimedia/wikipedia", "20231101.ch", downloaded_only=False,)
print(ds)

ingestor.list_partitions("wikimedia/wikipedia", "20231101.ady")
ingestor.list_subsets("wikimedia/wikipedia")

ingestor.remove("wikimedia/wikipedia")
