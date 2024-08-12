"""
An unit test script to test core functionalities

rlsn 2024
"""
import os, sys
sys.path.append(os.getcwd())
import ingestor

def test_core():


    ingestor.initialize("wikimedia/wit_base",verbose=True)

    ingestor.list_datasets()
    ingestor.list_subsets("wikimedia/wikipedia")

    ingestor.download("wikimedia/wikipedia", "20231101.ady", force_redownload=True)


    ds = ingestor.load_dataset("wikimedia/wikipedia", "20231101.ch", download_if_missing=True,)
    print(ds)

    ingestor.list_partitions("wikimedia/wikipedia", "20231101.ady")
    ingestor.list_subsets("wikimedia/wikipedia")

    ds = ingestor.stream_dataset("wikimedia/wikipedia", "20231101.ch", download_if_missing=False)
    x = ingestor.process_samples("wikimedia/wikipedia", next(iter(ds)))
    print(x)

    ingestor.remove("wikimedia/wikipedia", force_remove=True)


if __name__=="__main__":
    test_core()