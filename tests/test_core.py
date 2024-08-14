"""
An unit test script to test core functionalities

rlsn 2024
"""
import os, sys
sys.path.append(os.getcwd())
import pygestor

def test_core():
    
    pygestor.initialize("wikimedia/wit_base",verbose=True)

    pygestor.list_datasets()
    pygestor.list_subsets("wikimedia/wikipedia")

    pygestor.download("wikimedia/wikipedia", "20231101.ady", force_redownload=True)


    ds = pygestor.load_dataset("wikimedia/wikipedia", "20231101.ch", download_if_missing=True,)
    print(ds)

    pygestor.list_partitions("wikimedia/wikipedia", "20231101.ady")
    pygestor.list_subsets("wikimedia/wikipedia")

    pygestor.version_check("wikimedia/wikipedia")

    ds = pygestor.stream_dataset("wikimedia/wikipedia", "20231101.ch", download_if_missing=False)
    x = pygestor.process_samples("wikimedia/wikipedia", next(iter(ds)))
    print(x)
    for b in ds:
        pass
    pygestor.remove("wikimedia/wikipedia", force_remove=True)


if __name__=="__main__":
    test_core()