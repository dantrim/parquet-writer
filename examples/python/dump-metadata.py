#!/usr/bin/env python

"""
Simple script to dump the KeyValueMetadata in a given Parquet file
written by the parquet-writer library.
"""

from argparse import ArgumentParser
from pathlib import Path
import json
import sys

import pyarrow.parquet as pq

def main() :

    parser = ArgumentParser(description = "Dump the KeyValueMetadata from a Parquet file")
    parser.add_argument("input", help = "Path to a Parquet file")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists() or not input_path.is_file() :
        print(f"ERROR: Provided input file (={args.input}) does not exist")
        sys.exit(1)

    parquet_dataset = pq.ParquetDataset(input_path)
    arrow_schema = parquet_dataset.schema.to_arrow_schema()
    keyval_metadata = arrow_schema.metadata
    if keyval_metadata :
        tmp = {}
        for key, val in keyval_metadata.items() :
            key = key.decode("utf-8")
            val = val.decode("utf-8")
            tmp[key] = val
        keyval_metadata = tmp
        if "metadata" in keyval_metadata :
            keyval_metadata = json.loads(keyval_metadata["metadata"])
            print(json.dumps(keyval_metadata, indent = 4))
        else :
            print("{}")
    else :
        print("{}")

if __name__ == "__main__" :
    main()
