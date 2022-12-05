#!/usr/bin/env python3

import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv

csv_file = sys.argv[1]
parquet_file = sys.argv[2]

print("reading header...")
with open(csv_file, "r") as fd:
    header = fd.readline().rstrip().split("\t")
print("done!")

non_variant_mac_columns = {"FID", "IID", "PAT", "MAT", "SEX", "PHENOTYPE"}
types_dict = {
    **{col: pa.utf8() for col in non_variant_mac_columns},
    **{col: pa.int8() for col in header if col not in non_variant_mac_columns},
}

read_options = pyarrow.csv.ReadOptions()
read_options.use_threads = True
read_options.column_names = header
read_options.block_size = 2**30
read_options.skip_rows = 1
parse_options = pyarrow.csv.ParseOptions()
parse_options.delimiter = "\t"
convert_options = pyarrow.csv.ConvertOptions()
convert_options.column_types = types_dict

#chunksize = 100000

writer = None
with pyarrow.csv.open_csv(
    csv_file,
    read_options=read_options,
    parse_options=parse_options,
    convert_options=convert_options,
) as reader:
    for i, next_chunk in enumerate(reader):
        print(f"Chunk {i}")
        if next_chunk is None:
            break
        if writer is None:
            writer = pq.ParquetWriter(parquet_file, next_chunk.schema)
        next_table = pa.Table.from_batches([next_chunk])
        writer.write_table(next_table)
writer.close()

print("Done!")

