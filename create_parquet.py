import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_file = sys.argv[1]
parquet_file = sys.argv[2]

col_names = pd.read_csv(csv_file, sep="\t",nrows=0).columns

non_variant_mac_columns = {"FID", "IID", "PAT", "MAT", "SEX", "PHENOTYPE"}
types_dict = {col: "Int8" for col in col_names if col not in non_variant_mac_columns}

chunksize = 10000
csv_stream = pd.read_csv(csv_file, sep='\t', chunksize=chunksize, dtype=types_dict)

for i, chunk in enumerate(csv_stream):
    print("Chunk", i)
    if i == 0:
        # Guess the schema of the CSV file from the first chunk
        parquet_schema = pa.Table.from_pandas(df=chunk).schema
        # Open a Parquet file for writing
        parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
    # Write CSV chunk to the parquet file
    table = pa.Table.from_pandas(chunk, schema=parquet_schema)
    parquet_writer.write_table(table)

parquet_writer.close()