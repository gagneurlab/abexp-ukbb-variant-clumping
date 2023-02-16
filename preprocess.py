#!/usr/bin/env python3

import sys
import pandas as pd
import numpy as np

# Read columns names
col_names = pd.read_csv(sys.argv[1], sep="\t", nrows=0).columns

# Check pvalue column name necessary as the UKBB changed the column name from 'pval_meta' (log value) to 'neglog10_pval_meta' (negative log10 value).
if "pval_meta" in col_names:
    df = pd.read_csv(
        sys.argv[1],
        sep="\t",
        usecols=["chr", "pos", "ref", "alt", "pval_meta"],
        dtype={"chr": str, "pos": int, "ref": str, "alt": str, "pval_meta": float},
    )
    # Convert from log to normal values:
    df["pval_meta_raw"] = np.exp(df["pval_meta"])
elif "neglog10_pval_meta" in col_names:
    df = pd.read_csv(
        sys.argv[1],
        sep="\t",
        usecols=["chr", "pos", "ref", "alt", "neglog10_pval_meta"],
        dtype={
            "chr": str,
            "pos": int,
            "ref": str,
            "alt": str,
            "neglog10_pval_meta": float,
        },
    )
    # Convert from neglog to normal values:
    df["pval_meta_raw"] = 10 ** ((-1) * df["neglog10_pval_meta"])
else:
    sys.exit(f"The {sys.argv[1]} file doesn't contain any of the expected pvalue columns.")

# Only keep variants that are below significance threshold:
df = df[df["pval_meta_raw"] <= float(sys.argv[3])]
# Set variant IDs to match the IDs in the plink2 genotype data:
df["varid"] = (
    "chr"
    + df["chr"].astype(str)
    + ":"
    + df["pos"].astype(str)
    + ":"
    + df["ref"]
    + ">"
    + df["alt"]
)

df.to_csv(sys.argv[2], sep="\t", index=False)
