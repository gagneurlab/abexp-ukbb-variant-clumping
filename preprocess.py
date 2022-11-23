import sys
import pandas as pd
import numpy as np

df = pd.read_csv(sys.argv[1], sep='\t', usecols=["chr", "pos", "ref", "alt", "pval_meta"] , dtype={"chr":str, "pos":int, "ref":str, "alt":str, "pval_meta":float})
#Convert from log to normal values:
df["pval_meta_raw"] = np.exp(df["pval_meta"])
#Only keep variants that are below significance threshold:
df = df[df["pval_meta_raw"]<=float(sys.argv[3])]
#Set variant IDs to match the IDs in the plink2 genotype data:
df["varid"] = "chr" + df["chr"].astype(str) + ":" + df["pos"].astype(str) + ":" + df["ref"] + ">" + df["alt"]
df.to_csv(sys.argv[2], sep='\t', index=False)