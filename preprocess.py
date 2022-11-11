import sys
import pandas as pd
import numpy as np

df = pd.read_csv(sys.argv[1], sep='\t', dtype={"chr":str, "pos":int, "ref":str, "alt":str, "pval_meta":float})
df["pval_meta_raw"] = np.exp(df["pval_meta"])
df = df[df["pval_meta_raw"]<=float(sys.argv[3])]
df["varid"] = "chr" + df["chr"].astype(str) + ":" + df["pos"].astype(str) + ":" + df["ref"] + ">" + df["alt"]
df.to_csv(sys.argv[2], sep='\t', index=False)
