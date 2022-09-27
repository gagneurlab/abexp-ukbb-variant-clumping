import sys
import pandas as pd
import numpy as np
import liftover

df = pd.read_csv(sys.argv[1], sep='\t', dtype={"chr":str, "pos":int, "ref":str, "alt":str, "pval_meta":float})
converter = liftover.get_lifter('hg19', 'hg38')
df["pval_meta_raw"] = np.exp(df["pval_meta"])
df = df[df["pval_meta_raw"]<=float(sys.argv[3])]
df["pos_liftover"] = df.apply(lambda v: converter[v["chr"]][v["pos"]][0][1] if converter[v["chr"]][v["pos"]] else 0 ,axis=1)
df["varid"] = "chr" + df["chr"].astype(str) + ":" + df["pos"].astype(str) + ":" + df["ref"] + ">" + df["alt"]
df.to_csv(sys.argv[2], sep='\t', index=False)