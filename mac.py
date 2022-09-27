import sys
import pandas as pd

def get_MAC(df_map, df_ped):
    res ={"ID":df_ped["ID"]}
    l = len(df_map["variant"])
    for i, var in enumerate(df_map["variant"]):
        print(f"Progress: {round(100*i/l)}%", end = "\r")
        minor_allel = var.split(">")[1]
        res[var] = ((df_ped[var+"_0"] == minor_allel).astype(int) + (df_ped[var+"_1"] == minor_allel).astype(int))
        break
    return pd.DataFrame(res)

print("Reading Data...")
df_map = pd.read_csv(f"{sys.argv[1][:-4]}.map", names = ["chr", "variant", "pos", "coordinate"],header=None, sep= "\t")
df_ped = pd.read_csv(sys.argv[1], names = ["FID", "ID", "father", "mother", "sex", "pheno"]+[f"{rsid}_{allel}" for rsid in df_map["variant"].to_list() for allel in (0, 1)], header=None, sep= " ", dtype="string")

print("Calculating MAC:")
df = get_MAC(df_map, df_ped)

print("Writing MAC...")
df.to_parquet(f"{sys.argv[2]}.p", engine="pyarrow")