# UKBB_CLUMP
A Snakemake pipeline for clumping GWAS results from the UK-Biobank using plink.
First, GWAS results are downloaded from the UK-Biobank. After some formatting steps, plink --clump is used in order to find a set of independent and significantly associated variants (index variants). Finally, allele counts of the index variants are extracted for all individuals found in the plink-binary genotype data.
# Usage
For use within the lab you can activate the conda environment *prs*:
```bash
conda activate prs
```
or use the *environment.yaml* to create a new conda environment with the necessary dependencies.

Clumping parameters can be specified in the config.yaml. The *filename* in the config.yaml needs to be set as the value from the *filename* field (column BW) from the [UKBB manifest](https://docs.google.com/spreadsheets/d/1AeeADtT0U1AukliiNyiVzVRdLYPkTbruQSk38DeutU8/edit#gid=903887429) for the desired phenotype.
Call the pipeline as follows to run the clumping on the plink2 genomic data specified in the config.yaml:
```bash
snakemake /some_dir/GWAS_variants_clumped_mac.parquet
```

The runtime of the pipeline strongly depends on the number of variants in the GWAS results.

See *./notebooks/gene_var_intersect.ipynb* for how to read the allele counts for some given gene ID after running the pipeline.