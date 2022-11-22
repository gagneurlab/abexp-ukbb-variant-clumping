# UKBB_CLUMP
A Snakemake pipeline for clumping GWAS results from the UK-Biobank using plink.
First, GWAS results are downloaded from the UK-Biobank. After some formatting steps, plink --clump is used in order to find a set of independent and significantly associated variants (index variants). Finally, allele counts of the index variants are extracted for all individuals found in the plink-binary genotype data.
# Usage
For use within the lab you can activate the conda environment *prs*:
```bash
conda activate prs
```
or use the *environment.yaml* to create a new conda environment with the necessary dependencies.

Clumping parameters can be set the config.yaml. See [this table](https://docs.google.com/spreadsheets/d/1AeeADtT0U1AukliiNyiVzVRdLYPkTbruQSk38DeutU8/edit#gid=903887429) for an overview of UKBB phenotype manifest. The *filename* in the config.yaml needs to be set as the value from the *filename* field (column BW) from the manifest for the desired phenotype. Then call the pipeline as follows to run the pipeline on the plink2 genomic data specified in the config.yaml:
```bash
snakemake /some_dir/GWAS_variants_clumped_mac.parquet
```

Then runtime of the pipeline strongly depends on the number of variants in the GWAS results.

See *./notebooks/gene_var_intersect.ipynb* for how to read the allele counts for some given gene ID after running the pipeline.