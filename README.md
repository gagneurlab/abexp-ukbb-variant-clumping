# UKBB_CLUMP
A Snakemake pipeline for clumping GWAS results from the UK-Biobank using plink.
First, GWAS results are downloaded from the UK-Biobank. After some formatting steps, plink --clump is used in order to find a set of independent and significantly associated variants (index variants). Finally, minor allele counts of the index variants are extracted for all individuals found in the plink-binary genotype data.
# Usage
For use within the lab you can activate the conda environment *prs*:
```bash
conda activate prs
```
or use the *environment.yaml* to create a new conda environment with the necessary dependencies.

Given the phenocode *30760* for HDL-Cholesterol:
```bash
snakemake /some_dir/GWAS_variants_clumped_mac_30760.parquet
```
runs the clumping pipeline for *30760* on the plink2 genomic data specified in the config.yaml.
Clumping parameters can be set the config.yaml. Depending on the phenotype, the *trait_type* parameter also needs to be set. See [this table](https://docs.google.com/spreadsheets/d/1AeeADtT0U1AukliiNyiVzVRdLYPkTbruQSk38DeutU8/edit#gid=903887429) for an overview of UKBB phenotype manifest.

Then runtime of the pipeline strongly depends on the number of variants in the GWAS results.

See *./notebooks/gene_var_intersect.ipynb* for how to read the allele counts for some given gene ID after running the pipeline.