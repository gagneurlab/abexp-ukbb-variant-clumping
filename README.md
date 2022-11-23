# UKBB_CLUMP
A Snakemake pipeline for clumping GWAS results from the UK-Biobank using plink.
First, GWAS results are downloaded from the UK-Biobank. After some formatting steps, plink --clump is used in order to find a set of independent and significantly associated variants (index variants). Finally, allele counts of the index variants are extracted for all individuals found in the plink-binary genotype data.
# Usage
For use within the lab you can activate the conda environment *prs*:
```bash
conda activate prs
```
or use the *environment.yaml* to create a new conda environment with the necessary dependencies.

Clumping parameters, the location of the plink2 binary genotype data and the output directory are specified in the config.yaml.

The *phenotype_annotation* csv-file specified in the config.yaml lists all phenotypes and GWAS-results which are to be clumped: The column *phenotype* contains the name of the trait and the column *GWAS_result_file* the filename of the GWAS-results as specified in the [UKBB manifest](https://docs.google.com/spreadsheets/d/1AeeADtT0U1AukliiNyiVzVRdLYPkTbruQSk38DeutU8/edit#gid=903887429) in the field *filename* (column BW).

Call the pipeline as follows to run the clumping for all traits listed in the *phenotype_annotation* csv-file:
```bash
snakemake -c16
```

The runtime of the pipeline strongly depends on the number of variants in the GWAS results that have a p-value below the *p_threshold*.

See *./notebooks/gene_var_intersect.ipynb* for how to read the allele counts for some given gene ID after running the pipeline.