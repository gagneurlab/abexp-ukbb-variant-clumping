configfile: "config.yaml"
import pandas as pd

phenotype_df = pd.read_csv(
    config["phenotype_annotation"],
    dtype={
        "phenotype": "string",
        "GWAS_result_file": "string",
    }
)

rule all:
    input:
        expand("{dir}/{phenotype}/GWAS_variants_clumped_mac.parquet", dir=config["output_dir"], phenotype=phenotype_df["phenotype"].tolist())

#Downloads GWAS-results from the UK-Biobank for a given phenotype. The filename of the GWAS-results file for this phenotype is set in the phenotype_annotation file specified in the config.
rule download_GWAS_results:
    threads: 1
    resources:
        mem_mb=500
    output:
        "{dir}/{phenotype}/sumstats.tsv.gz"
    params:
        download_link=lambda wc: f"https://pan-ukb-us-east-1.s3.amazonaws.com/sumstats_flat_files/{phenotype_df.set_index('phenotype').loc[wc.phenotype]['GWAS_result_file']}"
    shell:
        "wget -q -O {output} {params.download_link}"

#Unzips the GWAS-results and removes unnecessary columns
rule extract_GWAS_results:
    threads: 1
    resources:
        mem_mb=lambda wildcards, attempt, threads: (4000 * threads) * attempt
    input:
        "{dir}sumstats.tsv.gz"
    output:
        "{dir}sumstats.tsv"
    shell:
        r"""
        gzip -d -k {input}
        cut -f 1,2,3,4,8,13 {output} > {output}.temp
        rm {output}
        mv {output}.temp {output} 
        """

#Uses a python script in order to convert the GWAS log p values into normal values. Also sets the variant IDs to match with the plink2 binary genotype data. Only variants with a p-value below the threshold are written into the output.
rule preprocess_p:
    threads: 1
    resources:
        mem_mb=lambda wildcards, attempt, threads: (4000 * threads) * attempt
    input:
        "{dir}sumstats.tsv"
    output:
        "{dir}p_values.tsv"
    shell:
        f"python preprocess.py {{input}} {{output}} {config['p_threshold']}"

#Extracts the column containing the variant IDs. This format can then be used by plink2 as a filter.
rule get_var_list:
    threads: 1
    resources:
        mem_mb=lambda wildcards, attempt, threads: (4000 * threads) * attempt
    input:
        "{dir}p_values.tsv"
    output:
        "{dir}varlist.tsv"
    shell:   
        r"awk '{{print $8}}' {input} > {output}"

#Using plink2 the genotype data is extracted for the significant variants and written into plink1.9 format. This is necessary because plink2 does not support clumping (yet).
rule create_subset_binary:
    threads: 16
    resources:
        mem_mb=lambda wildcards, attempt, threads: (4000 * threads) * attempt
    input:
        "{dir}varlist.tsv"
    output:
        "{dir}subset_binary.bed"
    shell:
        f"plink2 --pfile {config['plink2_binary_files']} --extract {{input}} --make-bed --out {{wildcards.dir}}subset_binary"

#Using plink1.9 the variants are clumped by LD and distance. The index variant IDs are written into the output file.
rule clump:
    threads: 16
    resources:
        mem_mb=lambda wildcards, attempt, threads: (4000 * threads) * attempt
    input:
        gwas_res="{dir}p_values.tsv",
        subset_binary="{dir}subset_binary.bed"
    output:
        "{dir}GWAS_variants.clumped.vars"
    shell:
        f"plink -bfile {{wildcards.dir}}subset_binary --clump {{input.gwas_res}} --clump-snp-field varid --clump-field {config['p_val_col']} --clump-p1 {config['p_threshold']} --clump-p2 {config['p_threshold']} --clump-r2 {config['ld_threshold']} --clump-kb {config['kb_radius']} --out {{wildcards.dir}}GWAS_variants" + "\n" + r"awk '{{print $3}}' {wildcards.dir}GWAS_variants.clumped > {output}"

#Using plink2 the allele counts for all individuals are extracted from the genotype data for all index variants
rule get_mac:
    threads: 16
    resources:
        mem_mb=lambda wildcards, attempt, threads: (4000 * threads) * attempt
    input:
        "{dir}GWAS_variants.clumped.vars"
    output:
        "{dir}GWAS_variants_clumped_mac.raw"
    shell:
        f"plink2 --pfile {config['plink2_binary_files']} --extract {{input}} --export A --out {{wildcards.dir}}GWAS_variants_clumped_mac"

#For efficient columnar reading of allele counts by variant ID, the raw allele counts are written to a parquet file using a python script:
rule write_parquet:
    threads: 16
    resources:
        mem_mb=lambda wildcards, attempt, threads: (4000 * threads) * attempt
    input:
        "{dir}GWAS_variants_clumped_mac.raw"
    output:
        "{dir}GWAS_variants_clumped_mac.parquet"
    shell:
        f"python create_parquet.py {{input}} {{output}}"
